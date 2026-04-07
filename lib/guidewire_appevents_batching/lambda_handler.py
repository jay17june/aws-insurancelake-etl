# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import json
import re
import boto3
import botocore
import os
import logging
from datetime import datetime, timezone
from urllib.parse import unquote_plus

# Logger initiation
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Event type to InsuranceLake table routing
# S3 key pattern: cc:NNNN/cc:NNNN-{EventType}-timestamp.json
EVENT_TYPE_ROUTING = {
    'ClaimCreated': 'Claims',
    'ClaimChanged': 'Claims',
    'ExposureAdded': 'Exposures',
    'ExposureChanged': 'Exposures',
    'PaymentCreated': 'Payments',
    'PaymentChanged': 'Payments',
}

# Nested fields to stringify per table to prevent Spark struct inference
# with dynamic colon-containing keys (e.g., cc:17499)
STRINGIFY_FIELDS = {
    'Claims': [
        'activities', 'contacts', 'exposures', 'reserves',
        'vehicle-incidents', 'notes', 'policyAddresses',
    ],
    'Exposures': [
        'contacts', 'exposures', 'vehicleIncidents',
        'allValidationLevelsReached', 'policyAddresses',
    ],
    'Payments': [
        'amount', 'transactionAmount', 'lineItems', 'payee',
    ],
}

EVENT_TYPE_PATTERN = re.compile(r'cc:\d+-(\w+)-\d{8}T\d{6}Z-\d+\.json$')


def classify_event(source_key):
    """Extract event type from S3 key and return the target table name."""
    match = EVENT_TYPE_PATTERN.search(source_key)
    if not match:
        return None
    event_type = match.group(1)
    return EVENT_TYPE_ROUTING.get(event_type)


def lambda_handler(event: dict, _) -> dict:
    """Lambda function's entry point. Triggered by EventBridge schedule to batch
    Guidewire AppEvents from SQS into consolidated JSONL files for InsuranceLake.

    Drains the SQS queue of S3 event notifications, reads the corresponding JSON
    files from the Guidewire S3 bucket, classifies events by type, and writes
    separate JSONL batch files per event group (Claims, Exposures, Payments).

    Parameters
    ----------
    event
        EventBridge scheduled event (contents not used)

    Returns
    -------
    dict
        Lambda result dictionary
    """
    s3_client = boto3.client('s3')
    sqs_client = boto3.client('sqs')

    queue_url = os.environ['SQS_QUEUE_URL']
    collect_bucket = os.environ['COLLECT_BUCKET_NAME']
    source_system = os.environ.get('SOURCE_SYSTEM', 'GWClaimCenter')

    # Separate event buckets per table
    events_by_table = {table: [] for table in set(EVENT_TYPE_ROUTING.values())}
    receipts_to_delete = []
    failed_count = 0

    # Drain the SQS queue by receiving up to 10 messages per call
    while True:
        try:
            response = sqs_client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=1,
            )
        except botocore.exceptions.ClientError as error:
            raise RuntimeError(f'SQS receive_message failed: {error}')

        messages = response.get('Messages', [])
        if not messages:
            break

        for message in messages:
            receipt_handle = message['ReceiptHandle']
            message_id = message.get('MessageId', 'unknown')

            try:
                body = json.loads(message['Body'])
                records = body.get('Records', [])

                for record in records:
                    source_bucket = record['s3']['bucket']['name']
                    source_key = unquote_plus(record['s3']['object']['key'])

                    # Skip folder creation events
                    if source_key.endswith('/'):
                        logger.info(f'Skipping folder creation event: {source_key}')
                        continue

                    # Classify event type from S3 key
                    table_name = classify_event(source_key)
                    if not table_name:
                        logger.warning(f'Unknown event type in key: {source_key}, skipping')
                        continue

                    logger.debug(f'Reading s3://{source_bucket}/{source_key} -> {table_name}')
                    obj_response = s3_client.get_object(
                        Bucket=source_bucket,
                        Key=source_key,
                    )
                    content = obj_response['Body'].read().decode('utf-8').strip()

                    # Parse JSON and stringify nested collections to prevent
                    # Spark from inferring structs with dynamic colon-containing
                    # keys (e.g., cc:17499) that are incompatible with Hive/Parquet
                    event_data = json.loads(content)
                    for field in STRINGIFY_FIELDS.get(table_name, []):
                        if field in event_data and not isinstance(event_data[field], str):
                            event_data[field] = json.dumps(event_data[field], separators=(',', ':'))

                    single_line = json.dumps(event_data, separators=(',', ':'))
                    events_by_table[table_name].append(single_line)

                receipts_to_delete.append(receipt_handle)

            except Exception:
                logger.exception(f'Failed to process SQS message {message_id}')
                failed_count += 1

    # Check if any events were collected
    total_events = sum(len(v) for v in events_by_table.values())
    if total_events == 0:
        logger.info('No messages in queue; no-op')
        return {
            'statusCode': 200,
            'body': json.dumps('No messages to process'),
        }

    # Write separate JSONL per event group to InsuranceLake collect bucket
    timestamp = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
    output_files = []

    for table_name, events in events_by_table.items():
        if not events:
            continue

        output_key = f'{source_system}/{table_name}/batch-{timestamp}.jsonl'
        jsonl_body = '\n'.join(events) + '\n'

        try:
            s3_client.put_object(
                Bucket=collect_bucket,
                Key=output_key,
                Body=jsonl_body.encode('utf-8'),
            )
        except botocore.exceptions.ClientError as error:
            raise RuntimeError(f'Failed to write batch to s3://{collect_bucket}/{output_key}: {error}')

        logger.info(f'Wrote {len(events)} events to s3://{collect_bucket}/{output_key}')
        output_files.append(f'{table_name}:{len(events)}')

    # Delete only successfully processed messages from SQS
    for receipt in receipts_to_delete:
        try:
            sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt)
        except botocore.exceptions.ClientError as error:
            logger.error(f'Failed to delete SQS message: {error}')

    return_message = f'Batched {total_events} events ({", ".join(output_files)})'
    if failed_count > 0:
        return_message += f', {failed_count} messages failed (will retry via SQS)'
    logger.info(return_message)

    return {
        'statusCode': 200,
        'body': json.dumps(return_message),
    }

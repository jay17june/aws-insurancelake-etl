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
    """Lambda function's entry point. Triggered by SQS event source mapping
    to batch Guidewire AppEvents into consolidated JSONL files for InsuranceLake.

    Receives a batch of SQS messages (up to 100) containing S3 event
    notifications, classifies events by type, and writes separate JSONL
    batch files per event group (Claims, Exposures, Payments).

    Parameters
    ----------
    event
        SQS batch event containing Records array

    Returns
    -------
    dict
        Batch item failures for partial failure reporting
    """
    s3_client = boto3.client('s3')

    collect_bucket = os.environ['COLLECT_BUCKET_NAME']
    source_system = os.environ.get('SOURCE_SYSTEM', 'GWClaimCenter')

    # Separate event buckets per table
    events_by_table = {table: [] for table in set(EVENT_TYPE_ROUTING.values())}
    failed_message_ids = []

    # Process each SQS record in the batch (AWS sends up to batch_size messages)
    for record in event.get('Records', []):
        message_id = record.get('messageId', 'unknown')

        try:
            body = json.loads(record['body'])
            s3_records = body.get('Records', [])

            for s3_record in s3_records:
                source_bucket = s3_record['s3']['bucket']['name']
                source_key = unquote_plus(s3_record['s3']['object']['key'])

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

        except Exception:
            logger.exception(f'Failed to process SQS message {message_id}')
            failed_message_ids.append(message_id)

    # Check if any events were collected
    total_events = sum(len(v) for v in events_by_table.values())
    if total_events == 0:
        logger.info('No processable events in batch')
        return {'batchItemFailures': [{'itemIdentifier': mid} for mid in failed_message_ids]}

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
            logger.error(f'Failed to write batch to s3://{collect_bucket}/{output_key}: {error}')
            # All messages in this invocation should retry
            return {'batchItemFailures': [{'itemIdentifier': r['messageId']} for r in event.get('Records', [])]}

        logger.info(f'Wrote {len(events)} events to s3://{collect_bucket}/{output_key}')
        output_files.append(f'{table_name}:{len(events)}')

    return_message = f'Batched {total_events} events ({", ".join(output_files)})'
    if failed_message_ids:
        return_message += f', {len(failed_message_ids)} messages failed'
    logger.info(return_message)

    # Report partial failures — AWS will retry only these messages
    return {
        'batchItemFailures': [{'itemIdentifier': mid} for mid in failed_message_ids]
    }

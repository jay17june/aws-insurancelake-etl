# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import json
import boto3
import botocore
import os
import logging
from datetime import datetime, timezone
from urllib.parse import unquote_plus

# Logger initiation
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event: dict, _) -> dict:
    """Lambda function's entry point. Triggered by EventBridge schedule to batch
    Guidewire AppEvents from SQS into consolidated JSONL files for InsuranceLake.

    Drains the SQS queue of S3 event notifications, reads the corresponding JSON
    files from the Guidewire S3 bucket, consolidates them into a single JSONL file,
    and writes it to the InsuranceLake collect bucket.

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
    target_prefix = os.environ.get('TARGET_PREFIX', 'GWClaimCenter/Claims')

    all_events = []
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

                    logger.debug(f'Reading s3://{source_bucket}/{source_key}')
                    obj_response = s3_client.get_object(
                        Bucket=source_bucket,
                        Key=source_key,
                    )
                    content = obj_response['Body'].read().decode('utf-8').strip()

                    # Parse JSON and stringify nested collections to prevent
                    # Spark from inferring structs with dynamic colon-containing
                    # keys (e.g., cc:17499) that are incompatible with Hive/Parquet
                    event_data = json.loads(content)
                    STRINGIFY_FIELDS = [
                        'activities', 'contacts', 'exposures', 'reserves',
                        'vehicle-incidents', 'vehicleIncidents', 'notes',
                        'policyAddresses',
                    ]
                    for field in STRINGIFY_FIELDS:
                        if field in event_data and not isinstance(event_data[field], str):
                            event_data[field] = json.dumps(event_data[field], separators=(',', ':'))

                    single_line = json.dumps(event_data, separators=(',', ':'))
                    all_events.append(single_line)

                receipts_to_delete.append(receipt_handle)

            except Exception:
                logger.exception(f'Failed to process SQS message {message_id}')
                failed_count += 1

    if not all_events:
        logger.info('No messages in queue; no-op')
        return {
            'statusCode': 200,
            'body': json.dumps('No messages to process'),
        }

    # Write consolidated JSONL to InsuranceLake collect bucket
    timestamp = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
    output_key = f'{target_prefix}/batch-{timestamp}.jsonl'
    jsonl_body = '\n'.join(all_events) + '\n'

    try:
        s3_client.put_object(
            Bucket=collect_bucket,
            Key=output_key,
            Body=jsonl_body.encode('utf-8'),
        )
    except botocore.exceptions.ClientError as error:
        raise RuntimeError(f'Failed to write batch to s3://{collect_bucket}/{output_key}: {error}')

    logger.info(f'Wrote {len(all_events)} events to s3://{collect_bucket}/{output_key}')

    # Delete only successfully processed messages from SQS
    for receipt in receipts_to_delete:
        try:
            sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt)
        except botocore.exceptions.ClientError as error:
            logger.error(f'Failed to delete SQS message: {error}')

    return_message = f'Batched {len(all_events)} events to {output_key}'
    if failed_count > 0:
        return_message += f', {failed_count} messages failed (will retry via SQS)'
    logger.info(return_message)

    return {
        'statusCode': 200,
        'body': json.dumps(return_message),
    }

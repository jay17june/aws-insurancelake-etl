# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
"""
One-time Guidewire AppEvents bulk migration Glue job.

Reads all JSON event files from a Guidewire S3 bucket, classifies them
by event type (Claims, Exposures, Payments), stringifies nested collections
to prevent Spark struct inference issues, and writes consolidated JSONL files
to the InsuranceLake collect bucket.

Usage:
    aws glue start-job-run \
        --job-name dev-insurancelake-gw-bulk-migration-job \
        --arguments '{"--source_path":"s3://gw-appevents-bucket/"}' \
        --region us-east-1
"""
import sys
import re
import json
from datetime import datetime, timezone

from pyspark.context import SparkContext
from pyspark.sql.functions import input_file_name, udf, col, to_json, struct
from pyspark.sql.types import StringType
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job


# Event type to InsuranceLake table routing
EVENT_TYPE_ROUTING = {
    'ClaimCreated': 'Claims',
    'ClaimChanged': 'Claims',
    'ExposureAdded': 'Exposures',
    'ExposureChanged': 'Exposures',
    'PaymentCreated': 'Payments',
    'PaymentChanged': 'Payments',
}

# Nested fields to stringify per table
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


def classify_event_type(file_path):
    """Extract event type from S3 file path and return the target table name."""
    match = EVENT_TYPE_PATTERN.search(file_path)
    if not match:
        return None
    event_type = match.group(1)
    return EVENT_TYPE_ROUTING.get(event_type)


def main():
    expected_arguments = [
        'JOB_NAME',
        'source_path',
        'target_bucket',
        'source_system',
    ]
    args = getResolvedOptions(sys.argv, expected_arguments)

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    source_path = args['source_path']
    target_bucket = args['target_bucket']
    source_system = args['source_system']
    timestamp = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')

    print(f'Bulk migration starting: source={source_path}, target={target_bucket}, system={source_system}')

    # Register UDF to classify events by file path
    classify_udf = udf(classify_event_type, StringType())

    # Read all JSON files from the Guidewire S3 bucket
    # Spark parallelizes across all files automatically
    print(f'Reading all JSON files from {source_path}')
    df = spark.read.option('multiLine', True).json(f'{source_path}**/*.json')
    total_count = df.count()
    print(f'Total events read: {total_count}')

    if total_count == 0:
        print('No events found, exiting')
        job.commit()
        return

    # Add file path column for event classification
    df = df.withColumn('_source_file', input_file_name())
    df = df.withColumn('_table_name', classify_udf(col('_source_file')))

    # Process each event type group
    for table_name in set(EVENT_TYPE_ROUTING.values()):
        table_df = df.filter(col('_table_name') == table_name)
        table_count = table_df.count()

        if table_count == 0:
            print(f'{table_name}: 0 events, skipping')
            continue

        print(f'{table_name}: {table_count} events')

        # Drop helper columns
        table_df = table_df.drop('_source_file', '_table_name')

        # Stringify nested collections to prevent Hive/Parquet issues
        # with dynamic colon-containing keys (e.g., cc:17499)
        for field in STRINGIFY_FIELDS.get(table_name, []):
            if field in table_df.columns:
                table_df = table_df.withColumn(field, to_json(col(field)))

        # Write as JSONL to InsuranceLake collect bucket
        output_path = f'{target_bucket}/{source_system}/{table_name}/migration-{timestamp}.jsonl'
        print(f'Writing {table_count} events to {output_path}')

        # Write as single JSONL file (coalesce to 1 partition)
        table_df.coalesce(1).write.mode('overwrite').json(output_path)

    print('Bulk migration complete')
    job.commit()


if __name__ == '__main__':
    main()

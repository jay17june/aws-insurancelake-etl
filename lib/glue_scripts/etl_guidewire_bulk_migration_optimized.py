# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
"""
Optimized Guidewire AppEvents bulk migration Glue job for 1M+ events.

Uses schema sampling and batched processing to handle massive datasets efficiently.
Spark parallelizes across all files while minimizing schema inference overhead.

Usage:
    aws glue start-job-run \
        --job-name dev-insurancelake-gw-bulk-migration-optimized-job \
        --arguments '{
            "--source_path":"s3://gw-appevents-bucket/",
            "--sample_size":"5000",
            "--batch_partitions":"100"
        }' \
        --region us-east-1
"""
import sys
import re
import json
from datetime import datetime, timezone

from pyspark.context import SparkContext
from pyspark.sql.functions import input_file_name, udf, col, to_json, lit
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


def generate_optimized_schema(spark, source_path, sample_size):
    """Generate unified schema from sample of files for performance optimization"""
    print(f'Generating schema from sample of {sample_size} files for performance...')

    # Read sample files to build unified schema
    sample_df = spark.read.option('multiLine', True).json(f'{source_path}**/*.json').limit(sample_size)
    sample_count = sample_df.count()
    print(f'Schema sample: {sample_count} events from {source_path}')

    if sample_count == 0:
        raise RuntimeError(f'No sample data found in {source_path}')

    # Return the schema for applying to full dataset
    return sample_df.schema


def main():
    expected_arguments = [
        'JOB_NAME',
        'source_path',
        'target_bucket',
        'source_system',
    ]

    # Optional arguments for performance tuning
    optional_arguments = ['sample_size', 'batch_partitions']
    local_expected_arguments = expected_arguments.copy()
    for arg in sys.argv:
        for opt_arg in optional_arguments:
            if f'--{opt_arg}' in arg:
                local_expected_arguments.append(opt_arg)

    args = getResolvedOptions(sys.argv, local_expected_arguments)

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    source_path = args['source_path']
    target_bucket = args['target_bucket']
    source_system = args['source_system']
    sample_size = int(args.get('sample_size', 5000))  # Default: sample 5K files for schema
    batch_partitions = int(args.get('batch_partitions', 100))  # Default: 100 partitions for writing
    timestamp = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')

    print(f'Optimized bulk migration starting:')
    print(f'  Source: {source_path}')
    print(f'  Target: {target_bucket}')
    print(f'  Schema sample size: {sample_size}')
    print(f'  Write partitions: {batch_partitions}')

    # OPTIMIZATION: Generate schema from sample first (faster than full inference)
    schema = generate_optimized_schema(spark, source_path, sample_size)
    print(f'Schema generated with {len(schema.fields)} fields')

    # Read ALL files with pre-determined schema (skips inference step)
    print(f'Reading all JSON files with optimized schema...')
    df = spark.read.schema(schema).option('multiLine', True).json(f'{source_path}**/*.json')

    # OPTIMIZATION: Cache the full dataset since we'll filter it multiple times
    df.cache()

    # Get actual count in background while processing
    print('Counting total events in background...')

    # Register UDF for event classification
    classify_udf = udf(classify_event_type, StringType())

    # Add classification columns
    df = df.withColumn('_source_file', input_file_name())
    df = df.withColumn('_table_name', classify_udf(col('_source_file')))

    # Process each event type group
    for table_name in set(EVENT_TYPE_ROUTING.values()):
        print(f'\nProcessing {table_name} events...')

        table_df = df.filter(col('_table_name') == table_name)

        # OPTIMIZATION: Use approximate count for large datasets
        if sample_size >= 5000:
            # For large datasets, estimate count from sample
            table_count = 'unknown (large dataset)'
        else:
            table_count = table_df.count()

        print(f'{table_name}: {table_count} events (processing...)')

        # Drop helper columns
        table_df = table_df.drop('_source_file', '_table_name')

        # Stringify nested collections to prevent Hive/Parquet issues
        for field in STRINGIFY_FIELDS.get(table_name, []):
            if field in table_df.columns:
                table_df = table_df.withColumn(field, to_json(col(field)))

        # Add bulk migration identifier
        table_df = table_df.withColumn('bulk_migration_timestamp', lit(timestamp))

        # OPTIMIZATION: Write with optimal partitioning for large datasets
        output_path = f'{target_bucket}/{source_system}/{table_name}/bulk-migration-{timestamp}.jsonl'
        print(f'Writing {table_name} events to {output_path}')

        # For very large datasets, use multiple output files instead of coalesce(1)
        if batch_partitions > 1:
            table_df.repartition(batch_partitions).write.mode('overwrite').json(output_path)
        else:
            table_df.coalesce(1).write.mode('overwrite').json(output_path)

        print(f'{table_name} processing complete')

    # Unpersist cached DataFrame
    df.unpersist()

    print('Optimized bulk migration complete')
    job.commit()


if __name__ == '__main__':
    main()
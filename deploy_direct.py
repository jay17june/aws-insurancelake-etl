#!/usr/bin/env python3
"""Direct deployment script - instantiates stacks at app level (no Stage/Pipeline)"""
import aws_cdk as cdk
from cdk_nag import AwsSolutionsChecks

from lib.step_functions_stack import StepFunctionsStack
from lib.glue_buckets_stack import GlueBucketsStack
from lib.glue_jobs_stack import GlueJobsStack
from lib.data_lake_consumer_stack import DataLakeConsumerStack
from lib.dynamodb_stack import DynamoDbStack
from lib.athena_workgroup_stack import AthenaWorkgroupStack
from lib.guidewire_appevents_stack import GuidewireAppEventsStack
from lib.tagging import tag
from lib.configuration import (
    ACCOUNT_ID, DEPLOYMENT, DEV, REGION, GUIDEWIRE_APPEVENTS_BUCKET,
    get_all_configurations, get_logical_id_prefix, get_local_configuration,
)

app = cdk.App()
cdk.Aspects.of(app).add(AwsSolutionsChecks())

raw_mappings = get_all_configurations()
target_environment = DEV
account = raw_mappings[DEPLOYMENT][ACCOUNT_ID]
region = raw_mappings[DEV][REGION]
env = cdk.Environment(account=account, region=region)
logical_id_prefix = get_logical_id_prefix()

dynamodb_stack = DynamoDbStack(
    app,
    f'{logical_id_prefix}EtlDynamoDb',
    description='InsuranceLake DynamoDB tables',
    target_environment=target_environment,
    env=env,
)

glue_buckets_stack = GlueBucketsStack(
    app,
    f'{logical_id_prefix}EtlGlueBuckets',
    description='InsuranceLake Glue S3 buckets',
    target_environment=target_environment,
    env=env,
)

athena_workgroup_stack = AthenaWorkgroupStack(
    app,
    f'{logical_id_prefix}EtlAthenaWorkgroup',
    description='InsuranceLake Athena Workgroup',
    target_environment=target_environment,
    env=env,
    glue_scripts_temp_bucket=glue_buckets_stack.glue_scripts_temp_bucket,
)

glue_jobs_stack = GlueJobsStack(
    app,
    f'{logical_id_prefix}EtlGlueJobs',
    description='InsuranceLake Glue jobs',
    target_environment=target_environment,
    env=env,
    hash_values_table=dynamodb_stack.hash_values_table,
    value_lookup_table=dynamodb_stack.value_lookup_table,
    multi_lookup_table=dynamodb_stack.multi_lookup_table,
    dq_results_table=dynamodb_stack.dq_results_table,
    data_lineage_table=dynamodb_stack.data_lineage_table,
    glue_scripts_bucket=glue_buckets_stack.glue_scripts_bucket,
    glue_scripts_temp_bucket=glue_buckets_stack.glue_scripts_temp_bucket,
    athena_workgroup=athena_workgroup_stack.athena_workgroup,
)

step_function_stack = StepFunctionsStack(
    app,
    f'{logical_id_prefix}EtlStepFunctions',
    description='InsuranceLake Step Functions',
    target_environment=target_environment,
    env=env,
    collect_to_cleanse_job=glue_jobs_stack.collect_to_cleanse_job,
    cleanse_to_consume_job=glue_jobs_stack.cleanse_to_consume_job,
    consume_entity_match_job=glue_jobs_stack.consume_entity_match_job,
    job_audit_table=dynamodb_stack.job_audit_table,
    glue_scripts_bucket=glue_buckets_stack.glue_scripts_bucket,
)

data_lake_consumer_stack = DataLakeConsumerStack(
    app,
    f'{logical_id_prefix}EtlDataLakeConsumer',
    description='InsuranceLake data lake consumer',
    target_environment=target_environment,
    env=env,
    glue_scripts_temp_bucket=glue_buckets_stack.glue_scripts_temp_bucket,
)

local_config = get_local_configuration(target_environment)
guidewire_appevents_stack = GuidewireAppEventsStack(
    app,
    f'{logical_id_prefix}EtlGuidewireAppEvents',
    description='InsuranceLake Guidewire AppEvents batching pipeline',
    target_environment=target_environment,
    env=env,
    guidewire_bucket_name=local_config[GUIDEWIRE_APPEVENTS_BUCKET],
)

for stack in [dynamodb_stack, glue_buckets_stack, athena_workgroup_stack,
              glue_jobs_stack, step_function_stack, data_lake_consumer_stack,
              guidewire_appevents_stack]:
    tag(stack, target_environment)

app.synth()

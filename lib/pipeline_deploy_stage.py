# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import aws_cdk as cdk
from constructs import Construct
from .step_functions_stack import StepFunctionsStack
from .glue_buckets_stack import GlueBucketsStack
from .glue_jobs_stack import GlueJobsStack
from .data_lake_consumer_stack import DataLakeConsumerStack
from .dynamodb_stack import DynamoDbStack
from .athena_workgroup_stack import AthenaWorkgroupStack
from .guidewire_appevents_stack import GuidewireAppEventsStack
from .tagging import tag
from .configuration import (
    GUIDEWIRE_APPEVENTS_BUCKET, LAMBDA_MEMORY, LAMBDA_TIMEOUT, LAMBDA_BATCH_SIZE,
    LAMBDA_CONCURRENCY, LAMBDA_BATCHING_WINDOW, SQS_VISIBILITY_TIMEOUT, SQS_RETENTION_DAYS,
    DLQ_RETENTION_DAYS, GLUE_WORKERS_STANDARD, GLUE_WORKERS_BULK,
    get_logical_id_prefix, get_local_configuration,
)

class PipelineDeployStage(cdk.Stage):
    def __init__(
        self, scope: Construct, construct_id: str,
        target_environment: str, env: cdk.Environment=None,
        context_config: dict = None,
        **kwargs
    ):
        """Adds deploy stage to CodePipeline

        Parameters
        ----------
        scope
            Parent of this stack, usually an App or a Stage, but could be any construct
        construct_id
            The construct ID of this stack; if stackName is not explicitly defined,
            this ID (and any parent IDs) will be used to determine the physical ID of the stack
        target_environment
            The target environment for stacks in the deploy stage
        env: optional
            AWS environment definition (account, region) to pass to stacks
        kwargs: optional
            Optional keyword arguments
        """
        super().__init__(scope, construct_id, **kwargs)
        logical_id_prefix = get_logical_id_prefix()

        dynamodb_stack = DynamoDbStack(
            self,
            f'{logical_id_prefix}EtlDynamoDb',
            description='InsuranceLake stack for DynamoDB tables to store pipeline metadata (SO9489) (uksb-1tu7mtee2)',
            target_environment=target_environment,
            env=env,
            **kwargs,
        )

        glue_buckets_stack = GlueBucketsStack(
            self,
            f'{logical_id_prefix}EtlGlueBuckets',
            description='InsuranceLake stack for S3 buckets used by Glue jobs (SO9489) (uksb-1tu7mtee2)',
            target_environment=target_environment,
            env=env,
            **kwargs,
        )

        athena_workgroup_stack = AthenaWorkgroupStack(
            self,
            f'{logical_id_prefix}EtlAthenaWorkgroup',
            description='InsuranceLake stack for Athena Workgroup (SO9489) (uksb-1tu7mtee2)',
            target_environment=target_environment,
            env=env,
            glue_scripts_temp_bucket=glue_buckets_stack.glue_scripts_temp_bucket,
            **kwargs,
        )

        # Use context-based configuration (fallback to local_config for backwards compatibility)
        if context_config:
            config = context_config
        else:
            config = get_local_configuration(target_environment)

        glue_jobs_stack = GlueJobsStack(
            self,
            f'{logical_id_prefix}EtlGlueJobs',
            description='InsuranceLake stack for Glue jobs to support the data pipeline (SO9489) (uksb-1tu7mtee2)',
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
            glue_config={
                'workers_standard': config.get(GLUE_WORKERS_STANDARD, 25),
                'workers_bulk': config.get(GLUE_WORKERS_BULK, 50),
            },
            **kwargs,
        )

        step_function_stack = StepFunctionsStack(
            self,
            f'{logical_id_prefix}EtlStepFunctions',
            description='InsuranceLake stack for Step Functions and supporting Lambda functions to orchestrate data pipeline steps (SO9489) (uksb-1tu7mtee2)',
            target_environment=target_environment,
            env=env,
            collect_to_cleanse_job=glue_jobs_stack.collect_to_cleanse_job,
            cleanse_to_consume_job=glue_jobs_stack.cleanse_to_consume_job,
            consume_entity_match_job=glue_jobs_stack.consume_entity_match_job,
            job_audit_table=dynamodb_stack.job_audit_table,
            glue_scripts_bucket=glue_buckets_stack.glue_scripts_bucket,
            **kwargs,
        )

        data_lake_consumer_stack = DataLakeConsumerStack(
            self,
            f'{logical_id_prefix}EtlDataLakeConsumer',
            description='InsuranceLake stack for data lake consumer IAM policy (SO9489) (uksb-1tu7mtee2)',
            target_environment=target_environment,
            env=env,
            glue_scripts_temp_bucket=glue_buckets_stack.glue_scripts_temp_bucket,
            **kwargs,
        )

        guidewire_appevents_stack = GuidewireAppEventsStack(
            self,
            f'{logical_id_prefix}EtlGuidewireAppEvents',
            description='InsuranceLake stack for Guidewire AppEvents batching pipeline (SO9489) (uksb-1tu7mtee2)',
            target_environment=target_environment,
            env=env,
            guidewire_bucket_name=config[GUIDEWIRE_APPEVENTS_BUCKET],
            lambda_config={
                'memory': config.get(LAMBDA_MEMORY, 512),
                'timeout': config.get(LAMBDA_TIMEOUT, 15),
                'batch_size': config.get(LAMBDA_BATCH_SIZE, 100),
                'max_concurrency': config.get(LAMBDA_CONCURRENCY, 10),
                'batching_window': config.get(LAMBDA_BATCHING_WINDOW, 30),
            },
            sqs_config={
                'visibility_timeout': config.get(SQS_VISIBILITY_TIMEOUT, 960),
                'retention_days': config.get(SQS_RETENTION_DAYS, 4),
                'dlq_retention_days': config.get(DLQ_RETENTION_DAYS, 14),
            },
            **kwargs,
        )

        tag(step_function_stack, target_environment)
        tag(dynamodb_stack, target_environment)
        tag(glue_buckets_stack, target_environment)
        tag(data_lake_consumer_stack, target_environment)
        tag(athena_workgroup_stack, target_environment)
        tag(glue_jobs_stack, target_environment)
        tag(guidewire_appevents_stack, target_environment)
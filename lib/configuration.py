# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import re
import boto3

# Environments (targeted at accounts)
DEPLOYMENT = 'Deploy'
DEV = 'Dev'
TEST = 'Test'
PROD = 'Prod'

# The following constants are used to map to parameter paths
ENVIRONMENT = 'environment'

# Manual Inputs
CODECONNECTIONS_ARN = 'codeconnections_arn'
CODECONNECTIONS_REPOSITORY_OWNER_NAME = 'codeconnections_repository_owner_name'
CODECONNECTIONS_REPOSITORY_NAME = 'codeconnections_repository_name'
CODECOMMIT_REPOSITORY_NAME = 'codecommit_repository_name'
CODECOMMIT_MIRROR_REPOSITORY_NAME = 'codecommit_mirror_repository_name'
ACCOUNT_ID = 'account_id'
REGION = 'region'
VPC_CIDR = 'vpc_cidr'
LOGICAL_ID_PREFIX = 'logical_id_prefix'
RESOURCE_NAME_PREFIX = 'resource_name_prefix'
CODE_BRANCH = 'code_branch'
LINEAGE='lineage'
GUIDEWIRE_APPEVENTS_BUCKET = 'guidewire_appevents_bucket'

# CDK Context Configuration Parameters
LAMBDA_MEMORY = 'lambda_memory'
LAMBDA_TIMEOUT = 'lambda_timeout'
LAMBDA_BATCH_SIZE = 'lambda_batch_size'
LAMBDA_CONCURRENCY = 'lambda_concurrency'
LAMBDA_BATCHING_WINDOW = 'lambda_batching_window'
SQS_VISIBILITY_TIMEOUT = 'sqs_visibility_timeout'
SQS_RETENTION_DAYS = 'sqs_retention_days'
DLQ_RETENTION_DAYS = 'dlq_retention_days'
GLUE_WORKERS_STANDARD = 'glue_workers_standard'
GLUE_WORKERS_BULK = 'glue_workers_bulk'

# Used in Automated Outputs
VPC_ID = 'vpc_id'
AVAILABILITY_ZONE_1 = 'availability_zone_1'
AVAILABILITY_ZONE_2 = 'availability_zone_2'
AVAILABILITY_ZONE_3 = 'availability_zone_3'
SUBNET_ID_1 = 'subnet_id_1'
SUBNET_ID_2 = 'subnet_id_2'
SUBNET_ID_3 = 'subnet_id_3'
ROUTE_TABLE_1 = 'route_table_1'
ROUTE_TABLE_2 = 'route_table_2'
ROUTE_TABLE_3 = 'route_table_3'
SHARED_SECURITY_GROUP_ID = 'shared_security_group_id'
S3_KMS_KEY = 's3_kms_key'
S3_ACCESS_LOG_BUCKET = 's3_access_log_bucket'
S3_RAW_BUCKET = 's3_raw_bucket'
S3_CONFORMED_BUCKET = 's3_conformed_bucket'
S3_PURPOSE_BUILT_BUCKET = 's3_purpose_built_bucket'
CROSS_ACCOUNT_DYNAMODB_ROLE = 'cross_account_dynamodb_role'
STATE_MACHINE = 'sfn_state_machine'
NOTIFICATION_TOPIC = 'sns_topic'

# Reference: https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html
MAX_S3_BUCKET_NAME_LENGTH = 63

def get_local_configuration(environment: str, local_mapping: dict = None) -> dict:
    """Provides manually configured variables that are validated for quality and safety.

    Parameters
    ----------
    environment
        The environment used to retrieve corresponding configuration
    local_mapping: optional
        Optional override the embedded local_mapping; used for testing

    Raises
    ------
    AttributeError
        If the resource_name_prefix does not conform or if the requested
        environment does not exist

    Returns
    -------
    dict
        Configuration for the requested environment
    """
    active_account_id = boto3.client('sts').get_caller_identity()['Account']

    if local_mapping is None:
        local_mapping = {
            DEPLOYMENT: {
                ACCOUNT_ID: active_account_id,
                REGION: 'us-east-1',

                # If you use Github, Gitlab, Bitbucket Cloud or any other supported CodeConnections
                # provider, specify the CodeConnections ARN
                CODECONNECTIONS_ARN: '',

                # CodeConections repository owner or workspace name if using CodeConnections
                CODECONNECTIONS_REPOSITORY_OWNER_NAME: '',

                # Leave empty if you do not use CodeConnections
                CODECONNECTIONS_REPOSITORY_NAME: '',

                # Use only if your repository is already in CodecCommit, otherwise leave empty!
                CODECOMMIT_REPOSITORY_NAME: '',

                # Name your CodeCommit mirror repo here (recommend matching your external repo)
                # Leave empty if you use CodeConnections or your repository is in CodeCommit already
                CODECOMMIT_MIRROR_REPOSITORY_NAME: 'aws-insurancelake-etl',

                # This is used in the Logical Id of CloudFormation resources.
                # We recommend Capital case for consistency, e.g. DataLakeCdkBlog
                LOGICAL_ID_PREFIX: 'InsuranceLake',

                # Important: This is used as a prefix for resources that must be **globally** unique!
                # Resource names may only contain alphanumeric characters, hyphens, and cannot contain trailing hyphens.
                # S3 bucket names from this application must be under the 63 character bucket name limit
                RESOURCE_NAME_PREFIX: 'insurancelake',
            },
            DEV: {
                ACCOUNT_ID: active_account_id,
                REGION: 'us-east-1',
                LINEAGE: True,
                # VPC_CIDR: '10.20.0.0/22',
                CODE_BRANCH: 'develop',
                GUIDEWIRE_APPEVENTS_BUCKET: 'gw-appevents-038462774895-collect',
            },
            TEST: {
                ACCOUNT_ID: active_account_id,
                REGION: 'us-east-1',
                LINEAGE: True,
                # VPC_CIDR: '10.10.0.0/22',
                CODE_BRANCH: 'test',
                GUIDEWIRE_APPEVENTS_BUCKET: 'gw-appevents-038462774895-collect',
            },
            PROD: {
                ACCOUNT_ID: active_account_id,
                REGION: 'us-east-1',
                LINEAGE: True,
                # VPC_CIDR: '10.0.0.0/22',
                CODE_BRANCH: 'main',
                GUIDEWIRE_APPEVENTS_BUCKET: 'gw-appevents-038462774895-collect',
            }
        }

    resource_prefix = local_mapping[DEPLOYMENT][RESOURCE_NAME_PREFIX]
    if (
        not re.fullmatch('^[a-z0-9-]+', resource_prefix)
        or '-' in resource_prefix[-1:] or '-' in resource_prefix[1]
    ):
        raise AttributeError('Resource names may only contain lowercase alphanumeric and hyphens '
                        'and cannot contain leading or trailing hyphens')

    for each_env in local_mapping:
        # NOTE: Resource with longest bucket name is from the infra
        #       code base, but we will assume the user wants to have
        #       a consistent resource prefix across all stacks
        longest_bucket_name = \
            f'{each_env}-{resource_prefix}-{local_mapping[each_env][ACCOUNT_ID]}-{local_mapping[each_env][REGION]}-access-logs'
        if len(longest_bucket_name) > MAX_S3_BUCKET_NAME_LENGTH:
            raise AttributeError('Resource name prefix is too long; at least one S3 bucket name '
                        f'would exceed maximum allowed length of {MAX_S3_BUCKET_NAME_LENGTH} '
                        f'characters, e.g. {longest_bucket_name}')

    if environment not in local_mapping:
        raise AttributeError(f'The requested environment: {environment} does not exist in local mappings')

    return local_mapping[environment]


def get_environment_configuration(environment: str, local_mapping: dict = None) -> dict:
    """Provides all configuration values for the given target environment

    Parameters
    ----------
    environment
        The environment used to retrieve corresponding configuration
    local_mapping: optional
        Optionally override the embedded local_mapping; used for testing

    Returns
    -------
    dict
        Combined configuration and Cloudformation output names for target environment
    """
    cloudformation_output_mapping = {
        ENVIRONMENT: f'{environment}',
        VPC_ID: f'{environment}VpcId',
        AVAILABILITY_ZONE_1: f'{environment}AvailabilityZone1',
        AVAILABILITY_ZONE_2: f'{environment}AvailabilityZone2',
        AVAILABILITY_ZONE_3: f'{environment}AvailabilityZone3',
        SUBNET_ID_1: f'{environment}SubnetId1',
        SUBNET_ID_2: f'{environment}SubnetId2',
        SUBNET_ID_3: f'{environment}SubnetId3',
        ROUTE_TABLE_1: f'{environment}RouteTable1',
        ROUTE_TABLE_2: f'{environment}RouteTable2',
        ROUTE_TABLE_3: f'{environment}RouteTable3',
        SHARED_SECURITY_GROUP_ID: f'{environment}SharedSecurityGroupId',
        S3_KMS_KEY: f'{environment}S3KmsKeyArn',
        S3_ACCESS_LOG_BUCKET: f'{environment}S3AccessLogBucket',
        S3_RAW_BUCKET: f'{environment}CollectBucketName',
        S3_CONFORMED_BUCKET: f'{environment}CleanseBucketName',
        S3_PURPOSE_BUILT_BUCKET: f'{environment}ConsumeBucketName',
        CROSS_ACCOUNT_DYNAMODB_ROLE: f'{environment}CrossAccountDynamoDbRoleArn',
        STATE_MACHINE: f'{environment}StepFunctionsStateMachineName',
        NOTIFICATION_TOPIC: f'{environment}EtlNotificationSnsTopicName',
    }

    return {
        **cloudformation_output_mapping,
        **get_local_configuration(environment, local_mapping=local_mapping)
    }


def get_all_configurations() -> dict:
    """Returns a dict mapping of configurations for all environments.
    These keys correspond to static values, stack names, and CloudFormation outputs

    Returns
    -------
    dict
        Combined configuration and Cloudformation output names for all environments
    """
    return {
        DEPLOYMENT: {
            ENVIRONMENT: DEPLOYMENT,
            **get_local_configuration(DEPLOYMENT),
        },
        DEV: get_environment_configuration(DEV),
        TEST: get_environment_configuration(TEST),
        PROD: get_environment_configuration(PROD),
    }


def get_logical_id_prefix() -> str:
    """Returns the logical id prefix to apply to all CloudFormation resources

    Returns
    -------
    str
        Logical ID prefix from deployment configuration
    """
    return get_local_configuration(DEPLOYMENT)[LOGICAL_ID_PREFIX]


def get_resource_name_prefix() -> str:
    """Returns the resource name prefix to apply to all resources names

    Returns
    -------
    str
        Resource name prefix from deployment configuration
    """
    return get_local_configuration(DEPLOYMENT)[RESOURCE_NAME_PREFIX]


def get_context_configuration(app, target_environment: str) -> dict:
    """Get configuration from CDK context parameters with validation and defaults

    Parameters
    ----------
    app
        CDK App instance to read context from
    target_environment
        The target environment (DEV, TEST, PROD)

    Returns
    -------
    dict
        Configuration dictionary with validated context parameters

    Raises
    ------
    ValueError
        If context parameters are invalid or out of range
    """
    active_account_id = boto3.client('sts').get_caller_identity()['Account']

    # Get context values with defaults
    region = app.node.try_get_context('region') or 'us-east-2'
    gw_bucket = app.node.try_get_context('gwappevents-landing-bucket') or f'gw-appevents-{active_account_id}-collect'

    # Numeric parameters with validation
    lambda_memory = int(app.node.try_get_context('lambda-memory') or 512)
    if not (128 <= lambda_memory <= 10240):
        raise ValueError(f'lambda-memory must be 128-10240 MB, got {lambda_memory}')

    lambda_timeout = int(app.node.try_get_context('lambda-timeout') or 15)
    if not (1 <= lambda_timeout <= 15):
        raise ValueError(f'lambda-timeout must be 1-15 minutes, got {lambda_timeout}')

    lambda_batch_size = int(app.node.try_get_context('lambda-batch-size') or 100)
    if not (1 <= lambda_batch_size <= 10000):
        raise ValueError(f'lambda-batch-size must be 1-10000, got {lambda_batch_size}')

    lambda_concurrency = int(app.node.try_get_context('lambda-concurrency') or 10)
    if not (1 <= lambda_concurrency <= 1000):
        raise ValueError(f'lambda-concurrency must be 1-1000, got {lambda_concurrency}')

    lambda_batching_window = int(app.node.try_get_context('lambda-batching-window') or 30)
    if not (0 <= lambda_batching_window <= 300):
        raise ValueError(f'lambda-batching-window must be 0-300 seconds, got {lambda_batching_window}')

    sqs_visibility_timeout = int(app.node.try_get_context('sqs-visibility-timeout') or 960)
    if not (lambda_timeout * 60 <= sqs_visibility_timeout <= 43200):
        raise ValueError(f'sqs-visibility-timeout must be >= lambda-timeout ({lambda_timeout * 60}s) and <= 12 hours')

    sqs_retention_days = int(app.node.try_get_context('sqs-retention-days') or 4)
    if not (1 <= sqs_retention_days <= 14):
        raise ValueError(f'sqs-retention-days must be 1-14 days, got {sqs_retention_days}')

    dlq_retention_days = int(app.node.try_get_context('dlq-retention-days') or 14)
    if not (1 <= dlq_retention_days <= 14):
        raise ValueError(f'dlq-retention-days must be 1-14 days, got {dlq_retention_days}')

    glue_workers_standard = int(app.node.try_get_context('glue-workers-standard') or 25)
    if not (2 <= glue_workers_standard <= 250):
        raise ValueError(f'glue-workers-standard must be 2-250, got {glue_workers_standard}')

    glue_workers_bulk = int(app.node.try_get_context('glue-workers-bulk') or 50)
    if not (2 <= glue_workers_bulk <= 250):
        raise ValueError(f'glue-workers-bulk must be 2-250, got {glue_workers_bulk}')

    # Return configuration dict using existing constants
    return {
        ACCOUNT_ID: active_account_id,
        REGION: region,
        GUIDEWIRE_APPEVENTS_BUCKET: gw_bucket,
        LAMBDA_MEMORY: lambda_memory,
        LAMBDA_TIMEOUT: lambda_timeout,
        LAMBDA_BATCH_SIZE: lambda_batch_size,
        LAMBDA_CONCURRENCY: lambda_concurrency,
        LAMBDA_BATCHING_WINDOW: lambda_batching_window,
        SQS_VISIBILITY_TIMEOUT: sqs_visibility_timeout,
        SQS_RETENTION_DAYS: sqs_retention_days,
        DLQ_RETENTION_DAYS: dlq_retention_days,
        GLUE_WORKERS_STANDARD: glue_workers_standard,
        GLUE_WORKERS_BULK: glue_workers_bulk,
        # Add base configuration for compatibility
        LOGICAL_ID_PREFIX: get_logical_id_prefix(),
        RESOURCE_NAME_PREFIX: get_resource_name_prefix(),
        LINEAGE: True,
        CODE_BRANCH: {'Dev': 'develop', 'Test': 'test', 'Prod': 'main'}[target_environment],
    }
# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import os
import aws_cdk as cdk
from constructs import Construct
import aws_cdk.aws_iam as iam
import aws_cdk.aws_lambda as _lambda
import aws_cdk.aws_lambda_event_sources as lambda_event_sources
import aws_cdk.aws_logs as logs
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_s3_notifications as s3_notifications
import aws_cdk.aws_sqs as sqs
from cdk_nag import NagSuppressions

from .stack_import_helper import ImportedBuckets
from .configuration import (
    DEV, PROD, TEST,
    get_logical_id_prefix, get_resource_name_prefix, get_environment_configuration,
)


class GuidewireAppEventsStack(cdk.Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        target_environment: str,
        guidewire_bucket_name: str,
        lambda_config: dict = None,
        sqs_config: dict = None,
        **kwargs,
    ):
        """CloudFormation stack to create Guidewire AppEvents batching pipeline

        Parameters
        ----------
        scope
            Parent of this stack, usually an App or a Stage, but could be any construct
        construct_id
            The construct ID of this stack; if stackName is not explicitly defined,
            this ID (and any parent IDs) will be used to determine the physical ID of the stack
        target_environment
            The target environment for stacks in the deploy stage
        guidewire_bucket_name
            Name of the S3 bucket where Guidewire writes AppEvents JSON files
        kwargs: optional
            Optional keyword arguments to pass up to parent Stack class
        """
        super().__init__(scope, construct_id, **kwargs)

        # Set configuration defaults
        lambda_config = lambda_config or {}
        sqs_config = sqs_config or {}

        self.target_environment = target_environment
        self.mappings = get_environment_configuration(target_environment)
        self.logical_id_prefix = get_logical_id_prefix()
        self.resource_name_prefix = get_resource_name_prefix()
        if target_environment == PROD or target_environment == TEST:
            self.removal_policy = cdk.RemovalPolicy.RETAIN
            self.log_retention = logs.RetentionDays.SIX_MONTHS
        else:
            self.removal_policy = cdk.RemovalPolicy.DESTROY
            self.log_retention = logs.RetentionDays.ONE_MONTH

        self.buckets = ImportedBuckets(self, logical_id_suffix='GuidewireAppEvents')

        # SQS Dead Letter Queue for failed messages
        # Uses SQS managed encryption (SSE-SQS) instead of KMS to allow S3
        # event notifications without requiring KMS key policy changes
        dlq = sqs.Queue(
            self,
            f'{target_environment}{self.logical_id_prefix}GwAppEventsDLQ',
            queue_name=f'{target_environment.lower()}-{self.resource_name_prefix}-gw-appevents-dlq',
            retention_period=cdk.Duration.days(sqs_config.get('dlq_retention_days', 14)),
            encryption=sqs.QueueEncryption.SQS_MANAGED,
            enforce_ssl=True,
            removal_policy=self.removal_policy,
        )

        # SQS Main Queue to buffer S3 event notifications from Guidewire bucket
        # Visibility timeout must be >= Lambda timeout for SQS event source mapping
        queue = sqs.Queue(
            self,
            f'{target_environment}{self.logical_id_prefix}GwAppEventsQueue',
            queue_name=f'{target_environment.lower()}-{self.resource_name_prefix}-gw-appevents-queue',
            visibility_timeout=cdk.Duration.seconds(sqs_config.get('visibility_timeout', 960)),
            retention_period=cdk.Duration.days(sqs_config.get('retention_days', 4)),
            encryption=sqs.QueueEncryption.SQS_MANAGED,
            enforce_ssl=True,
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=3,
                queue=dlq,
            ),
            removal_policy=self.removal_policy,
        )

        # S3 Event Notification on Guidewire bucket -> SQS
        guidewire_bucket = s3.Bucket.from_bucket_name(
            self,
            f'{target_environment}{self.logical_id_prefix}ImportedGwBucket',
            bucket_name=guidewire_bucket_name,
        )
        guidewire_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3_notifications.SqsDestination(queue),
        )

        # Lambda Function for batching
        lambda_function_name = (
            f'{target_environment.lower()}-{self.resource_name_prefix}-gw-appevents-batching'
        )

        cloudwatch_log_group = logs.LogGroup(
            self,
            f'{target_environment}{self.logical_id_prefix}GwAppEventsBatchingLogGroup',
            log_group_name=f'/aws/lambda/{lambda_function_name}',
            retention=self.log_retention,
            removal_policy=self.removal_policy,
        )

        lambda_role = self._create_lambda_role(
            cloudwatch_log_group, queue, guidewire_bucket,
        )

        lambda_function = _lambda.Function(
            self,
            f'{target_environment}{self.logical_id_prefix}GwAppEventsBatching',
            function_name=lambda_function_name,
            description='Guidewire AppEvents batching - consolidates JSON events from SQS into JSONL for InsuranceLake',
            runtime=_lambda.Runtime.PYTHON_3_14,
            handler='lambda_handler.lambda_handler',
            code=_lambda.Code.from_asset(
                f'{os.path.dirname(__file__)}/guidewire_appevents_batching'
            ),
            architecture=_lambda.Architecture.ARM_64,
            memory_size=lambda_config.get('memory', 512),
            environment={
                'COLLECT_BUCKET_NAME': self.buckets.raw.bucket_name,
                'SOURCE_SYSTEM': 'GWClaimCenter',
            },
            timeout=cdk.Duration.minutes(lambda_config.get('timeout', 15)),
            log_group=cloudwatch_log_group,
            role=lambda_role,
        )

        # SQS event source mapping - auto-scales Lambda with queue depth
        # Replaces EventBridge schedule for real-time, surge-resilient processing
        lambda_function.add_event_source(
            lambda_event_sources.SqsEventSource(
                queue,
                batch_size=lambda_config.get('batch_size', 100),
                max_batching_window=cdk.Duration.seconds(lambda_config.get('batching_window', 30)),
                max_concurrency=lambda_config.get('max_concurrency', 10),
                report_batch_item_failures=True,
            )
        )

        NagSuppressions.add_resource_suppressions(self, [
            {
                'id': 'AwsSolutions-IAM4',
                'reason': 'Bucket Notification CustomResource used only during stack deployment and deletion'
            },
            {
                'id': 'AwsSolutions-IAM5',
                'reason': 'Bucket Notification CustomResource used only during stack deployment and deletion'
            },
        ], apply_to_children=True)


    def _create_lambda_role(
        self,
        log_group: logs.LogGroup,
        queue: sqs.Queue,
        guidewire_bucket: s3.IBucket,
    ) -> iam.Role:
        """Creates IAM role for the batching Lambda with least-privilege policies

        Parameters
        ----------
        log_group
            CloudWatch Log Group for the Lambda function
        queue
            SQS Queue to read and delete messages from
        guidewire_bucket
            Guidewire S3 bucket to read JSON files from

        Returns
        -------
        iam.Role
            The IAM role that was created
        """
        policies = {
            'CloudWatchLogAccess':
            iam.PolicyDocument(statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        'logs:CreateLogStream',
                        'logs:PutLogEvents',
                    ],
                    resources=[log_group.log_group_arn],
                )
            ]),
            'SqsAccess':
            iam.PolicyDocument(statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        'sqs:ReceiveMessage',
                        'sqs:DeleteMessage',
                        'sqs:GetQueueAttributes',
                        'sqs:ChangeMessageVisibility',
                    ],
                    resources=[queue.queue_arn],
                )
            ]),
            'GuidewireS3ReadAccess':
            iam.PolicyDocument(statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        's3:GetObject',
                    ],
                    resources=[guidewire_bucket.arn_for_objects('*')],
                )
            ]),
            'CollectBucketWriteAccess':
            iam.PolicyDocument(statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        's3:PutObject',
                    ],
                    resources=[self.buckets.raw.arn_for_objects('*')],
                )
            ]),
            'KmsAccess':
            iam.PolicyDocument(statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        'kms:Decrypt',
                        'kms:GenerateDataKey',
                    ],
                    resources=[self.buckets.s3_kms_key.key_arn],
                )
            ]),
        }

        iam_role = iam.Role(
            self,
            f'{self.target_environment}{self.logical_id_prefix}GwAppEventsBatchingLambdaRole',
            description='Role for Guidewire AppEvents Batching Lambda',
            role_name=(
                f'{self.target_environment.lower()}-{self.resource_name_prefix}'
                f'-{self.region}-gw-appevents-batching-lambda'
            ),
            assumed_by=iam.ServicePrincipal('lambda.amazonaws.com'),
            inline_policies=policies,
        )
        NagSuppressions.add_resource_suppressions(iam_role, [
            {
                'id': 'AwsSolutions-IAM5',
                'reason': 'S3 object-level actions require wildcard on key path'
            },
        ], apply_to_children=True)

        return iam_role

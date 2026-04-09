# !/usr/bin/env python3
# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import os
import aws_cdk as cdk
from cdk_nag import AwsSolutionsChecks, NagSuppressions

from lib.pipeline_stack import PipelineStack
from lib.code_commit_stack import CodeCommitStack
from lib.configuration import (
    ACCOUNT_ID, CODECOMMIT_MIRROR_REPOSITORY_NAME, DEPLOYMENT, DEV, TEST, PROD, REGION, CODE_BRANCH,
    get_logical_id_prefix, get_all_configurations, get_context_configuration
)
from lib.tagging import tag

app = cdk.App()

# Enable CDK Nag for the Mirror repository, Pipeline, and related stacks
# Environment stacks must be enabled on the Stage resource
cdk.Aspects.of(app).add(AwsSolutionsChecks())

# Get environment from context (default: DEV, can be overridden with --context env=Prod)
target_environment = app.node.try_get_context('env') or DEV
if target_environment not in [DEV, TEST, PROD]:
    raise ValueError(f'Invalid environment: {target_environment}. Use Dev, Test, or Prod')

# Get context-based configuration
context_config = get_context_configuration(app, target_environment)

# Build AWS environment from context
aws_env = cdk.Environment(
    account=context_config[ACCOUNT_ID],
    region=context_config[REGION]
)

# Legacy deployment configuration for CodeCommit mirror (not commonly used)
raw_mappings = get_all_configurations()
deployment_aws_env = {
    'account': raw_mappings[DEPLOYMENT][ACCOUNT_ID],
    'region': raw_mappings[DEPLOYMENT][REGION],
}
logical_id_prefix = get_logical_id_prefix()

if raw_mappings[DEPLOYMENT][CODECOMMIT_MIRROR_REPOSITORY_NAME] != '':
    mirror_repository_stack = CodeCommitStack(
        app,
        f'{DEPLOYMENT}-{logical_id_prefix}EtlMirrorRepository',
        description='InsuranceLake stack for ETL repository mirror (SO9489) (uksb-1tu7mtee2)',
        target_environment=DEPLOYMENT,
        env=deployment_aws_env,
    )
    tag(mirror_repository_stack, DEPLOYMENT)

# Create deployment stage with context configuration (replaces env-specific blocks)
from lib.pipeline_deploy_stage import PipelineDeployStage

stage = PipelineDeployStage(
    app, target_environment,
    target_environment=target_environment,
    env=aws_env,
    context_config=context_config,
)

# Apply tagging to cross-region support stacks (legacy from pipeline-based deployment)
for stack in app.node.children:
    # All other stacks in the app are custom constructs
    if type(stack) == cdk.Stack:
        # Use the deployment environment for tagging because there
        # is no way to determine 1:1 which pipeline created the stack
        tag(stack, DEPLOYMENT)

        NagSuppressions.add_resource_suppressions(stack, [
            {
                'id': 'AwsSolutions-S1',
                'reason': 'Cross-region support stack and bucket are auto-created by Codepipeline'
            },
            {
                'id': 'AwsSolutions-KMS5',
                'reason': 'Cross-region support stack and bucket are auto-created by Codepipeline'
            },
        ], apply_to_children=True)

app.synth()
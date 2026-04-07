# Guidewire ClaimCenter AppEvents Integration with AWS InsuranceLake

## Overview

This integration connects Guidewire ClaimCenter AppEvents to AWS InsuranceLake, enabling automated ingestion, transformation, and analytics of claim event data. A batching pipeline consolidates individual JSON events into optimized JSONL batches, which InsuranceLake processes through its standard Collect-Cleanse-Consume architecture.

## Architecture

```
Guidewire ClaimCenter (SaaS)
  │  Writes individual JSON events every ~10 minutes
  ▼
┌─────────────────────────────────────┐
│  Guidewire S3 Bucket                │
│  (gw-appevents-{account}-collect)   │
└──────────────┬──────────────────────┘
               │ S3 OBJECT_CREATED event
               ▼
┌─────────────────────────────────────┐
│  SQS Queue (buffer)                 │
│  + Dead Letter Queue (3 retries)    │
└──────────────┬──────────────────────┘
               │ EventBridge schedule (every 15 min)
               ▼
┌─────────────────────────────────────┐
│  Batching Lambda                    │
│  - Drains SQS queue                 │
│  - Reads JSONs from GW bucket       │
│  - Stringifies nested collections   │
│  - Writes consolidated JSONL        │
└──────────────┬──────────────────────┘
               │ Writes batch-{timestamp}.jsonl
               ▼
┌─────────────────────────────────────┐
│  InsuranceLake Collect Bucket       │
│  GWClaimCenter/Claims/              │
└──────────────┬──────────────────────┘
               │ S3 event triggers InsuranceLake
               ▼
┌─────────────────────────────────────┐
│  InsuranceLake ETL Pipeline         │
│  Step Functions → Glue Jobs         │
│                                     │
│  1. Collect → Cleanse               │
│     - Schema mapping (62 fields)    │
│     - Date conversions              │
│     - Data quality checks           │
│     - Write to Parquet              │
│                                     │
│  2. Cleanse → Consume               │
│     - Spark SQL (flat claims table) │
│     - Athena views (5 views for     │
│       nested data)                  │
└──────────────┬──────────────────────┘
               ▼
┌─────────────────────────────────────┐
│  Analytics Layer                    │
│  - Athena SQL queries               │
│  - QuickSight dashboards            │
│  - Redshift Spectrum                │
└─────────────────────────────────────┘
```

## Prerequisites

1. **AWS Account** with permissions to deploy CloudFormation, Lambda, SQS, Glue, Step Functions, S3, DynamoDB, Athena, KMS, IAM
2. **AWS CDK v2** installed (`npm install -g aws-cdk`)
3. **Python 3.9+** with pip
4. **AWS CLI v2** configured with credentials
5. **InsuranceLake Infrastructure** deployed (creates S3 buckets and KMS keys) — see [aws-insurancelake-infrastructure](https://github.com/aws-solutions-library-samples/aws-insurancelake-infrastructure)
6. **Guidewire AppEvents S3 bucket** already created and receiving events from Guidewire ClaimCenter

## Deployment Steps

### Step 1: Clone and Configure

```bash
git clone https://github.com/jay17june/aws-insurancelake-etl.git
cd aws-insurancelake-etl
git checkout feature/guidewire-appevents-integration
```

### Step 2: Update Configuration

Edit `lib/configuration.py` to set your environment values:

```python
# Set your AWS region (must match where GW bucket is located)
REGION: 'us-east-1',

# Set your Guidewire AppEvents bucket name for each environment
GUIDEWIRE_APPEVENTS_BUCKET: 'your-gw-appevents-bucket-name',
```

The Guidewire bucket name is configured per environment (DEV, TEST, PROD) in the `local_mapping` dictionary.

### Step 3: Deploy InsuranceLake Infrastructure (if not already deployed)

The ETL stack depends on S3 buckets and KMS keys created by the infrastructure stack:

```bash
cd /path/to/aws-insurancelake-infrastructure

# Update lib/configuration.py with your region
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Direct deploy (bypasses CodePipeline for quick setup)
cdk deploy --all --require-approval never --app "python3 deploy_direct.py"
```

This creates:
- S3 Collect bucket: `{env}-insurancelake-{account}-{region}-collect`
- S3 Cleanse bucket: `{env}-insurancelake-{account}-{region}-cleanse`
- S3 Consume bucket: `{env}-insurancelake-{account}-{region}-consume`
- S3 Access Logs bucket
- KMS encryption key

### Step 4: Deploy InsuranceLake ETL with Guidewire Integration

```bash
cd /path/to/aws-insurancelake-etl

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Verify synthesis
cdk synth --app "python3 deploy_direct.py"

# Deploy all stacks
cdk deploy --all --require-approval never --app "python3 deploy_direct.py"
```

This deploys 7 CloudFormation stacks:

| Stack | Resources |
|-------|-----------|
| `InsuranceLakeEtlDynamoDb` | 6 DynamoDB tables (audit, hash, lookup, multi-lookup, DQ results, lineage) |
| `InsuranceLakeEtlGlueBuckets` | Glue scripts and temp S3 buckets |
| `InsuranceLakeEtlAthenaWorkgroup` | Athena workgroup for SQL queries |
| `InsuranceLakeEtlGlueJobs` | 3 Glue jobs + all configuration files deployed to S3 |
| `InsuranceLakeEtlStepFunctions` | State machine, trigger Lambda, SNS notifications |
| `InsuranceLakeEtlDataLakeConsumer` | Consumer IAM policy |
| `InsuranceLakeEtlGuidewireAppEvents` | SQS queues, batching Lambda, EventBridge rule, S3 notification |

### Step 5: Verify Deployment

```bash
# Check all stacks are CREATE_COMPLETE
aws cloudformation list-stacks --region us-east-1 \
  --stack-status-filter CREATE_COMPLETE UPDATE_COMPLETE \
  --query 'StackSummaries[?contains(StackName, `InsuranceLake`)].StackName' \
  --output table

# Verify S3 notification on GW bucket
aws s3api get-bucket-notification-configuration \
  --bucket your-gw-appevents-bucket-name --region us-east-1

# Check SQS queue is receiving messages
aws sqs get-queue-attributes \
  --queue-url $(aws sqs get-queue-url --queue-name dev-insurancelake-gw-appevents-queue --region us-east-1 --query 'QueueUrl' --output text) \
  --attribute-names ApproximateNumberOfMessages --region us-east-1
```

## Testing

### Manual End-to-End Test

```bash
# 1. Invoke the batching Lambda manually
aws lambda invoke \
  --function-name dev-insurancelake-gw-appevents-batching \
  --region us-east-1 \
  --payload '{}' /tmp/response.json && cat /tmp/response.json

# 2. Check the JSONL file was written to the collect bucket
aws s3 ls s3://dev-insurancelake-{account}-us-east-1-collect/GWClaimCenter/Claims/

# 3. Monitor the Step Functions execution
aws stepfunctions list-executions \
  --state-machine-arn $(aws stepfunctions list-state-machines --region us-east-1 \
    --query 'stateMachines[?contains(name, `insurancelake`)].stateMachineArn' --output text) \
  --region us-east-1 --max-results 3 \
  --query 'executions[].{Name:name,Status:status}' --output table

# 4. Query the data in Athena
aws athena start-query-execution \
  --query-string "SELECT claimnumber, claimstate, lobcode, lossdate, insured_name FROM gwclaimcenter.claims LIMIT 10" \
  --work-group insurancelake --region us-east-1
```

### Query Examples

**Claims summary:**
```sql
SELECT claimnumber, claimstate, lobcode, lossdate, reporteddate,
       insured_name, losslocation_city, losslocation_statecode
FROM gwclaimcenter.claims
ORDER BY lossdate DESC
LIMIT 20;
```

**Claim activities:**
```sql
SELECT * FROM gwclaimcenter.vw_claim_activities
WHERE claimnumber = '000-00-009898';
```

**Claim exposures:**
```sql
SELECT * FROM gwclaimcenter.vw_claim_exposures
WHERE claimnumber = '000-00-009898';
```

**Claim reserves:**
```sql
SELECT * FROM gwclaimcenter.vw_claim_reserves
WHERE claimnumber = '000-00-009898';
```

**Claims by state:**
```sql
SELECT losslocation_statecode, claimstate, COUNT(*) as claim_count
FROM gwclaimcenter.claims
GROUP BY losslocation_statecode, claimstate
ORDER BY claim_count DESC;
```

## Configuration Files

All configuration files are located in `lib/glue_scripts/` and are automatically deployed to S3 by the `InsuranceLakeEtlGlueJobs` stack.

### Schema Mapping (`transformation-spec/GWClaimCenter-Claims.csv`)

Maps 62+ Guidewire AppEvents fields to InsuranceLake column names. Key patterns:

- **Enum struct extraction**: Guidewire uses `{code, name}` structs for enumerated values. The mapping extracts the `.code` value using backtick notation:
  ```
  state,null
  `state`.`code`,claimstate
  ```

- **Nested object flattening**: Complex objects (policy, lossLocation, insured, etc.) are nulled at the parent level and individual fields are extracted:
  ```
  policy,null
  `policy`.`policyNumber`,policy_policynumber
  `policy`.`policyType`.`code`,policytype
  ```

- **Nested collections preserved as strings**: Activities, contacts, exposures, reserves, and vehicle-incidents are kept as JSON strings for Athena UNNEST querying.

### Transform Specification (`transformation-spec/GWClaimCenter-Claims.json`)

| Transform | Fields | Description |
|-----------|--------|-------------|
| `date` | lossdate, reporteddate | Parse ISO 8601 timestamps to date type |
| `combinecolumns` | losslocation_full | Concatenate address components |
| `literal` | sourcesystem, eventtype | Add static metadata columns |

### Data Quality Rules (`dq-rules/dq-GWClaimCenter-Claims.json`)

| Type | Rules |
|------|-------|
| **Warn** | policynumber completeness > 90%, lobcode > 90%, reporteddate > 80%, insured_name > 80%, claimstate in valid values |
| **Halt** | claimnumber must exist and be complete, lossdate must exist and be complete |

### Consume SQL (`transformation-sql/spark-GWClaimCenter-Claims.sql`)

Creates the consume-layer claims table with flat claim fields and a computed `days_to_report` column.

### Athena Views (`transformation-sql/athena-GWClaimCenter-Claims.sql`)

| View | Source Field | Data Pattern | Key Columns |
|------|-------------|--------------|-------------|
| `vw_claim_activities` | activities | Map (key=activity ID) | subject, status, priority, dueDate, assignedUser |
| `vw_claim_exposures` | exposures | Map (key=exposure ID) | coverageType, state, claimantType, lossParty |
| `vw_claim_reserves` | reserves | Map (key=reserve ID) | costType, costCategory, reservingAmount, status |
| `vw_claim_contacts` | contacts | Map (key=contact ID) | displayName, contactType, city, state, email |
| `vw_claim_vehicle_incidents` | vehicle_incidents | Map (key=incident ID) | make, model, year, VIN, color, licensePlate |

## Data Flow Details

### Batching Lambda

The batching Lambda (`lib/guidewire_appevents_batching/lambda_handler.py`) runs every 15 minutes and:

1. **Drains SQS**: Receives up to 10 messages per API call, loops until queue is empty
2. **Reads JSON files**: Fetches each event JSON from the Guidewire S3 bucket
3. **Stringifies nested collections**: Converts `activities`, `contacts`, `exposures`, `reserves`, `vehicle-incidents`, `notes`, and `policyAddresses` from nested objects to JSON strings. This prevents Spark from inferring struct schemas with dynamic, colon-containing keys (e.g., `cc:17499`) that are incompatible with Hive/Parquet.
4. **Writes JSONL**: Consolidates all events into a single `batch-{timestamp}.jsonl` file in the collect bucket
5. **Deletes processed messages**: Only deletes SQS messages for successfully processed events; failed messages remain for retry (max 3 attempts before DLQ)

### Why Stringify Nested Collections?

Guidewire AppEvents use dynamic IDs as map keys (e.g., `activities: {"cc:17499": {...}, "cc:5087": {...}}`). When Spark reads this JSON, it infers struct schemas with colon-containing field names. These are incompatible with Hive metastore operations (table creation, partition management). By stringifying these fields in the Lambda, Spark reads them as plain strings, and Athena views parse them on demand using `json_parse()` and `UNNEST()`.

## AWS Resources Created

### Guidewire AppEvents Stack

| Resource | Name Pattern | Purpose |
|----------|-------------|---------|
| SQS Queue | `{env}-insurancelake-gw-appevents-queue` | Buffers S3 event notifications |
| SQS DLQ | `{env}-insurancelake-gw-appevents-dlq` | Failed message isolation (14-day retention) |
| Lambda | `{env}-insurancelake-gw-appevents-batching` | Consolidates events into JSONL batches |
| EventBridge Rule | `{env}-insurancelake-gw-appevents-schedule` | Triggers Lambda every 15 minutes |
| IAM Role | `{env}-insurancelake-{region}-gw-appevents-batching-lambda` | Least-privilege Lambda execution role |
| CloudWatch Log Group | `/aws/lambda/{env}-insurancelake-gw-appevents-batching` | Lambda execution logs |

### InsuranceLake Data Catalog

| Database | Table/View | Description |
|----------|-----------|-------------|
| `gwclaimcenter` | `claims` (cleanse) | All claim fields including nested JSON strings |
| `gwclaimcenter_consume` | `claims` (consume) | Flat claim fields with computed columns |
| `gwclaimcenter` | `vw_claim_activities` | Athena view - exploded activities |
| `gwclaimcenter` | `vw_claim_exposures` | Athena view - exploded exposures |
| `gwclaimcenter` | `vw_claim_reserves` | Athena view - exploded reserves |
| `gwclaimcenter` | `vw_claim_contacts` | Athena view - exploded contacts |
| `gwclaimcenter` | `vw_claim_vehicle_incidents` | Athena view - exploded vehicle incidents |

## Monitoring

### CloudWatch Metrics

- **Lambda**: Invocations, Errors, Duration, Throttles
- **SQS**: ApproximateNumberOfMessages, ApproximateAgeOfOldestMessage
- **Glue Jobs**: Job run status, duration
- **Step Functions**: Execution status, duration

### Key Logs

| Log Group | Content |
|-----------|---------|
| `/aws/lambda/{env}-insurancelake-gw-appevents-batching` | Batching Lambda execution (events processed, failures) |
| `/aws/lambda/{env}-insurancelake-etl-trigger` | InsuranceLake pipeline trigger events |
| `/aws-glue/jobs/output` | Glue job processing logs (transforms, DQ results) |
| `/aws-glue/jobs/error` | Glue job error details |

### Alerts

The InsuranceLake Step Functions state machine publishes to an SNS topic on pipeline completion (success or failure). Subscribe to `{env}-insurancelake-etl-notification-topic` for email/Slack alerts.

## Troubleshooting

| Issue | Cause | Resolution |
|-------|-------|------------|
| SQS queue not receiving messages | S3 event notification not configured | Check `aws s3api get-bucket-notification-configuration` on GW bucket |
| Lambda returns "No messages to process" | Queue is empty or messages are in-flight | Check SQS queue depth and visibility timeout |
| Glue job fails with "no viable alternative at input" | Nested struct with colon-containing keys in Hive | Ensure batching Lambda stringifies nested collections |
| Glue job fails with "CAST" error | Field is a struct, not a scalar | Update schema mapping to extract `.code` from enum structs |
| Glue job fails with "lookup data not found" | DynamoDB lookup table not populated | Either populate lookup tables or remove lookup transforms |
| Athena view fails with "Column alias list" | Map UNNEST needs two aliases (key, value) | Use `as t(key_alias, value_alias)` syntax |
| DQ halt rule fails | Data quality issue in source events | Check Glue output logs for specific rule failures |

## Customization

### Adding New Guidewire Event Types

To integrate additional Guidewire sources (e.g., PolicyCenter, BillingCenter):

1. Create a new schema mapping CSV: `transformation-spec/GWPolicyCenter-Policies.csv`
2. Create a new transform spec: `transformation-spec/GWPolicyCenter-Policies.json`
3. Create DQ rules: `dq-rules/dq-GWPolicyCenter-Policies.json`
4. Create consume SQL: `transformation-sql/spark-GWPolicyCenter-Policies.sql`
5. Update the batching Lambda's `TARGET_PREFIX` environment variable or create a separate Lambda for each source

### Adding DynamoDB Lookups

To enrich data with lookup values (e.g., LOB code → LOB name):

1. Load lookup data into DynamoDB: `dev-insurancelake-etl-value-lookup`
2. Add lookup transforms to the transform spec:
   ```json
   "lookup": [
       {
           "field": "lobname",
           "source": "lobcode",
           "lookup": "GWLOBCode",
           "nomatch": "Unknown"
       }
   ]
   ```
3. Use the `resources/load_dynamodb_lookup_table.py` script to populate lookup tables

### Adjusting Batch Interval

The EventBridge schedule is configured in `lib/guidewire_appevents_stack.py`:

```python
schedule=events.Schedule.rate(cdk.Duration.minutes(15))
```

Change `15` to your desired interval and redeploy.

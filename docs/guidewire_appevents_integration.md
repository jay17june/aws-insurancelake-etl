# Guidewire ClaimCenter AppEvents Integration with AWS InsuranceLake

## Overview

This integration connects Guidewire ClaimCenter AppEvents to AWS InsuranceLake, enabling automated ingestion, transformation, and analytics of claim event data. A batching pipeline consolidates individual JSON events into optimized JSONL batches, **split by event type** into separate InsuranceLake tables (Claims, Exposures, Payments). The **cleanse layer accumulates all events** (append-only) preserving full history for audit and compliance, while the **consume layer deduplicates to current-state** tables optimized for analytics.

## Architecture

```
Guidewire ClaimCenter (SaaS)
  │  Writes individual JSON events every ~10 minutes
  │  Event types: ClaimCreated, ClaimChanged, ExposureAdded,
  │               ExposureChanged, PaymentCreated, PaymentChanged
  ▼
┌─────────────────────────────────────┐
│  Guidewire S3 Bucket                │
│  (gw-appevents-{account}-collect)   │
│  Key pattern:                       │
│  cc:NNNN/cc:NNNN-{EventType}-      │
│         {timestamp}-{suffix}.json   │
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
│  - Classifies events by type        │
│  - Stringifies nested collections   │
│  - Writes separate JSONL per group  │
└──────┬───────────┬──────────┬───────┘
       │           │          │
       ▼           ▼          ▼
┌────────────┐┌──────────┐┌──────────┐
│ Claims/    ││Exposures/││Payments/ │
│ batch.jsonl││batch.jsonl││batch.jsonl│
└─────┬──────┘└────┬─────┘└────┬─────┘
      │            │           │
      ▼            ▼           ▼
┌─────────────────────────────────────┐
│  InsuranceLake Collect Bucket       │
│  GWClaimCenter/Claims/              │
│  GWClaimCenter/Exposures/           │
│  GWClaimCenter/Payments/            │
└──────────────┬──────────────────────┘
               │ S3 events trigger 3 pipelines
               ▼
┌─────────────────────────────────────┐
│  InsuranceLake ETL Pipeline (x3)    │
│  Step Functions → Glue Jobs         │
│                                     │
│  1. Collect → Cleanse               │
│     - Schema mapping + transforms   │
│     - Data quality checks           │
│     - APPEND to partition           │
│       (partition_append: true)      │
│     - Full event history preserved  │
│                                     │
│  2. Cleanse → Consume               │
│     - Reads ALL cleanse data        │
│     - Deduplicates by entity key    │
│       (ROW_NUMBER, latest wins)     │
│     - Writes current-state table    │
└──────────────┬──────────────────────┘
               ▼
┌─────────────────────────────────────┐
│  Analytics Layer (Athena)           │
│                                     │
│  CLEANSE (all events, append-only): │
│  gwclaimcenter.claims               │
│  gwclaimcenter.exposures            │
│  gwclaimcenter.payments             │
│                                     │
│  CONSUME (current state, deduped):  │
│  gwclaimcenter_consume.claims       │
│  gwclaimcenter_consume.exposures    │
│  gwclaimcenter_consume.payments     │
│                                     │
│  VIEWS (nested JSON exploded):      │
│  gwclaimcenter.vw_claim_activities  │
│  gwclaimcenter.vw_claim_exposures   │
│  gwclaimcenter.vw_claim_reserves    │
│  gwclaimcenter.vw_claim_contacts    │
│  gwclaimcenter.vw_claim_vehicle_*   │
└─────────────────────────────────────┘
```

## Event Type Routing

The batching Lambda classifies each AppEvent by parsing the event type from the S3 key and routes it to the appropriate InsuranceLake table:

| Event Type | S3 Key Pattern | Target Table | Description |
|-----------|---------------|-------------|-------------|
| `ClaimCreated` | `cc:NNNN-ClaimCreated-*.json` | `GWClaimCenter/Claims/` | New claim with full context |
| `ClaimChanged` | `cc:NNNN-ClaimChanged-*.json` | `GWClaimCenter/Claims/` | Claim updates with activities, notes, reserves |
| `ExposureAdded` | `cc:NNNN-ExposureAdded-*.json` | `GWClaimCenter/Exposures/` | New exposure on a claim |
| `ExposureChanged` | `cc:NNNN-ExposureChanged-*.json` | `GWClaimCenter/Exposures/` | Exposure status changes |
| `PaymentCreated` | `cc:NNNN-PaymentCreated-*.json` | `GWClaimCenter/Payments/` | New payment with financial details |
| `PaymentChanged` | `cc:NNNN-PaymentChanged-*.json` | `GWClaimCenter/Payments/` | Payment status changes |

**Why split by event type?** Each event type has a different schema. Claim events have `lossDate`, `lossLocation`, `insured`, and `contacts`. Payment events have `amount`, `checkNumber`, `payee`, and `lineItems` but no loss details. Mixing them in one table causes data quality failures (e.g., `lossDate` completeness drops to 58% because payment events don't carry it).

## Data Layer Model

InsuranceLake uses a 3-layer architecture. For Guidewire AppEvents, each layer serves a distinct purpose:

```
┌──────────────────────────────────────────────────────────────────┐
│  COLLECT (Raw)                                                    │
│  S3: GWClaimCenter/Claims/batch-20260407151126.jsonl             │
│  Format: JSONL (one JSON event per line)                          │
│  Retention: All batch files preserved                             │
└──────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌──────────────────────────────────────────────────────────────────┐
│  CLEANSE (All Events - Append Only)                               │
│  S3: Parquet, partitioned by year/month/day                       │
│  gwclaimcenter.claims      → 15 events (includes duplicates)     │
│  gwclaimcenter.exposures   → 13 events                           │
│  gwclaimcenter.payments    →  3 events                           │
│                                                                   │
│  Every 15-min batch APPENDS to the partition (no overwrite).      │
│  Full event history preserved for audit, compliance, and          │
│  point-in-time analysis.                                          │
│                                                                   │
│  Enabled by: "partition_append": true in input_spec               │
└──────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌──────────────────────────────────────────────────────────────────┐
│  CONSUME (Current State - Deduplicated)                           │
│  S3: Parquet, rebuilt from full cleanse on every pipeline run      │
│  gwclaimcenter_consume.claims      → 10 unique claims            │
│  gwclaimcenter_consume.exposures   → 13 unique exposures         │
│  gwclaimcenter_consume.payments    →  3 unique payments          │
│                                                                   │
│  Spark SQL reads ALL cleanse data, deduplicates using             │
│  ROW_NUMBER() OVER (PARTITION BY entity_key ORDER BY              │
│  execution_id DESC), keeping only the latest event per entity.    │
│                                                                   │
│  Overwrites consume table on each run (intentional — always       │
│  rebuilt from complete cleanse history).                           │
└──────────────────────────────────────────────────────────────────┘
```

### Why This Design?

| Stakeholder | Need | Layer |
|------------|------|-------|
| **Claims Adjusters** | Current state of each claim (one row, latest data) | Consume |
| **Finance** | Payment totals per claim (join claims + payments) | Consume |
| **Actuaries** | Point-in-time loss development, all state transitions | Cleanse |
| **Regulators** | Full audit trail of every change | Cleanse |
| **Executives** | Claims by LOB, geography, trends | Consume |
| **Data Engineers** | Raw event payloads for debugging | Collect |

### Partition Append Mode

Standard InsuranceLake behavior is to **overwrite** each partition on every pipeline run (`clear_partition`). This works for full-load sources but destroys data for incremental event streams.

The `partition_append` option (added to `input_spec` in the transform spec) skips the partition clear:

```json
{
    "input_spec": {
        "partition_append": true,
        ...
    }
}
```

When enabled:
- Each batch **appends** new parquet files to the existing daily partition
- Multiple batches per day coexist in the same `year=YYYY/month=MM/day=DD/` partition
- The cleanse table grows with every pipeline run
- The consume SQL handles deduplication by reading ALL cleanse data

This option is **backwards-compatible** — existing tables default to `false` (overwrite behavior preserved).

### Current-State Deduplication

The consume Spark SQL deduplicates events to produce one row per entity:

```sql
-- Claims: one row per claim, latest event wins
SELECT ...
FROM (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY claimnumber
        ORDER BY execution_id DESC
    ) as rn
    FROM gwclaimcenter.claims
)
WHERE rn = 1
```

| Table | Dedup Key | Ordering | Effect |
|-------|-----------|----------|--------|
| Claims | `claimnumber` | `execution_id DESC` | Latest ClaimCreated or ClaimChanged per claim |
| Exposures | `claimid` | `execution_id DESC` | Latest ExposureAdded or ExposureChanged per claim |
| Payments | `paymentid` | `execution_id DESC` | Latest PaymentCreated or PaymentChanged per payment |

The `execution_id` is a UUID assigned by InsuranceLake's trigger Lambda for each pipeline run, ensuring deterministic ordering of batches.

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
# Expected: "Batched 10 events (Claims:5, Exposures:4, Payments:1)"

# 2. Check JSONL files were written to separate paths in collect bucket
aws s3 ls s3://dev-insurancelake-{account}-us-east-1-collect/GWClaimCenter/Claims/
aws s3 ls s3://dev-insurancelake-{account}-us-east-1-collect/GWClaimCenter/Exposures/
aws s3 ls s3://dev-insurancelake-{account}-us-east-1-collect/GWClaimCenter/Payments/

# 3. Monitor the Step Functions executions (one per table)
aws stepfunctions list-executions \
  --state-machine-arn $(aws stepfunctions list-state-machines --region us-east-1 \
    --query 'stateMachines[?contains(name, `insurancelake`)].stateMachineArn' --output text) \
  --region us-east-1 --max-results 5 \
  --query 'executions[].{Name:name,Status:status}' --output table

# 4. Query each table in Athena
aws athena start-query-execution \
  --query-string "SELECT COUNT(*) FROM gwclaimcenter.claims" \
  --work-group insurancelake --region us-east-1
```

### Query Examples

**Claims with loss details:**
```sql
SELECT claimnumber, claimstate, lobcode, lossdate, reporteddate,
       datediff(reporteddate, lossdate) as days_to_report,
       insured_name, losslocation_city, losslocation_statecode
FROM gwclaimcenter_consume.claims
ORDER BY lossdate DESC
LIMIT 20;
```

**Exposures by claim:**
```sql
SELECT claimnumber, claimstate, lobcode, lossdate,
       losslocation_statecode, coverageinquestion
FROM gwclaimcenter_consume.exposures
WHERE claimnumber = '000-00-001051';
```

**Payments by claim:**
```sql
SELECT claimnumber, paymentid, lobcode, paymentstatus,
       costtype, costcategory, coverage, checknumber, createtime
FROM gwclaimcenter_consume.payments
ORDER BY createtime DESC
LIMIT 20;
```

**Join claims and payments:**
```sql
SELECT c.claimnumber, c.claimstate, c.lobcode, c.lossdate,
       p.paymentid, p.paymentstatus, p.costtype, p.checknumber
FROM gwclaimcenter_consume.claims c
JOIN gwclaimcenter_consume.payments p
  ON c.claimnumber = p.claimnumber
ORDER BY c.lossdate DESC;
```

**Claim activities (from nested JSON via Athena view):**
```sql
SELECT * FROM gwclaimcenter.vw_claim_activities
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
FROM gwclaimcenter_consume.claims
GROUP BY losslocation_statecode, claimstate
ORDER BY claim_count DESC;
```

**Event history for a claim (from cleanse, all events):**
```sql
SELECT claimnumber, claimstate, execution_id, year, month, day
FROM gwclaimcenter.claims
WHERE claimnumber = '000-00-005976'
ORDER BY execution_id;
```

**Claims with multiple events (state transitions):**
```sql
SELECT claimnumber, COUNT(*) as event_count
FROM gwclaimcenter.claims
GROUP BY claimnumber
HAVING COUNT(*) > 1
ORDER BY event_count DESC;
```

**Verify deduplication (cleanse vs consume row counts):**
```sql
-- Cleanse: all events (grows with each batch)
SELECT COUNT(*) as total_events FROM gwclaimcenter.claims;

-- Consume: unique claims (deduplicated)
SELECT COUNT(*) as unique_claims FROM gwclaimcenter_consume.claims;
```

## Table Schemas

### Claims Table (`gwclaimcenter.claims`)

Source events: `ClaimCreated`, `ClaimChanged`

| Category | Fields |
|----------|--------|
| **Claim Identity** | claimid, claimnumber, claimstate, description |
| **Classification** | lobcode, losstype, losscause, segment, faultrating |
| **Dates** | lossdate (date), reporteddate (date) |
| **Policy** | policynumber, policy_policynumber, policytype, producercode, policycurrency |
| **Location** | losslocation_address1, losslocation_city, losslocation_statecode, losslocation_postalcode, losslocation_county, losslocation_full |
| **People** | insured_name, insured_contactid, maincontact_name/id, reporter_name/id, assigneduser_name/id, assignedgroup_name/id, assignedbyuser_name/id |
| **Status** | flagged, howreported, incidentonly, assignmentstatus, validationlevel, strategycode |
| **Nested JSON** | activities, contacts, exposures, reserves, policyaddresses, vehicle_incidents, notes |
| **Metadata** | sourcesystem, eventtype, execution_id, year, month, day |

### Exposures Table (`gwclaimcenter.exposures`)

Source events: `ExposureAdded`, `ExposureChanged`

| Category | Fields |
|----------|--------|
| **Claim Context** | claimid, claimnumber, claimstate, lobcode, losstype, losscause, lossdate, reporteddate |
| **Policy** | policynumber, policy_policynumber, policytype, policyeffectivedate, policyexpirationdate |
| **Location** | losslocation_address1, losslocation_city, losslocation_statecode, losslocation_postalcode |
| **Exposure Detail** | coverageinquestion, assignmentstatus, faultrating, segment |
| **Nested JSON** | exposures, contacts, vehicleincidents |
| **Metadata** | sourcesystem, eventtype, execution_id, year, month, day |

### Payments Table (`gwclaimcenter.payments`)

Source events: `PaymentCreated`, `PaymentChanged`

| Category | Fields |
|----------|--------|
| **Payment Identity** | paymentid, claimid, claimnumber |
| **Classification** | lobcode, losstype, segment, costtype, costcategory, coverage |
| **Payment Details** | paymenttype, paymentstatus, currency, createdvia, checknumber |
| **Timestamps** | createtime (timestamp), issuedate (timestamp) |
| **References** | exposure_name, exposure_id, reserve_id, assigneduser_name/id, assignedgroup_name/id |
| **Nested JSON** | amount, transactionamount, lineitems, payee |
| **Metadata** | sourcesystem, eventtype, validationlevel, execution_id, year, month, day |

## Configuration Files

All configuration files are located in `lib/glue_scripts/` and are automatically deployed to S3 by the `InsuranceLakeEtlGlueJobs` stack.

### Per-Table Configuration

Each table has a complete set of InsuranceLake configuration files:

| File | Claims | Exposures | Payments |
|------|--------|-----------|----------|
| Schema Mapping | `GWClaimCenter-Claims.csv` | `GWClaimCenter-Exposures.csv` | `GWClaimCenter-Payments.csv` |
| Transform Spec | `GWClaimCenter-Claims.json` | `GWClaimCenter-Exposures.json` | `GWClaimCenter-Payments.json` |
| DQ Rules | `dq-GWClaimCenter-Claims.json` | `dq-GWClaimCenter-Exposures.json` | `dq-GWClaimCenter-Payments.json` |
| Consume SQL | `spark-GWClaimCenter-Claims.sql` | `spark-GWClaimCenter-Exposures.sql` | `spark-GWClaimCenter-Payments.sql` |
| Athena Views | `athena-GWClaimCenter-Claims.sql` | — | — |

### Schema Mapping Patterns

All schema mappings follow the same Guidewire-specific patterns:

- **Enum struct extraction**: Guidewire uses `{code, name}` structs for enumerated values. The mapping extracts the `.code` value:
  ```
  state,null
  `state`.`code`,claimstate
  ```

- **Nested object flattening**: Complex objects are nulled at the parent level and individual fields are extracted:
  ```
  policy,null
  `policy`.`policyNumber`,policy_policynumber
  `policy`.`policyType`.`code`,policytype
  ```

- **Nested collections preserved as strings**: Collections with dynamic keys (activities, contacts, exposures, reserves) are kept as JSON strings for Athena UNNEST querying.

### Data Quality Rules

| Table | Warn Rules | Halt Rules |
|-------|-----------|------------|
| **Claims** | policynumber > 90%, lobcode > 90%, reporteddate > 90%, insured_name > 80%, claimstate in valid values | claimnumber complete, lossdate complete |
| **Exposures** | lobcode > 90%, lossdate > 90% | claimnumber complete |
| **Payments** | policynumber > 90%, paymentstatus > 90% | claimnumber complete, paymentid complete |

### Athena Views (Claims only)

Nested JSON collections in the Claims cleanse table are queryable via 5 Athena views:

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
2. **Classifies events**: Parses event type from S3 key using regex pattern `cc:\d+-(\w+)-\d{8}T\d{6}Z-\d+\.json` and routes to the correct table
3. **Reads JSON files**: Fetches each event JSON from the Guidewire S3 bucket
4. **Stringifies nested collections**: Converts nested objects to JSON strings using table-specific field lists to prevent Spark struct inference issues
5. **Writes separate JSONL files**: One `batch-{timestamp}.jsonl` per event group (Claims, Exposures, Payments) — skips groups with 0 events
6. **Deletes processed messages**: Only deletes SQS messages for successfully processed events; failed messages remain for retry (max 3 attempts before DLQ)

### Event Type Classification

The Lambda extracts the event type from each S3 key:

```
S3 Key: cc:9898/cc:9898-ClaimCreated-20260406T204224Z-223.json
                        ^^^^^^^^^^^^
                        Event type extracted via regex
```

Routing map:
```python
EVENT_TYPE_ROUTING = {
    'ClaimCreated':    'Claims',
    'ClaimChanged':    'Claims',
    'ExposureAdded':   'Exposures',
    'ExposureChanged': 'Exposures',
    'PaymentCreated':  'Payments',
    'PaymentChanged':  'Payments',
}
```

### Why Stringify Nested Collections?

Guidewire AppEvents use dynamic IDs as map keys (e.g., `activities: {"cc:17499": {...}, "cc:5087": {...}}`). When Spark reads this JSON, it infers struct schemas with colon-containing field names. These are incompatible with Hive metastore operations (table creation, partition management). By stringifying these fields in the Lambda, Spark reads them as plain strings, and Athena views parse them on demand using `json_parse()` and `UNNEST()`.

Each table has its own stringify field list since the nested structures differ:

| Table | Stringified Fields |
|-------|--------------------|
| Claims | activities, contacts, exposures, reserves, vehicle-incidents, notes, policyAddresses |
| Exposures | contacts, exposures, vehicleIncidents, allValidationLevelsReached, policyAddresses |
| Payments | amount, transactionAmount, lineItems, payee |

## AWS Resources Created

### Guidewire AppEvents Stack

| Resource | Name Pattern | Purpose |
|----------|-------------|---------|
| SQS Queue | `{env}-insurancelake-gw-appevents-queue` | Buffers S3 event notifications |
| SQS DLQ | `{env}-insurancelake-gw-appevents-dlq` | Failed message isolation (14-day retention) |
| Lambda | `{env}-insurancelake-gw-appevents-batching` | Classifies events and writes JSONL per type |
| EventBridge Rule | `{env}-insurancelake-gw-appevents-schedule` | Triggers Lambda every 15 minutes |
| IAM Role | `{env}-insurancelake-{region}-gw-appevents-batching-lambda` | Least-privilege Lambda execution role |
| CloudWatch Log Group | `/aws/lambda/{env}-insurancelake-gw-appevents-batching` | Lambda execution logs |

### InsuranceLake Data Catalog

**Cleanse Layer** (all events, append-only, full history):

| Database | Table | Source | Pattern |
|----------|-------|--------|---------|
| `gwclaimcenter` | `claims` | ClaimCreated, ClaimChanged | Append-only, multiple events per claim |
| `gwclaimcenter` | `exposures` | ExposureAdded, ExposureChanged | Append-only, multiple events per exposure |
| `gwclaimcenter` | `payments` | PaymentCreated, PaymentChanged | Append-only, multiple events per payment |

**Consume Layer** (current state, deduplicated, rebuilt on each run):

| Database | Table | Dedup Key | Description |
|----------|-------|-----------|-------------|
| `gwclaimcenter_consume` | `claims` | claimnumber | One row per claim, latest state, includes `days_to_report` |
| `gwclaimcenter_consume` | `exposures` | claimid | One row per claim's exposure set, latest state |
| `gwclaimcenter_consume` | `payments` | paymentid | One row per payment, latest status |

**Athena Views** (nested JSON exploded from cleanse layer):

| Database | View | Source |
|----------|------|--------|
| `gwclaimcenter` | `vw_claim_activities` | Exploded activities from claims cleanse |
| `gwclaimcenter` | `vw_claim_exposures` | Exploded exposures from claims cleanse |
| `gwclaimcenter` | `vw_claim_reserves` | Exploded reserves from claims cleanse |
| `gwclaimcenter` | `vw_claim_contacts` | Exploded contacts from claims cleanse |
| `gwclaimcenter` | `vw_claim_vehicle_incidents` | Exploded vehicle incidents from claims cleanse |

## Monitoring

### CloudWatch Metrics

- **Lambda**: Invocations, Errors, Duration, Throttles
- **SQS**: ApproximateNumberOfMessages, ApproximateAgeOfOldestMessage
- **Glue Jobs**: Job run status, duration
- **Step Functions**: Execution status, duration

### Key Logs

| Log Group | Content |
|-----------|---------|
| `/aws/lambda/{env}-insurancelake-gw-appevents-batching` | Batching Lambda execution (events classified and processed per type) |
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
| Lambda logs "Unknown event type in key" | New AppEvent type not in routing map | Add the event type to `EVENT_TYPE_ROUTING` in `lambda_handler.py` |
| Only some tables get JSONL files | Not all event types present in batch | Normal behavior — Lambda skips tables with 0 events |
| Glue job fails with "no viable alternative at input" | Nested struct with colon-containing keys | Ensure the event type's stringify fields list is correct in `STRINGIFY_FIELDS` |
| Glue job fails with "CAST" error | Field is a struct, not a scalar | Update schema mapping to extract `.code` from enum structs |
| Glue job fails with "lookup data not found" | DynamoDB lookup table not populated | Either populate lookup tables or remove lookup transforms |
| Athena view fails with "Column alias list" | Map UNNEST needs two aliases (key, value) | Use `as t(key_alias, value_alias)` syntax |
| DQ halt rule fails on Claims | Non-claim events mixed in Claims table | Verify Lambda routing — check S3 key pattern matches regex |
| DQ halt rule fails on Payments | Missing paymentid or claimnumber | Check source data quality in Guidewire |
| Cleanse table only has latest batch | `partition_append` not set to `true` | Add `"partition_append": true` to `input_spec` in transform spec |
| Consume table has duplicate rows | Dedup key mismatch or missing `ROW_NUMBER()` in consume SQL | Verify the `PARTITION BY` key matches the entity's unique identifier |
| Schema change error on Payments | PaymentCreated/Changed have different fields | Set `"allow_schema_change": "permissive"` in Payments transform spec |
| Cleanse partition grows indefinitely | Expected behavior with `partition_append` | Implement periodic compaction or lifecycle rules on old partitions |

## Customization

### Adding New AppEvent Types

To add a new Guidewire event type (e.g., `ReserveCreated`):

1. Add the routing in `lambda_handler.py`:
   ```python
   EVENT_TYPE_ROUTING = {
       ...
       'ReserveCreated': 'Reserves',  # new
   }
   ```
2. Add stringify fields for the new table:
   ```python
   STRINGIFY_FIELDS = {
       ...
       'Reserves': ['lineItems', 'exposure'],  # new
   }
   ```
3. Create InsuranceLake config files:
   - `transformation-spec/GWClaimCenter-Reserves.csv`
   - `transformation-spec/GWClaimCenter-Reserves.json`
   - `dq-rules/dq-GWClaimCenter-Reserves.json`
   - `transformation-sql/spark-GWClaimCenter-Reserves.sql`
4. Redeploy: `cdk deploy InsuranceLakeEtlGlueJobs InsuranceLakeEtlGuidewireAppEvents`

### Adding New Guidewire Products

To integrate other Guidewire products (e.g., PolicyCenter, BillingCenter):

1. Create a separate SQS queue and Lambda (or extend the existing Lambda with product-specific routing)
2. Use a different source system prefix: `GWPolicyCenter/Policies/`
3. Create product-specific schema mapping, transform, DQ, and SQL files

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

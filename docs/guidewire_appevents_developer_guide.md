---
title: Guidewire AppEvents Developer Guide
parent: Developer Documentation
nav_order: 6
last_modified_date: 2026-04-09
---
# Guidewire ClaimCenter AppEvents Integration Developer Guide
{: .no_toc }

This section provides detailed implementation information for developers working with or extending the Guidewire AppEvents integration.

For user documentation on the Guidewire AppEvents integration, refer to the [Guidewire AppEvents User Documentation](guidewire_appevents.md).

## Contents
{: .no_toc }

* TOC
{:toc}

## Architecture Deep Dive

### Data Layer Model

The integration uses InsuranceLake's 3-layer architecture with custom partition behavior:

![Data Layer Model](guidewire-appevents-data-layer-model.drawio)

**Collect Layer**: Raw JSONL batches from Lambda batching function
**Cleanse Layer**: Append-only Parquet tables with full event history (enabled by `cleanse_partition_append: true`)
**Consume Layer**: Current-state tables with ROW_NUMBER() deduplication

### Why This Design?

The 3-layer architecture with append-only cleanse and deduplicated consume serves distinct analytical needs across insurance operations:

#### Claims Adjusters → Consume Layer (Current State)
**Need**: Daily claim management requires the latest status of each claim for assignment, processing, and customer communication.
**Why Consume**: Single row per claim with current claimstate, assigneduser, faultrating. No need to sort through historical state transitions.
**Query Pattern**: `WHERE claimstate = 'open' AND assigneduser = 'John Adjuster'`

#### Finance Teams → Consume Layer (Aggregated Views)
**Need**: Payment reporting, reserve analysis, and financial reconciliation require current balances and cleared payment totals.
**Why Consume**: Can JOIN claims + payments tables for complete financial picture without duplicate payment records.
**Query Pattern**: `SUM(payment_amount) WHERE paymentstatus = 'cleared' GROUP BY claimnumber`

#### Actuaries → Cleanse Layer (Full Event History)
**Need**: Loss development triangles require every state transition to calculate reserve adequacy and premium pricing models.
**Why Cleanse**: Complete chronological sequence of claim events (open → assigned → investigated → closed) with exact timestamps.
**Query Pattern**: `WHERE claimnumber = 'CLM-123' ORDER BY reporteddate` to analyze claim lifecycle patterns

#### Regulators → Cleanse Layer (Audit Trail)
**Need**: Compliance reporting requires immutable record of every change with no data loss for regulatory examinations.
**Why Cleanse**: Full audit trail preserved; can demonstrate when/how claim decisions were made and by whom.
**Query Pattern**: Event-level analysis with complete data lineage and user attribution

#### Executives → Consume Layer (Business Intelligence)
**Need**: Dashboard metrics, trend analysis, and KPI reporting require consistent snapshots for period-over-period comparisons.
**Why Consume**: Clean, deduplicated data optimized for aggregation without concern for event-level complexity.
**Query Pattern**: `GROUP BY lobcode, losslocation_statecode, month` for geographic and line-of-business trending

#### Data Engineers → Collect Layer (Debugging)
**Need**: Root cause analysis for data quality issues requires access to original Guidewire event payloads.
**Why Collect**: Raw JSONL preserves complete original structure for troubleshooting schema mapping or transformation issues.

This separation ensures each stakeholder gets data optimized for their analytical workflows without compromising others' needs.

### Event Type Classification

The batching Lambda (`lib/guidewire_appevents_batching/lambda_handler.py`) classifies events using regex pattern matching on S3 keys:

```python
# S3 key pattern: cc:NNNN/cc:NNNN-{EventType}-timestamp.json
EVENT_TYPE_PATTERN = re.compile(r'cc:\d+-(\w+)-\d{8}T\d{6}Z-\d+\.json$')

EVENT_TYPE_ROUTING = {
    'ClaimCreated': 'Claims',
    'ClaimChanged': 'Claims',
    'ExposureAdded': 'Exposures',
    'ExposureChanged': 'Exposures',
    'PaymentCreated': 'Payments',
    'PaymentChanged': 'Payments',
}
```

### Dynamic Schema Processing

**Major Architectural Innovation**: The integration uses **dynamic schema processing** that eliminates the need for predefined schema mappings and automatically adapts to any Guidewire payload structure.

**How Dynamic Schema Works:**

Instead of fixed schema mappings, InsuranceLake's native `clean_column_names()` function automatically:

1. **Processes ALL fields** in each event (no predefined list needed)
2. **Auto-cleans field names** for Parquet/Athena compatibility:
   - `claimNumber` → `claimnumber`
   - `lossLocation.city` → `losslocation_city`
   - `state.code` → `state_code`
   - `brandNewField` → `brandnewfield`
3. **Preserves ALL data** while making it query-friendly
4. **Handles any payload variation** without configuration changes

**Original Guidewire Event** (any structure):
```json
{
  "id": "cc:9898",
  "claimNumber": "000-00-009898",
  "state": {"code": "open", "name": "Open"},
  "activities": {"cc:17499": {"subject": "Test"}},
  "newGuidewireField": "Added in future release",
  "customEnhancement": {"version": 2.0, "data": "Complex new structure"}
}
```

**Automatic Processing Result**:
```sql
-- ALL fields automatically available in Athena:
SELECT
  claimnumber,           -- Auto-cleaned from claimNumber
  state_code,            -- Auto-flattened from state.code
  activities,            -- Stringified to preserve nested JSON
  newguidewirefield,     -- New field automatically included
  customenhancement      -- Complex structures preserved as JSON strings
FROM gwclaimcenter.claims;
```

**Benefits of Dynamic Schema:**
- ✅ **No schema mapping files needed** — Zero configuration for new fields
- ✅ **Automatic field preservation** — ALL Guidewire data included
- ✅ **Future-proof** — New Guidewire releases work immediately
- ✅ **Zero maintenance** — No code updates for schema changes
- ✅ **Eliminates missing field errors** — Any payload structure works

**Why Stringify Nested Collections?**

Although InsuranceLake has robust field name escaping capabilities (`escape_field_name()` in `custom_mapping.py`) that handle special characters in schema mappings, the stringify approach addresses a different technical challenge: **Spark schema inference** vs **explicit field mapping**.

**The Technical Issue:**

1. **Schema Inference Phase**: When Spark reads raw Guidewire JSON, it automatically infers struct schemas like:
   ```
   contacts.cc:65066.displayName
   exposures.cc:58446.claimantType.code
   vehicle-incidents.cc:40379.vehicle.make
   ```

2. **Hive Metastore Incompatibility**: These inferred schemas with colon-containing field names cause Hive operations to fail (e.g., `purgeTable` during partition management) with errors like "no viable alternative at input 'cc:17499'".

3. **Field Escaping Limitation**: InsuranceLake's field escaping works excellently for **explicit field references** in schema mappings (e.g., `` `activities.cc:12345`.`subject` `` → `activity_subject`), but cannot prevent the **automatic schema inference** that happens when Spark first reads the JSON.

**The Stringify Solution:**

By converting nested collections to JSON strings in the Lambda (before InsuranceLake processes them), Spark reads them as simple `StringType` fields instead of inferring complex struct schemas. This approach:

- **Prevents problematic schema inference** while preserving all data
- **Maintains queryability** through Athena views with `json_parse()` and `UNNEST()`
- **Works alongside field escaping** for the best of both techniques
- **Ensures Hive compatibility** for all metastore operations

**Complementary Techniques:**
- **Field Escaping**: Handles explicit field references in schema mappings
- **Stringify Preprocessing**: Prevents automatic schema inference with dynamic keys
- **Together**: Enable complete processing of complex Guidewire event structures

## Configuration Files

### Simplified Configuration for Dynamic Schema

Each event type uses minimal configuration that adapts automatically to payload variations:

| File Type | Claims | Exposures | Payments |
|-----------|--------|-----------|----------|
| **Schema Mapping** | None (dynamic) | None (dynamic) | None (dynamic) |
| **Transform Spec** | `GWClaimCenter-Claims.json` | `GWClaimCenter-Exposures.json` | `GWClaimCenter-Payments.json` |
| **Data Quality Rules** | `dq-GWClaimCenter-Claims.json` | `dq-GWClaimCenter-Exposures.json` | `dq-GWClaimCenter-Payments.json` |
| **Consume SQL** | `spark-GWClaimCenter-Claims.sql` | `spark-GWClaimCenter-Exposures.sql` | `spark-GWClaimCenter-Payments.sql` |
| **Athena Views** | Manual ([reference SQL](athena-views-GWClaimCenter-Claims.sql)) | — | — |

**Key Change**: No schema mapping CSV files needed. InsuranceLake automatically processes ALL fields from Guidewire events.

{: .note }
Athena views for flattening nested JSON (activities, exposures, contacts, reserves) are provided as reference SQL in `docs/athena-views-GWClaimCenter-Claims.sql`. These are not created automatically by the pipeline because the nested columns are optional and may not exist in every batch. Create the views manually in the Athena console once your cleanse table has accumulated events containing these fields.

### Schema Mapping Details

With dynamic schema processing, explicit schema mappings are no longer used. InsuranceLake's `clean_column_names()` automatically processes ALL fields. The following patterns show how Guidewire fields are handled:

**Enum Normalization** (handled in Lambda):
```
state: {"code": "open", "name": "Open"} → state: "open"
lobCode: {"code": "PersonalAutoLine"} → lobcode: "PersonalAutoLine"
`faultRating`.`code`,faultrating
```

**Nested Object Flattening**:
```csv
lossLocation,null
`lossLocation`.`addressLine1`,losslocation_address1
`lossLocation`.`city`,losslocation_city
`lossLocation`.`state`.`code`,losslocation_statecode
```

**Collections Preserved as Strings**:
```csv
activities,activities
contacts,contacts
exposures,exposures
```

### Transform Specifications

#### Simplified Transform Specs for Dynamic Schema

**Claims Transform (`GWClaimCenter-Claims.json`)**:
```json
{
  "input_spec": {
    "cleanse_partition_append": true,    // Append-only for full event history
    "allow_schema_change": "permissive", // Handle any payload variation
    "strict_schema_mapping": false       // Enable dynamic schema processing
  },
  "transform_spec": {
    "literal": {
      "sourcesystem": "GWClaimCenter",
      "eventtype": "AppEvents"
    }
  }
}
```

**Key Features of Dynamic Transform Specs**:

| Setting | Value | Purpose |
|---------|-------|---------|
| **Schema Mapping** | None (removed) | InsuranceLake auto-processes ALL fields |
| **Schema Change Mode** | `permissive` | Handles any field variations between events |
| **Field-Specific Transforms** | Removed | Avoids errors when fields are missing |
| **Cleanse Partition Append** | `true` | Preserves full event history |

**Result**: Transform specs work with **any Guidewire event structure** automatically.

#### Automatic Field Processing

Instead of predefined mappings, the integration relies on InsuranceLake's native field processing:

```python
# InsuranceLake automatically:
for field in event_schema:  # ALL fields processed
    clean_name = field.name.lower().replace(' ', '_')  # Auto-clean
    include_in_table(clean_name)  # ALL fields included
```

**Examples**:
- `claimNumber` → `claimnumber`
- `lossLocation.addressLine1` → `losslocation_addressline1`
- `policy.effectiveDate` → `policy_effectivedate`
- `newGuidewireField` → `newguidewirefield`

### Data Quality Rules

| Table | Warn Rules | Halt Rules |
|-------|------------|------------|
| **Claims** | policynumber > 90%, lobcode > 90%, reporteddate > 90%, insured_name > 80% | claimnumber complete, lossdate complete |
| **Exposures** | lobcode > 90%, lossdate > 90% | claimnumber complete |
| **Payments** | policynumber > 90%, paymentstatus > 90% | claimnumber complete, paymentid complete |

### Consume SQL Deduplication

All consume tables use ROW_NUMBER() windowing to deduplicate events:

```sql
-- Claims deduplication example
SELECT ...
FROM (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY claimnumber
        ORDER BY reporteddate DESC
    ) as rn
    FROM gwclaimcenter.claims
)
WHERE rn = 1
ORDER BY lossdate DESC, claimnumber ASC
```

| Table | Dedup Key | Ordering Field | Logic |
|-------|-----------|----------------|-------|
| Claims | `claimnumber` | `reporteddate DESC` | Most recently reported claim event per claim |
| Exposures | `claimid` | `reporteddate DESC` | Most recently reported exposure event per claim |
| Payments | `paymentid` | `createtime DESC` | Most recently created payment event per payment |

## Implementation Details

### Partition Append Mode

**Standard InsuranceLake Behavior:**
```python
# In etl_collect_to_cleanse.py (line 336)
clear_partition(args['target_database_name'], args['table_name'], partition, glueContext)
```
This purges the existing partition before writing new data.

**AppEvents Enhancement:**
```python
# Modified behavior (line 336-340)
if not input_spec.get('cleanse_partition_append', False):
    clear_partition(args['target_database_name'], args['table_name'], partition, glueContext)
else:
    print('Partition append mode enabled: skipping clear_partition to accumulate data')
```

**Result**: When `partition_append: true` is set in the transform spec, new batches append to existing daily partitions instead of overwriting them.

### SQS Event Source Mapping

**Lambda Trigger Configuration:**
```python
# In guidewire_appevents_stack.py
lambda_function.add_event_source(
    lambda_event_sources.SqsEventSource(
        queue,
        batch_size=lambda_config.get('batch_size', 100),        # 1-10,000
        max_batching_window=cdk.Duration.seconds(30),           # 0-300s
        max_concurrency=lambda_config.get('max_concurrency', 10), # 1-1,000
        report_batch_item_failures=True,
    )
)
```

**Throughput Capacity**: Up to 100,000 events/hour (10 Lambda instances × 100 messages/batch × 6 batches/minute)

### Bulk Migration Jobs

**Dual-Mode Bulk Migration for Different Scales:**

#### Standard Bulk Migration Job
**Best for**: 100K - 500K events
```bash
aws glue start-job-run \
  --job-name dev-insurancelake-gw-bulk-migration-job \
  --arguments '{"--source_path":"s3://gw-bucket/"}' \
  --region us-east-1
```
- **Full schema inference** across all files
- **Single output file** per table (coalesce to 1 partition)
- **Performance**: 500K events in 20-30 minutes

#### Optimized Bulk Migration Job
**Best for**: 1M+ events
```bash
aws glue start-job-run \
  --job-name dev-insurancelake-gw-bulk-migration-optimized-job \
  --arguments '{
    "--source_path":"s3://gw-bucket/",
    "--sample_size":"10000",
    "--batch_partitions":"200"
  }' \
  --region us-east-1
```
- **Schema sampling**: Infers schema from sample (5K-10K files) vs all files
- **Batched output**: Creates multiple files per table for parallel downstream processing
- **Configurable performance**: Tune sample size and partition count
- **Performance**: 1M events in 20-40 minutes, 5M+ events in 60-120 minutes

#### Performance Comparison

| Dataset Size | Job Type | Schema Inference | Write Strategy | Duration |
|-------------|----------|------------------|----------------|----------|
| **100K events** | Standard | All files (~10 min) | 1 file/table | ~20 min |
| **500K events** | Standard | All files (~30 min) | 1 file/table | ~45 min |
| **1M events** | Optimized | Sample 5K files (~2 min) | 100 files/table | ~30 min |
| **5M events** | Optimized | Sample 10K files (~3 min) | 200 files/table | ~90 min |

#### Tuning Parameters

| Parameter | Default | Purpose | Range |
|-----------|---------|---------|--------|
| `sample_size` | 5000 | Files used for schema inference | 1000-50000 |
| `batch_partitions` | 100 | Output files per table | 1-1000 |
| `workers_bulk` | 50-100 | Glue workers for processing | 10-250 |

## Stack Configuration

### File-Based Configuration

All Guidewire settings are in `lib/configuration.py` following InsuranceLake patterns:

```python
# Configuration constants
ENABLE_GUIDEWIRE_APPEVENTS = 'enable_guidewire_appevents'
GUIDEWIRE_APPEVENTS_BUCKET = 'guidewire_appevents_bucket'
GUIDEWIRE_LAMBDA_MEMORY = 'guidewire_lambda_memory'
# ... other constants

# Environment-specific values
PROD: {
    ENABLE_GUIDEWIRE_APPEVENTS: True,
    GUIDEWIRE_APPEVENTS_BUCKET: 'prod-gw-bucket',
    GUIDEWIRE_LAMBDA_MEMORY: 1024,
    # ... other settings
}
```

### Stack Conditional Deployment

```python
# In pipeline_deploy_stage.py (lines 113-136)
local_config = get_local_configuration(target_environment)
if local_config.get(ENABLE_GUIDEWIRE_APPEVENTS, False):
    guidewire_appevents_stack = GuidewireAppEventsStack(
        # ... stack instantiation
    )
```

The Guidewire stack only deploys when `ENABLE_GUIDEWIRE_APPEVENTS: True` in the environment configuration.

## Code Structure

### Key Files

| File | Purpose | Key Functions/Classes |
|------|---------|----------------------|
| `lib/guidewire_appevents_stack.py` | CDK stack definition | GuidewireAppEventsStack class, SQS/Lambda/IAM resources |
| `lib/guidewire_appevents_batching/lambda_handler.py` | Event batching logic | classify_event(), lambda_handler() |
| `lib/glue_scripts/etl_guidewire_bulk_migration.py` | Bulk migration job | classify_event_type(), Spark folder read |
| `lib/glue_scripts/etl_collect_to_cleanse.py` | Modified ETL script | Partition append conditional logic |
| `transformation-spec/*.csv` | Schema mappings | Field name mappings with backtick notation |
| `transformation-spec/*.json` | Transform specs | Date parsing, type conversion, partition_append flag |
| `dq-rules/*.json` | Data quality rules | Warn/halt rules per table |
| `transformation-sql/*.sql` | Consume deduplication | ROW_NUMBER() windowing SQL |

### Lambda Handler Logic

**SQS Batch Event Processing:**
```python
def lambda_handler(event: dict, _) -> dict:
    # Receive up to 100 SQS messages from event source mapping
    for record in event.get('Records', []):
        body = json.loads(record['body'])
        # Extract S3 event notification from SQS message body
        for s3_record in body.get('Records', []):
            source_key = unquote_plus(s3_record['s3']['object']['key'])
            table_name = classify_event(source_key)
            # Read JSON, stringify nested fields, accumulate by table

    # Write separate JSONL per table type
    # Return partial failures for retry
    return {'batchItemFailures': [...]}
```

### Glue Job Modifications

**Partition Append Implementation:**
```python
# In etl_collect_to_cleanse.py (lines 335-340)
if not input_spec.get('cleanse_partition_append', False):
    clear_partition(args['target_database_name'], args['table_name'], partition, glueContext)
else:
    print('Partition append mode enabled: skipping clear_partition to accumulate data')
```

This allows incremental event data to accumulate within daily partitions instead of being overwritten.

## Table Schemas

### Dynamic Schema Tables

**All tables use dynamic schema processing** — the exact fields depend on what Guidewire sends in each event type.

#### Claims Table (`gwclaimcenter.claims`)
**Event Sources**: ClaimCreated, ClaimChanged
**Field Processing**: **ALL fields automatically included** with auto-cleaned names

**Common Fields** (when present in events):
```sql
-- Core fields (usually present)
claimnumber, lossdate, reporteddate, description, policynumber

-- Auto-flattened enum fields
claimstate (from state.code), lobcode (from lobCode.code)

-- Auto-flattened nested objects
losslocation_addressline1, losslocation_city, losslocation_state_code
policy_policynumber, policy_policytype_code

-- Stringified collections (JSON format)
activities, contacts, exposures, reserves

-- Any additional fields Guidewire adds in future
customfield, newfeature, enhanceddata (all auto-included)
```

#### Exposures Table (`gwclaimcenter.exposures`)
**Event Sources**: ExposureAdded, ExposureChanged
**Field Processing**: **ALL exposure event fields** automatically included

**Typical Fields**: Exposure-specific data plus claim context when available

#### Payments Table (`gwclaimcenter.payments`)
**Event Sources**: PaymentCreated, PaymentChanged
**Field Processing**: **ALL payment event fields** automatically included

**Typical Fields**: Payment-specific financial data plus claim references when available

### Schema Flexibility Examples

**Scenario 1**: ClaimCreated has 50 fields, ClaimChanged has 55 fields
**Result**: Table automatically accommodates both (missing fields = NULL)

**Scenario 2**: Guidewire adds `aiAnalysisResult` field in future release
**Result**: Field automatically included as `aianalysisresult` (no code changes)

**Scenario 3**: Some claims missing `lossLocation` entirely
**Result**: Related fields (losslocation_*) are NULL for those events

## Surge Handling Implementation

### Auto-Scaling Lambda

**SQS Event Source Mapping Parameters:**
```python
batch_size=100,                    # Process up to 100 events per invocation
max_batching_window=30,            # Wait up to 30s to fill batch
max_concurrency=10,                # Up to 10 concurrent Lambda instances
report_batch_item_failures=True,   # Partial failure support
```

**Theoretical Throughput**: 10 instances × 100 events × 120 batches/hour = ~120K events/hour

### Bulk Migration Job

**Spark Configuration:**
```python
# 50 workers process 1M+ files in parallel
number_of_workers=50,
worker_type='G.1X',
glue_version='5.1',
max_concurrent_runs=1,    # One-time use, not concurrent
```

**Performance**: 1M events in 30-60 minutes using Spark's native file parallelism.

## Troubleshooting

### Common Issues

**Note**: Dynamic schema processing eliminates most field-related errors. The following issues may still occur:

| Issue | Cause | Resolution |
|-------|-------|------------|
| **Lambda logs "Unknown event type"** | Event doesn't match regex pattern | Check S3 key format: `cc:NNNN-EventType-timestamp.json` |
| **Glue job fails with "colon-containing keys"** | Nested struct schema inference | Verify stringify fields list in Lambda (should be rare) |
| **Pipeline not triggering** | SQS event source mapping disabled | Check Lambda event sources in console |
| **Cleanse table only has latest batch** | cleanse_partition_append disabled | Add `"cleanse_partition_append": true` to transform spec |

**Eliminated Issues** (with dynamic schema):
- ~~Schema change errors~~ → **Solved**: Permissive mode handles any field variations
- ~~Missing field errors~~ → **Solved**: Dynamic processing includes all fields automatically
- ~~Manual schema mapping maintenance~~ → **Solved**: Zero configuration for new Guidewire fields

### Debugging via AWS Console

**Check Lambda configuration:**

1. Navigate to the AWS Lambda console
1. Find the `dev-insurancelake-gw-appevents-batching` function
1. Verify event source mapping is configured with correct batch size and concurrency
1. Check function memory and timeout settings match configuration

**Monitor Glue job execution:**

1. Navigate to the AWS Glue console
1. Select Jobs and find `dev-insurancelake-collect-to-cleanse-job`
1. Check recent job runs for status and duration
1. Navigate to CloudWatch Logs for detailed job output

**Verify data accumulation:**

Navigate to the Athena console and run validation queries:

```sql
-- Verify cleanse accumulation (should grow with each batch)
SELECT COUNT(*) as total_events FROM gwclaimcenter.claims;

-- Verify consume deduplication (unique entities only)
SELECT COUNT(*) as unique_claims FROM gwclaimcenter_consume.claims;
```

## Deployment Validation

### Integration Testing

**End-to-End Validation:**

1. Upload a test event (with any field structure) to your Guidewire AppEvents S3 bucket using the AWS Console.

1. Navigate to the CloudWatch console and check the Lambda function logs.

1. Verify Lambda execution triggered within 30 seconds (auto-scaling behavior).

1. Navigate to the Step Functions console to verify pipeline execution.

1. Check that the execution completed successfully.

1. Navigate to the Athena console and run a validation query:

    ```sql
    -- All fields automatically available (dynamic schema)
    SELECT * FROM gwclaimcenter_consume.claims
    WHERE claimnumber LIKE '%test%' OR id LIKE '%test%';
    ```

**Dynamic Schema Validation:**

1. Upload events with **different field structures** (some missing optional fields).

1. Verify ALL events process successfully (no missing field errors).

1. Check that ALL fields from ALL events are preserved in the tables.

1. Confirm new/unexpected fields are automatically included with cleaned names.

### Surge Resilience Testing

**Simulate CAT Event:**

1. Navigate to the S3 console and upload multiple test events simultaneously.

1. Navigate to the CloudWatch console and check Lambda metrics.

1. Verify concurrent Lambda executions occurred (up to 10 instances).

1. Monitor the `ConcurrentExecutions` metric in CloudWatch to confirm auto-scaling behavior.

## Future Enhancements

### Apache Iceberg for Consume Layer

**Current Implementation**: Consume tables use ROW_NUMBER() deduplication SQL to maintain current state.

**Future Enhancement**: Replace consume tables with Apache Iceberg format to enable:
- **UPSERT operations**: `MERGE INTO table USING source ON key WHEN MATCHED THEN UPDATE`
- **Time travel queries**: Query historical snapshots without maintaining full event history
- **Schema evolution**: Add/remove columns without reprocessing data
- **Improved performance**: Eliminate deduplication SQL overhead

**Implementation Approach**:
1. Update consume Glue jobs to write Iceberg format
2. Replace deduplication SQL with MERGE statements
3. Configure Iceberg table properties for optimal performance
4. Maintain backward compatibility during transition

This enhancement would simplify the consume layer logic while maintaining the same current-state semantics.

## Customization and Extensions

### For Business Users

#### Cost Optimization Use Cases

**Development Environment Setup:**
```python
# In lib/configuration.py:
DEV: {
    GUIDEWIRE_LAMBDA_MEMORY: 256,         # Minimum viable memory
    GUIDEWIRE_LAMBDA_BATCH_SIZE: 50,      # Smaller batches for cost
    GUIDEWIRE_GLUE_WORKERS_STANDARD: 10,  # Fewer workers
    GUIDEWIRE_GLUE_WORKERS_BULK: 25,      # Reduced bulk migration capacity
}
```

**Production Cost Control:**
```python
# In lib/configuration.py:
PROD: {
    GUIDEWIRE_LAMBDA_MEMORY: 1024,        # Balanced performance vs cost
    GUIDEWIRE_LAMBDA_BATCH_SIZE: 200,     # Larger batches for efficiency
    GUIDEWIRE_LAMBDA_CONCURRENCY: 15,     # Moderate surge capacity
}
```

#### Multi-Environment Management

**Selective Environment Deployment:**
```python
# Enable only where needed
DEV: { ENABLE_GUIDEWIRE_APPEVENTS: True },   # For testing
TEST: { ENABLE_GUIDEWIRE_APPEVENTS: False }, # Skip test environment
PROD: { ENABLE_GUIDEWIRE_APPEVENTS: True },  # Production deployment
```

### For Operations Teams

#### Surge Preparation Use Cases

**CAT Event Readiness:**
```python
# High-surge configuration for catastrophic events
PROD: {
    GUIDEWIRE_LAMBDA_MEMORY: 2048,        # Maximum memory for speed
    GUIDEWIRE_LAMBDA_CONCURRENCY: 25,     # High concurrency limit
    GUIDEWIRE_LAMBDA_BATCH_SIZE: 500,     # Large batches
    GUIDEWIRE_SQS_VISIBILITY_TIMEOUT: 1800, # 30-min visibility for large batches
}
```

**Performance Monitoring Adjustments:**
- **Lambda memory**: Monitor CloudWatch memory utilization; increase if consistently above 80%
- **SQS visibility timeout**: Must be ≥ Lambda timeout to prevent duplicate processing
- **Glue workers**: Increase for faster processing during peak periods; decrease for cost optimization

#### Data Retention Management

**DLQ and SQS Retention:**
```python
# Extend retention for compliance environments
PROD: {
    GUIDEWIRE_SQS_RETENTION_DAYS: 14,      # Maximum SQS retention
    GUIDEWIRE_DLQ_RETENTION_DAYS: 14,      # Match main queue retention
}
```

### For Developers

#### Extension Use Cases

**Adding New Guidewire Event Types:**

To support additional event types (e.g., `ReserveCreated`, `DocumentAdded`):

1. **Update event classification** in `lambda_handler.py`:
   ```python
   EVENT_TYPE_ROUTING = {
       # existing types...
       'ReserveCreated': 'Reserves',
       'DocumentAdded': 'Documents',
   }
   ```

1. **Configure stringify fields** for new table types:
   ```python
   STRINGIFY_FIELDS = {
       # existing tables...
       'Reserves': ['lineItems', 'exposure', 'coverage'],
       'Documents': ['attachments', 'metadata'],
   }
   ```

1. **Create InsuranceLake configuration files**:
   - `transformation-spec/GWClaimCenter-Reserves.csv`
   - `transformation-spec/GWClaimCenter-Reserves.json`
   - `dq-rules/dq-GWClaimCenter-Reserves.json`
   - `transformation-sql/spark-GWClaimCenter-Reserves.sql`

1. **Deploy updated configuration**: `ENV=prod cdk deploy`

**Adding New Guidewire Products:**

To integrate PolicyCenter or BillingCenter AppEvents:

1. **Create separate Lambda and SQS** for each product (isolation and independent scaling)
1. **Use different source system prefixes**: `GWPolicyCenter/Policies/`, `GWBillingCenter/Invoices/`
1. **Configure product-specific event routing** and stringify field lists
1. **Deploy additional stacks** with product-specific configuration

#### Advanced Customization Use Cases

**Custom Data Processing Logic:**

Extend transform specifications for business-specific enrichment:

```json
{
  "transform_spec": {
    "lookup": [
      {
        "field": "territory_name",
        "source": "losslocation_statecode",
        "lookup": "TerritoryMapping",
        "nomatch": "Unknown Territory"
      }
    ],
    "columnfromcolumn": [
      {
        "field": "claim_age_days",
        "source": "lossdate",
        "pattern": "datediff(current_date(), '{}')"
      }
    ]
  }
}
```

**Performance Optimization for High-Volume:**
- **Glue worker scaling**: Use G.2X worker type for memory-intensive transformations
- **Partition strategy**: Consider hourly partitions for extremely high-volume scenarios
- **Batch size tuning**: Balance Lambda invocation frequency vs processing efficiency
- **Concurrent execution limits**: Adjust Glue max_concurrent_runs based on cluster capacity

---

*For user documentation and deployment instructions, see [Guidewire AppEvents User Guide](guidewire_appevents.md).*
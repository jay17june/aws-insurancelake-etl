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

### Nested JSON Structure Handling

Guidewire AppEvents contain complex nested structures that require special handling to prevent Spark schema inference issues:

**Original Nested Structure Example:**
```json
{
  "id": "cc:9898",
  "claimNumber": "000-00-009898",
  "state": {"code": "open", "name": "Open"},
  "lobCode": {"code": "PersonalAutoLine", "name": "Personal Auto"},
  "activities": {
    "cc:17499": {
      "subject": "Initial claim setup",
      "status": {"code": "open", "name": "Open"},
      "assignedUser": {"displayName": "John Adjuster", "id": "user:123"}
    },
    "cc:5087": {
      "subject": "Investigation complete",
      "status": {"code": "closed", "name": "Closed"}
    }
  },
  "exposures": {
    "cc:94741": {
      "coverageType": "Comprehensive",
      "lossParty": {"code": "first", "name": "First Party"}
    }
  }
}
```

**Flattening Strategy:**

1. **Enum Structs**: Extract `.code` values using schema mapping
   ```csv
   state,null
   `state`.`code`,claimstate
   lobCode,null
   `lobCode`.`code`,lobcode
   ```

2. **Nested Collections**: Stringify in Lambda to preserve as JSON strings
   ```python
   STRINGIFY_FIELDS = {
       'Claims': ['activities', 'contacts', 'exposures', 'reserves'],
       'Exposures': ['contacts', 'exposures', 'vehicleIncidents'],
       'Payments': ['amount', 'transactionAmount', 'lineItems']
   }
   ```

3. **Complex Objects**: Flatten specific fields, null parent
   ```csv
   policy,null
   `policy`.`policyNumber`,policy_policynumber
   `policy`.`policyType`.`code`,policytype
   ```

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

### Per-Table Configuration

Each event type group has a complete set of InsuranceLake configuration files:

| File Type | Claims | Exposures | Payments |
|-----------|--------|-----------|----------|
| **Schema Mapping** | `GWClaimCenter-Claims.csv` | `GWClaimCenter-Exposures.csv` | `GWClaimCenter-Payments.csv` |
| **Transform Spec** | `GWClaimCenter-Claims.json` | `GWClaimCenter-Exposures.json` | `GWClaimCenter-Payments.json` |
| **Data Quality Rules** | `dq-GWClaimCenter-Claims.json` | `dq-GWClaimCenter-Exposures.json` | `dq-GWClaimCenter-Payments.json` |
| **Consume SQL** | `spark-GWClaimCenter-Claims.sql` | `spark-GWClaimCenter-Exposures.sql` | `spark-GWClaimCenter-Payments.sql` |
| **Athena Views** | `athena-GWClaimCenter-Claims.sql` | — | — |

### Schema Mapping Details

Schema mappings follow consistent Guidewire-specific patterns:

**Enum Struct Extraction**:
```csv
SourceName,DestName
flagged,null
`flagged`.`code`,flagged
faultRating,null
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

#### Claims Transform (`GWClaimCenter-Claims.json`)
```json
{
  "input_spec": {
    "cleanse_partition_append": true,      // Key: enables append-only cleanse
    "allow_schema_change": "evolve"
  },
  "transform_spec": {
    "date": [
      {"field": "lossdate", "format": "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"},
      {"field": "reporteddate", "format": "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"}
    ],
    "combinecolumns": [{
      "field": "losslocation_full",
      "format": "{}, {}, {} {}",
      "source_columns": ["losslocation_address1", "losslocation_city",
                         "losslocation_statecode", "losslocation_postalcode"]
    }],
    "literal": {
      "sourcesystem": "GWClaimCenter",
      "eventtype": "AppEvents"
    }
  }
}
```

#### Key Differences by Table

| Setting | Claims | Exposures | Payments |
|---------|--------|-----------|----------|
| **Schema Change Mode** | `evolve` | `evolve` | `permissive` |
| **Date Fields** | lossdate, reporteddate | lossdate, reporteddate | createtime, issuedate |
| **Date Format** | `yyyy-MM-dd'T'HH:mm:ss.SSS'Z'` | `yyyy-MM-dd'T'HH:mm:ss.SSS'Z'` | `yyyy-MM-dd'T'HH:mm:ss.SSS'Z'` |

{: .note }
Payments uses `permissive` schema change mode because PaymentCreated and PaymentChanged events have different fields (e.g., `updateTime` only appears in PaymentChanged).

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

### Bulk Migration Job

**Glue Job Configuration:**
```python
# In glue_jobs_stack.py (lines 284-309)
self.bulk_migration_job = glue.CfnJob(
    name=f'{target_environment.lower()}-{self.resource_name_prefix}-gw-bulk-migration-job',
    script_location='s3://{bucket}/etl/etl_guidewire_bulk_migration.py',
    number_of_workers=glue_config.get('workers_bulk', 50),    # Configurable
    worker_type='G.1X',
    glue_version='5.1',
    max_concurrent_runs=1,    # One-time use only
)
```

**Script Logic** (`etl_guidewire_bulk_migration.py`):
1. Reads ALL JSON files using `spark.read.json('s3://bucket/**/*.json')`
2. Classifies events by file path using same regex pattern
3. Stringifies nested collections per table type
4. Writes consolidated JSONL per event group
5. Triggers standard InsuranceLake pipeline

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

### Claims Table Structure

**Guidewire Source Fields** → **InsuranceLake Columns**:

| Category | Original Structure | Flattened Fields |
|----------|-------------------|------------------|
| **Identity** | `id`, `claimNumber` | claimid, claimnumber |
| **Enums** | `state: {code, name}` | claimstate (code only) |
| **Nested Objects** | `policy: {policyNumber, policyType: {code, name}}` | policy_policynumber, policytype |
| **Location** | `lossLocation: {addressLine1, city, state: {code}, postalCode}` | losslocation_address1, losslocation_city, losslocation_statecode, losslocation_postalcode |
| **Dynamic Collections** | `activities: {"cc:17499": {...}, "cc:5087": {...}}` | activities (JSON string) |

### Exposures Table Structure

**Key Differences from Claims:**
- Has `coverageInQuestion` boolean field
- Uses `vehicleIncidents` (camelCase) instead of `vehicle-incidents`
- Contains exposure-specific fields like `allValidationLevelsReached`
- Shares claim context (lossdate, policy info) but adds exposure details

### Payments Table Structure

**Financial Focus:**
- No loss details (lossDate, lossLocation, insured)
- Payment-specific: `amount`, `checkNumber`, `payee`, `costType`, `costCategory`
- Timestamps: `createTime`, `issueDate` (not lossDate/reportedDate)
- References: `exposure_id`, `reserve_id` for linking back to claims

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

| Issue | Cause | Resolution |
|-------|-------|------------|
| **Lambda logs "Unknown event type"** | Event doesn't match regex pattern | Check S3 key format: `cc:NNNN-EventType-timestamp.json` |
| **Glue job fails with "colon-containing keys"** | Nested struct schema inference | Verify stringify fields list in Lambda |
| **Schema change error on Payments** | PaymentCreated/Changed have different fields | Ensure `"allow_schema_change": "permissive"` |
| **Cleanse table only has latest batch** | cleanse_partition_append disabled | Add `"cleanse_partition_append": true` to transform spec |
| **Consume has duplicate rows** | Dedup key incorrect | Check PARTITION BY clause in consume SQL |
| **Pipeline not triggering** | SQS event source mapping disabled | Check Lambda event sources in console |

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

1. Upload a test event to your Guidewire AppEvents S3 bucket using the AWS Console.

1. Navigate to the CloudWatch console and check the Lambda function logs.

1. Verify Lambda execution triggered within 30 seconds (auto-scaling behavior).

1. Navigate to the Step Functions console to verify pipeline execution.

1. Check that the execution completed successfully.

1. Navigate to the Athena console and run a verification query:

    ```sql
    SELECT * FROM gwclaimcenter_consume.claims WHERE claimnumber = '000-00-099999';
    ```

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
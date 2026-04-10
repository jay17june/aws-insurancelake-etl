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
**Cleanse Layer**: Append-only Parquet tables with full event history (enabled by `partition_append: true`)
**Consume Layer**: Current-state tables with ROW_NUMBER() deduplication

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

Guidewire uses dynamic IDs as map keys (e.g., `"cc:17499"`). When Spark reads this JSON, it infers struct schemas with colon-containing field names that are incompatible with Hive metastore operations. By stringifying these fields in the Lambda, Spark reads them as plain strings, and Athena views parse them on demand.

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
        ORDER BY execution_id DESC
    ) as rn
    FROM gwclaimcenter.claims
)
WHERE rn = 1
ORDER BY lossdate DESC, claimnumber ASC
```

| Table | Dedup Key | Logic |
|-------|-----------|-------|
| Claims | `claimnumber` | Latest ClaimCreated or ClaimChanged per claim |
| Exposures | `claimid` | Latest ExposureAdded or ExposureChanged per claim |
| Payments | `paymentid` | Latest PaymentCreated or PaymentChanged per payment |

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

### Debug Commands

**Check Lambda configuration:**
```bash
aws lambda get-function --function-name dev-insurancelake-gw-appevents-batching
aws lambda list-event-source-mappings --function-name dev-insurancelake-gw-appevents-batching
```

**Monitor Glue job execution:**
```bash
aws glue get-job-runs --job-name dev-insurancelake-collect-to-cleanse-job
aws logs tail /aws-glue/jobs/output --since 1h
```

**Check data accumulation:**
```sql
-- Verify cleanse accumulation (should grow)
SELECT COUNT(*) as total_events FROM gwclaimcenter.claims;

-- Verify consume deduplication (unique entities)
SELECT COUNT(*) as unique_claims FROM gwclaimcenter_consume.claims;
```

## Testing

### Integration Tests

**End-to-End Test:**
```bash
# 1. Upload test event
aws s3 cp test-claim.json s3://gw-bucket/cc:99999/cc:99999-ClaimCreated-20260409T120000Z-001.json

# 2. Monitor Lambda execution (should trigger within 30s)
aws logs tail /aws/lambda/dev-insurancelake-gw-appevents-batching --since 2m

# 3. Verify pipeline execution
aws stepfunctions list-executions --state-machine-arn <arn> --max-results 3

# 4. Query results
aws athena start-query-execution --query-string "SELECT * FROM gwclaimcenter_consume.claims WHERE claimnumber = '000-00-099999'"
```

### Surge Testing

**Simulate CAT Event:**
```bash
# Upload 100 events in parallel
for i in $(seq 1 100); do
  aws s3 cp test-event.json "s3://gw-bucket/cc:$i/cc:$i-ClaimCreated-20260409T120000Z-$i.json" &
done
wait

# Monitor Lambda concurrency
aws cloudwatch get-metric-statistics \
  --namespace AWS/Lambda --metric-name ConcurrentExecutions \
  --dimensions Name=FunctionName,Value=dev-insurancelake-gw-appevents-batching \
  --start-time $(date -u -v-10M '+%Y-%m-%dT%H:%M:%SZ') \
  --end-time $(date -u '+%Y-%m-%dT%H:%M:%SZ') \
  --period 60 --statistics Maximum
```

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

## Extending the Integration

### Adding New Event Types

To support additional Guidewire event types (e.g., `ReserveCreated`):

1. **Update event routing** in `lambda_handler.py`:
   ```python
   EVENT_TYPE_ROUTING = {
       # existing types...
       'ReserveCreated': 'Reserves',
   }
   ```

2. **Add stringify fields**:
   ```python
   STRINGIFY_FIELDS = {
       # existing tables...
       'Reserves': ['lineItems', 'exposure', 'coverage'],
   }
   ```

3. **Create InsuranceLake configuration files**:
   - `transformation-spec/GWClaimCenter-Reserves.csv`
   - `transformation-spec/GWClaimCenter-Reserves.json`
   - `dq-rules/dq-GWClaimCenter-Reserves.json`
   - `transformation-sql/spark-GWClaimCenter-Reserves.sql`

4. **Deploy**: `ENV=prod cdk deploy`

### Performance Tuning

**Lambda Optimization:**
- Increase memory for faster S3 reads during high-volume periods
- Adjust batch_size vs concurrency based on event size and frequency
- Monitor memory utilization and adjust accordingly

**Glue Job Optimization:**
- Standard workers: Balance cost vs processing speed for regular batches
- Bulk workers: Optimize for one-time migration scenarios
- Consider G.2X worker type for memory-intensive transformations

**SQS Tuning:**
- Visibility timeout must be ≥ Lambda timeout to prevent duplicate processing
- Adjust retention periods based on acceptable data loss risk
- Monitor DLQ depth for systematic failures

---

*For user documentation and deployment instructions, see [Guidewire AppEvents User Guide](guidewire_appevents.md).*
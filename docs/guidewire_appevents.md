---
title: Guidewire AppEvents Integration
parent: User Documentation
nav_order: 9
last_modified_date: 2026-04-09
---
# Guidewire ClaimCenter AppEvents Integration
{: .no_toc }

This section provides instructions for deploying and using the Guidewire ClaimCenter AppEvents integration with AWS InsuranceLake.

For developers looking to understand or extend the implementation, refer to the [Guidewire AppEvents Developer Guide](guidewire_appevents_developer_guide.md).

## Contents
{: .no_toc }

* TOC
{:toc}

## Overview

This integration connects Guidewire ClaimCenter AppEvents to AWS InsuranceLake, enabling automated ingestion, transformation, and analytics of claim event data. The solution handles six AppEvent types (ClaimCreated, ClaimChanged, ExposureAdded, ExposureChanged, PaymentCreated, PaymentChanged) and provides analytics-ready data tables for business intelligence, regulatory reporting, and operational insights.

**This integration helps you to:**
- **Ingest Guidewire data automatically** — No manual ETL configuration or data movement required
- **Handle surge scenarios** — Process catastrophic events (10K+ claims/hour) and bulk migrations (1M+ events) without data loss
- **Maintain regulatory compliance** — Full audit trail preserved with append-only event history
- **Accelerate analytics** — Pre-built current-state tables and nested data views ready for Athena, QuickSight, and Redshift
- **Reduce operational overhead** — Event-driven pipeline with automated retries, monitoring, and notifications

## Architecture

The integration processes Guidewire AppEvents through an auto-scaling pipeline:

![Overall Architecture](guidewire-appevents-overall-architecture.drawio)

**Data Flow**:
1. **Guidewire ClaimCenter** writes JSON events to S3 bucket
2. **S3 Event Notifications** send messages to SQS queue
3. **Auto-scaling Lambda** (up to 10 concurrent instances) processes events
4. **Event Classification** routes to separate tables (Claims, Exposures, Payments)
5. **InsuranceLake Pipeline** transforms and loads data through Glue jobs
6. **Analytics Layer** provides SQL access via Athena

### AWS Resources Created

| Resource | Purpose |
|----------|---------|
| **Amazon S3** | Event landing bucket, data lake storage (collect/cleanse/consume) |
| **Amazon SQS** | Event buffering with dead letter queue for resilience |
| **AWS Lambda** | Auto-scaling event batching and classification (512MB-2GB, 15min timeout) |
| **AWS Glue** | ETL processing with configurable workers (25-100 workers per job) |
| **AWS Step Functions** | Pipeline orchestration with retry logic |
| **Amazon DynamoDB** | Job audit trails and data lineage tracking |
| **Amazon Athena** | SQL analytics with pre-built views for nested data |
| **AWS KMS** | Data encryption across all storage layers |

## Getting Started

### Prerequisites

- AWS account with administrator access
- Guidewire ClaimCenter configured to send AppEvents to S3
- AWS CLI and CDK v2 installed

### Deployment Steps

**Step 1: Deploy AWS InsuranceLake Infrastructure**
```bash
git clone https://github.com/aws-solutions-library-samples/aws-insurancelake-infrastructure
cd aws-insurancelake-infrastructure
# Configure region and account in lib/configuration.py
cdk deploy --all
```

**Step 2: Configure the Integration**

Edit `lib/configuration.py` to customize the integration for your environment:

```python
# In lib/configuration.py, update the environment settings:
PROD: {
    ACCOUNT_ID: active_account_id,
    REGION: 'us-east-1',  # Set your AWS region
    LINEAGE: True,
    CODE_BRANCH: 'main',
    # Guidewire AppEvents Integration Settings
    ENABLE_GUIDEWIRE_APPEVENTS: True,  # Set to False to disable
    GUIDEWIRE_APPEVENTS_BUCKET: 'your-gw-appevents-bucket',  # Your bucket name
    GUIDEWIRE_LAMBDA_MEMORY: 1024,     # Lambda memory (128-10240 MB)
    GUIDEWIRE_LAMBDA_TIMEOUT: 15,      # Lambda timeout (1-15 minutes)
    GUIDEWIRE_LAMBDA_BATCH_SIZE: 200,  # SQS messages per invocation
    GUIDEWIRE_LAMBDA_CONCURRENCY: 20,  # Max parallel Lambdas
    GUIDEWIRE_SQS_VISIBILITY_TIMEOUT: 1200,  # Must be >= Lambda timeout * 60
    GUIDEWIRE_GLUE_WORKERS_STANDARD: 50,     # Regular ETL workers
    GUIDEWIRE_GLUE_WORKERS_BULK: 100,        # Bulk migration workers
},
```

**Step 3: Deploy the Integration**
```bash
git clone https://github.com/jay17june/aws-insurancelake-etl
cd aws-insurancelake-etl
git checkout feature/guidewire-appevents-integration

# Deploy to your target environment
ENV=prod cdk deploy
```

**Step 4: Verify Data Flow**
```bash
# Check that events are flowing
aws athena start-query-execution \
  --query-string "SELECT COUNT(*) FROM gwclaimcenter_consume.claims" \
  --work-group insurancelake
```

## Configuration Options

### Performance Tuning Scenarios

**Production Environment (High Performance)**:
```python
GUIDEWIRE_LAMBDA_MEMORY: 2048,        # More memory for faster processing
GUIDEWIRE_LAMBDA_CONCURRENCY: 25,     # Higher concurrency for surges
GUIDEWIRE_GLUE_WORKERS_STANDARD: 50,  # More workers for faster ETL
GUIDEWIRE_GLUE_WORKERS_BULK: 100,     # Optimized for bulk migrations
```

**Development Environment (Cost Optimized)**:
```python
GUIDEWIRE_LAMBDA_MEMORY: 256,         # Lower memory for cost savings
GUIDEWIRE_LAMBDA_BATCH_SIZE: 50,      # Smaller batches
GUIDEWIRE_GLUE_WORKERS_STANDARD: 10,  # Fewer workers
```

**Disable Integration**:
```python
ENABLE_GUIDEWIRE_APPEVENTS: False,    # Skip Guidewire stack deployment
```

## Data Tables

The integration creates analytics-ready tables for three event types:

| Table | Event Types | Description |
|-------|------------|-------------|
| `gwclaimcenter_consume.claims` | ClaimCreated, ClaimChanged | Current state of claims with loss details |
| `gwclaimcenter_consume.exposures` | ExposureAdded, ExposureChanged | Coverage and incident details by claim |
| `gwclaimcenter_consume.payments` | PaymentCreated, PaymentChanged | Financial transactions and payment status |

Each table contains deduplicated, current-state data optimized for business queries.

## Query Examples

### Claims Analysis
```sql
-- Current state of all open claims
SELECT claimnumber, claimstate, lobcode, lossdate,
       insured_name, losslocation_city, losslocation_statecode,
       datediff(CURRENT_DATE, lossdate) as days_open
FROM gwclaimcenter_consume.claims
WHERE claimstate = 'open'
ORDER BY lossdate DESC;
```

### Financial Reporting
```sql
-- Payment summary by claim
SELECT c.claimnumber, c.claimstate, c.lobcode,
       COUNT(p.paymentid) as payment_count,
       SUM(CASE WHEN p.paymentstatus = 'cleared' THEN 1 ELSE 0 END) as cleared_payments
FROM gwclaimcenter_consume.claims c
LEFT JOIN gwclaimcenter_consume.payments p ON c.claimnumber = p.claimnumber
GROUP BY c.claimnumber, c.claimstate, c.lobcode
ORDER BY payment_count DESC;
```

### Operational Metrics
```sql
-- Claims by state and line of business
SELECT losslocation_statecode as state,
       lobcode,
       COUNT(*) as claim_count,
       AVG(datediff(reporteddate, lossdate)) as avg_days_to_report
FROM gwclaimcenter_consume.claims
WHERE year = '2026' AND month = '04'
GROUP BY losslocation_statecode, lobcode
ORDER BY claim_count DESC;
```

## Surge Scenarios

### Catastrophic Events
- **Capacity**: 100,000+ events per hour
- **Resilience**: Auto-scaling Lambda with SQS buffering prevents data loss
- **Recovery**: Failed events automatically retry with dead letter queue isolation

### Data Migrations
For one-time bulk loads of 1M+ historical events, use the dedicated bulk migration Glue job:

```bash
aws glue start-job-run \
  --job-name dev-insurancelake-gw-bulk-migration-job \
  --arguments '{"--source_path":"s3://your-gw-bucket/"}' \
  --region us-east-1
```

**Performance**: 1M events processed in 30-60 minutes using Spark's native parallelism.

## Cost

### Quick Summary

For processing typical Guidewire ClaimCenter AppEvents volume (1,000 events per day) in production, **your monthly AWS cost will be approximately $285**.

### Cost Breakdown

| AWS Service | Purpose | Estimated Monthly Cost |
|-------------|---------|----------------------|
| **AWS Glue** | ETL processing (Collect-Cleanse-Consume) | $275 |
| **AWS Lambda** | Event batching and routing | $0.01 |
| **Amazon S3** | Data lake storage (all layers) | $0.20 |
| **AWS Step Functions** | Pipeline orchestration | $2.14 |
| **Amazon DynamoDB** | Job audit and data lineage | $1.50 |
| **Other Services** | SQS, Athena, KMS, CloudWatch | $6-10 |
| | **Total** | **~$285** |

### Cost Optimization

**Adjust performance settings** to balance cost with requirements:
- Higher Lambda memory/concurrency: Better surge handling, higher cost
- More Glue workers: Faster processing, higher cost
- Smaller batch sizes: More frequent processing, higher cost

**Use AWS Cost Explorer** to create budgets and track actual expenses.

## Next Steps

### Extend to Other Guidewire Products
- PolicyCenter AppEvents for policy analytics
- BillingCenter AppEvents for financial reporting
- Multiple Guidewire instances with separate data domains

### Advanced Analytics
- QuickSight dashboards for executive reporting
- Machine learning models for claims prediction
- Real-time alerting for exceptional claims patterns

---

*This integration builds on AWS InsuranceLake, an AWS Solutions Library framework for insurance data analytics. Learn more at [aws-solutions-library-samples/aws-insurancelake-etl](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl).*
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

{: .important}
The instructions in the following sections assume you have completed the [Quickstart guide](quickstart.md) and have InsuranceLake infrastructure and ETL pipelines deployed.

Additional prerequisites:
- Guidewire ClaimCenter configured to send AppEvents to S3
- S3 bucket for Guidewire AppEvents already created

### Deployment Steps

1. Configure the integration by editing `lib/configuration.py` to customize for your environment:

    ```python
    # Update the target environment settings:
    PROD: {
        ACCOUNT_ID: active_account_id,
        REGION: 'us-east-1',  # Set your AWS region
        LINEAGE: True,
        CODE_BRANCH: 'main',
        # Guidewire AppEvents Integration Settings
        ENABLE_GUIDEWIRE_APPEVENTS: True,  # Set to False to disable
        GUIDEWIRE_APPEVENTS_BUCKET: 'your-gw-appevents-bucket',
        GUIDEWIRE_LAMBDA_MEMORY: 1024,
        GUIDEWIRE_LAMBDA_TIMEOUT: 15,
        GUIDEWIRE_LAMBDA_BATCH_SIZE: 200,
        GUIDEWIRE_LAMBDA_CONCURRENCY: 20,
        GUIDEWIRE_SQS_VISIBILITY_TIMEOUT: 1200,
        GUIDEWIRE_GLUE_WORKERS_STANDARD: 50,
        GUIDEWIRE_GLUE_WORKERS_BULK: 100,
    },
    ```

1. Deploy the Guidewire AppEvents integration stack:

    ```bash
    cdk deploy Prod-InsuranceLakeEtlPipeline/Prod/InsuranceLakeEtlGuidewireAppEvents
    ```

1. Review and accept IAM credential creation for the Guidewire AppEvents stack.

1. Wait for deployment to finish (approximately 3 minutes).

1. Verify the pipeline deployed successfully by navigating to the AWS CloudFormation console.

1. Verify data flow by opening the Amazon Athena console.

1. In Athena, run the following query to check for claims data:

    ```sql
    SELECT COUNT(*) FROM gwclaimcenter_consume.claims;
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

1. Navigate to the AWS Glue console
1. Select Jobs and find `dev-insurancelake-gw-bulk-migration-job`
1. Click "Run job" and configure the job parameters:
   - `--source_path`: `s3://your-gw-bucket/`
   - `--target_bucket`: (auto-configured)
   - `--source_system`: `GWClaimCenter`
1. Monitor job progress in the console

**Performance**: 1M events processed in 30-60 minutes using Spark's native parallelism.

## Cost

### Cost Estimate

For processing typical Guidewire ClaimCenter AppEvents volume in production, **your monthly AWS cost will be approximately $285 USD**. This estimate assumes:

- 1,000 mixed AppEvents per day (Claims, Exposures, Payments)
- Standard AWS Glue auto-scaling configuration (25-50 workers)
- Regular Athena queries for reporting and analytics (5 queries/day, 1GB scanned each)
- US East (Ohio) Region pricing as of April 2026

Costs scale primarily with event volume. Higher volumes increase AWS Glue DPU-hours but benefit from batch processing efficiency.

### Cost Table

| AWS service | Dimensions | Cost [USD] |
|-------------|-----------|------------|
| AWS Glue | $0.44 per DPU-Hour | $275.00 |
| AWS Lambda | $0.20 per 1M requests | $0.01 |
| Amazon S3 | $0.023 per GB-month | $0.20 |
| AWS Step Functions | $0.025 per 1K state transitions | $2.14 |
| Amazon DynamoDB | On-demand requests | $1.50 |
| Amazon SQS | $0.40 per 1M requests | $0.00 |
| Amazon Athena | $5.00 per TB scanned | $1.50 |
| AWS KMS | $1.00 per key per month | $1.00 |
| Amazon CloudWatch | $0.50 per GB ingested | $1.00 |
| **Total** | | **$282.35** |

*Pricing as of April 9, 2026, US East (Ohio) Region. For current pricing, refer to [AWS Pricing](https://aws.amazon.com/pricing/).*

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
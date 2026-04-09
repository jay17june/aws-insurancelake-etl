# Guidewire ClaimCenter AppEvents Integration for AWS InsuranceLake

*Seamlessly integrate Guidewire ClaimCenter AppEvents with AWS InsuranceLake for automated claims data analytics*

## Overview

This solution enables insurance organizations to leverage their existing Guidewire ClaimCenter AppEvents data within AWS InsuranceLake's proven analytics framework. Built on AWS's "4 Cs" architecture (Collect, Cleanse, Consume, Comply), this integration automatically ingests claim events and provides analytics-ready data tables for business intelligence, regulatory reporting, and operational insights.

**This integration helps you to:**
- **Ingest Guidewire data automatically** — No manual ETL configuration or data movement required
- **Handle surge scenarios** — Process catastrophic events (10K+ claims/hour) and bulk migrations (1M+ events) without data loss
- **Maintain regulatory compliance** — Full audit trail preserved with append-only event history
- **Accelerate analytics** — Pre-built current-state tables and nested data views ready for Athena, QuickSight, and Redshift
- **Reduce operational overhead** — Event-driven pipeline with automated retries, monitoring, and notifications

## Architecture

### Real-Time Processing Pipeline

```
Guidewire ClaimCenter → S3 Events → SQS Queue → Auto-Scaling Lambda → InsuranceLake
```

The integration processes six AppEvent types from Guidewire ClaimCenter:
- **Claim Events**: ClaimCreated, ClaimChanged (loss details, policy info, contacts)
- **Exposure Events**: ExposureAdded, ExposureChanged (coverage details, incidents)
- **Payment Events**: PaymentCreated, PaymentChanged (financial transactions, reserves)

Events are automatically classified and routed to separate analytics tables optimized for each data pattern.

### Data Layer Architecture

Following AWS InsuranceLake's **4 Cs framework**:

**Collect** — Raw JSON events from Guidewire, consolidated into efficient batch files

**Cleanse** — Standardized, validated data with full event history preserved
- `gwclaimcenter.claims` — All claim events with loss details and policy context
- `gwclaimcenter.exposures` — Coverage and incident details by claim
- `gwclaimcenter.payments` — Financial transactions and payment history

**Consume** — Analytics-ready current-state tables optimized for business queries
- `gwclaimcenter_consume.claims` — One row per claim with latest status
- `gwclaimcenter_consume.exposures` — Current exposure details by claim
- `gwclaimcenter_consume.payments` — Latest payment status by transaction

**Comply** — Built-in data quality checks, audit trails, and lineage tracking for regulatory requirements

## Key Benefits

### Business Value

- **Near real-time insights**: Claims data available within minutes of creation in Guidewire
- **360-degree claim view**: Unified analytics across claim details, exposures, and payments
- **Regulatory readiness**: Complete audit trail with data lineage for compliance reporting
- **Cost optimization**: Batch processing reduces compute costs by ~90% vs. individual event processing

### Technical Advantages

- **Surge resilient**: Auto-scales to handle 100,000+ events per hour during catastrophic events
- **Schema adaptive**: Automatically handles Guidewire data structure changes
- **Query performance**: Pre-built current-state tables eliminate complex deduplication in analytics queries
- **Flexible analytics**: Raw event history preserved for actuarial analysis, current-state tables for operational reporting

## Getting Started

### Prerequisites

- AWS account with administrator access
- Guidewire ClaimCenter configured to send AppEvents to S3
- AWS CLI and CDK v2 installed

### Quick Start

**Step 1: Deploy AWS InsuranceLake Infrastructure**
```bash
git clone https://github.com/aws-solutions-library-samples/aws-insurancelake-infrastructure
cd aws-insurancelake-infrastructure
# Configure region and account in lib/configuration.py
cdk deploy --all
```

**Step 2: Deploy the Guidewire Integration**
```bash
git clone https://github.com/jay17june/aws-insurancelake-etl
cd aws-insurancelake-etl
git checkout feature/guidewire-appevents-integration

# Deploy with your configuration (no code changes needed)
cdk deploy --app "python3 app.py" \
  --context env=prod \
  --context guidewire-bucket=your-gw-appevents-bucket \
  --context region=us-east-1
```

**Step 3: (Optional) Customize Configuration**

Fine-tune the integration for your specific requirements:

```bash
# Available configuration parameters:
cdk deploy --app "python3 app.py" \
  --context env=prod \                          # Environment: Dev, Test, or Prod
  --context guidewire-bucket=your-bucket \      # Your Guidewire AppEvents S3 bucket
  --context region=us-east-1 \                  # AWS region
  --context lambda-memory=1024 \                # Lambda memory: 128-10240 MB
  --context lambda-timeout=10 \                 # Lambda timeout: 1-15 minutes
  --context lambda-batch-size=200 \             # SQS messages per Lambda: 1-10000
  --context lambda-concurrency=20 \             # Max parallel Lambdas: 1-1000
  --context glue-workers-standard=40 \          # Standard Glue job workers: 2-250
  --context glue-workers-bulk=100               # Bulk migration workers: 2-250
```

| Parameter | Default | Purpose |
|-----------|---------|---------|
| `env` | Dev | Target environment (Dev, Test, Prod) |
| `guidewire-bucket` | auto-generated | S3 bucket where Guidewire writes AppEvents |
| `region` | us-east-2 | AWS region for all resources |
| `lambda-memory` | 512 | Lambda memory in MB (higher = faster processing) |
| `lambda-batch-size` | 100 | SQS messages per Lambda invocation |
| `lambda-concurrency` | 10 | Max parallel Lambda instances during surges |
| `glue-workers-standard` | 25 | Workers for regular ETL processing |
| `glue-workers-bulk` | 50 | Workers for 1M+ event bulk migrations |

**Step 4: Verify Data Flow**
```bash
# Check that events are flowing
aws athena start-query-execution \
  --query-string "SELECT COUNT(*) FROM gwclaimcenter_consume.claims" \
  --work-group insurancelake
```

## Use Cases and Query Examples

### Claims Management
```sql
-- Current state of all open claims with location and policy details
SELECT claimnumber, claimstate, lobcode, lossdate,
       insured_name, losslocation_city, losslocation_statecode,
       datediff(CURRENT_DATE, lossdate) as days_open
FROM gwclaimcenter_consume.claims
WHERE claimstate = 'open'
ORDER BY lossdate DESC;
```

### Financial Analysis
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

### Operational Reporting
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

### Audit and Compliance
```sql
-- Full event history for a claim (regulatory audit)
SELECT claimnumber, claimstate, eventtype, execution_id,
       CONCAT(year, '-', month, '-', day) as event_date
FROM gwclaimcenter.claims
WHERE claimnumber = '000-00-009898'
ORDER BY execution_id;
```

## Surge Scenarios

The integration is designed to handle common insurance industry surge patterns:

### Catastrophic Events
- **Capacity**: 100,000+ events per hour
- **Resilience**: Auto-scaling Lambda with SQS buffering prevents data loss
- **Recovery**: Failed events automatically retry with dead letter queue isolation

### Policy Renewals
- **Handling**: Burst processing with 30-second batching windows
- **Efficiency**: Multiple events consolidated into optimized batch files

### Data Migrations
- **One-time bulk loads**: Dedicated Glue job for 1M+ historical event migration
- **Performance**: Spark's native parallelism processes large datasets efficiently
- **Duration**: 1M events migrated in 30-60 minutes

## Cost

### Quick Summary

For processing typical Guidewire ClaimCenter AppEvents volume (1,000 events per day) in production, **your monthly AWS cost will be approximately $285**.

This estimate assumes:
- 1,000 mixed AppEvents per day (Claims, Exposures, Payments)
- Standard AWS Glue auto-scaling configuration
- Regular Athena queries for reporting and analytics
- US East 1 (N. Virginia) region pricing as of January 2025

*Note: Prices are subject to change. Please refer to the [AWS Pricing Calculator](https://calculator.aws/) for the most current information.*

### Cost Breakdown

| AWS Service | Purpose | Unit Cost | Estimated Monthly Cost |
|-------------|---------|-----------|----------------------|
| **AWS Glue** | ETL processing (Collect-Cleanse-Consume) | $0.44 per DPU-Hour | $275 |
| **AWS Lambda** | Event batching and routing | $0.20 per 1M requests | $0.01 |
| **Amazon S3** | Data lake storage (all layers) | $0.023 per GB | $0.20 |
| **AWS Step Functions** | Pipeline orchestration | $0.025 per 1K state transitions | $2.14 |
| **Amazon DynamoDB** | Job audit and data lineage | On-demand pricing | $1.50 |
| **Amazon SQS** | Event buffering | $0.40 per 1M requests | $0 (Free Tier) |
| **Amazon Athena** | Ad-hoc analytics queries | $5 per TB scanned | $1-5 |
| **AWS KMS** | Data encryption | $1 per key per month | $1.00 |
| **Amazon EventBridge** | Scheduling (if used) | $1 per 1M events | $0 (Free Tier) |
| **Amazon CloudWatch** | Monitoring and logs | $0.50 per GB ingested | $1.00 |
| | | **Total** | **~$285** |

*Costs scale primarily with event volume. Higher volumes use more Glue DPU hours but benefit from economies of scale in batch processing.*

### Cost Optimization

**Adjust batch processing frequency** to balance cost with data freshness:
- Every 15 minutes (current): $285/month, ~15-minute data latency
- Every hour: $70/month, ~1-hour data latency
- Daily batches: $9/month, ~24-hour data latency

**Use AWS Free Tier** for development and testing environments. Many services (SQS, EventBridge, Lambda) include generous free tier allowances.

**Monitor with AWS Cost Explorer**

We recommend creating a Budget with Cost Explorer to track expenses. Estimated costs are shown as guidelines, and your actual costs will vary based on your usage patterns and AWS pricing changes.

1. Navigate to **AWS Cost Management** in your AWS Console
2. Select **Budgets** and create a new budget
3. Set threshold alerts at 80% and 100% of expected monthly spend

## Next Steps

### Extend to Other Guidewire Products
- PolicyCenter AppEvents for policy analytics
- BillingCenter AppEvents for financial reporting
- Multiple Guidewire instances with separate data domains

### Advanced Analytics
- QuickSight dashboards for executive reporting
- Machine learning models for claims prediction
- Real-time alerting for exceptional claims patterns

### Integration Options
- Redshift Spectrum for data warehouse integration
- DataZone for data cataloging and governance
- EventBridge integration for downstream workflow automation

## Support and Documentation

For detailed technical implementation, see [Internal Integration Guide](guidewire_appevents_integration_internal.md).

---

*This integration builds on AWS InsuranceLake, an AWS Solutions Library framework for insurance data analytics. Learn more at [aws-solutions-library-samples/aws-insurancelake-etl](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl).*
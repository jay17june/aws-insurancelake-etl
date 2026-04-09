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
# Configure Guidewire bucket name in lib/configuration.py
cdk deploy --all --app "python3 deploy_direct.py"
```

**Step 3: Verify Data Flow**
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
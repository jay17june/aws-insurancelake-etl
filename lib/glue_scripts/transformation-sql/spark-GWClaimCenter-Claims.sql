-- Current-state claims: deduplicate by claimnumber, keeping the most recent event
SELECT
    claimid
  , claimnumber
  , claimstate
  , lobcode
  , lossdate
  , losstype
  , losscause
  , reporteddate
  , reportedbytype
  , datediff(reporteddate, lossdate) as days_to_report
  , description
  , segment
  , flagged
  , faultrating
  , howreported
  , incidentonly
  , jurisdiction
  , validationlevel
  , assignmentstatus
  , policynumber
  , policy_policynumber
  , policytype
  , producercode
  , policycurrency
  , strategycode
  , losslocation_address1
  , losslocation_city
  , losslocation_statecode
  , losslocation_postalcode
  , losslocation_county
  , losslocation_full
  , insured_name
  , insured_contactid
  , maincontact_name
  , maincontact_id
  , reporter_name
  , reporter_id
  , assigneduser_name
  , assigneduser_id
  , assignedgroup_name
  , assignedgroup_id
  , assignedbyuser_name
  , assignedbyuser_id
  , sourcesystem
  , eventtype
  , execution_id
  , year
  , month
  , day

FROM (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY claimnumber
        ORDER BY reporteddate DESC
    ) as rn
    FROM gwclaimcenter.claims
)
WHERE rn = 1

ORDER BY lossdate DESC, claimnumber ASC

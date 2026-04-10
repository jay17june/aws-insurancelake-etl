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
  , strategycode
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
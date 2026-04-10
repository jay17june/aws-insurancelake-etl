-- Current-state exposures: deduplicate by claimid, keeping the most recent event
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
  , description
  , segment
  , flagged
  , faultrating
  , incidentonly
  , assignmentstatus
  , policynumber
  , sourcesystem
  , eventtype
  , execution_id
  , year
  , month
  , day

FROM (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY claimid
        ORDER BY reporteddate DESC
    ) as rn
    FROM gwclaimcenter.exposures
)
WHERE rn = 1

ORDER BY lossdate DESC, claimnumber ASC
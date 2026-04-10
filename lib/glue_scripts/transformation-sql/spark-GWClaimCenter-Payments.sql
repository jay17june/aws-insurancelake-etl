-- Current-state payments: deduplicate by paymentid, keeping the most recent event
SELECT
    paymentid
  , claimid
  , claimnumber
  , description
  , lobcode
  , losstype
  , segment
  , policynumber
  , costtype
  , costcategory
  , coverage
  , paymenttype
  , paymentstatus
  , currency
  , createdvia
  , checknumber
  , createtime
  , issuedate
  , exposure_name
  , exposure_id
  , reserve_id
  , assigneduser_name
  , assigneduser_id
  , assignedgroup_name
  , assignedgroup_id
  , validationlevel
  , sourcesystem
  , eventtype
  , execution_id
  , year
  , month
  , day

FROM (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY paymentid
        ORDER BY createtime DESC
    ) as rn
    FROM gwclaimcenter.payments
)
WHERE rn = 1

ORDER BY createtime DESC, claimnumber ASC

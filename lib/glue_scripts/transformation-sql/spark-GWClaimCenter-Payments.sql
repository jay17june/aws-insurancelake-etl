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
  , checknumber
  , createtime
  , issuedate
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
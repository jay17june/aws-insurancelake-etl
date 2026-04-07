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

  , gwclaimcenter.payments.execution_id
  , gwclaimcenter.payments.year
  , gwclaimcenter.payments.month
  , gwclaimcenter.payments.day

FROM
    gwclaimcenter.payments

ORDER BY createtime DESC, claimnumber ASC

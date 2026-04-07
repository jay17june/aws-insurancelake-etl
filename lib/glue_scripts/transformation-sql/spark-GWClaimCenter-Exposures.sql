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
  , policy_policynumber
  , policytype
  , policyeffectivedate
  , policyexpirationdate
  , losslocation_address1
  , losslocation_city
  , losslocation_statecode
  , losslocation_postalcode
  , coverageinquestion
  , assigneduser_id
  , assignedgroup_id
  , sourcesystem
  , eventtype

  , gwclaimcenter.exposures.execution_id
  , gwclaimcenter.exposures.year
  , gwclaimcenter.exposures.month
  , gwclaimcenter.exposures.day

FROM
    gwclaimcenter.exposures

ORDER BY lossdate DESC, claimnumber ASC

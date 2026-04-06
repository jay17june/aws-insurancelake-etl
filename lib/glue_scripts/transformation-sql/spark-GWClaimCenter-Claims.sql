SELECT
    claimid
  , claimnumber
  , claimstate
  , lobcode
  , lobname
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
  , policyid
  , policyeffectivedate
  , policyexpirationdate
  , policyproductcode
  , strategycode
  , strategyname
  , losslocation_address1
  , losslocation_address2
  , losslocation_city
  , losslocation_statecode
  , losslocation_statename
  , losslocation_fullstatename
  , losslocation_postalcode
  , losslocation_country
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

  , gwclaimcenter.claims.execution_id
  , gwclaimcenter.claims.year
  , gwclaimcenter.claims.month
  , gwclaimcenter.claims.day

FROM
    gwclaimcenter.claims

ORDER BY lossdate DESC, claimnumber ASC

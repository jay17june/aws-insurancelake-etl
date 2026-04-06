-- Athena view: Flattened claim activities from nested JSON
-- Query activities using: SELECT * FROM gwclaimcenter.vw_claim_activities WHERE claimnumber = 'CLM-123'
CREATE OR REPLACE VIEW gwclaimcenter.vw_claim_activities AS
SELECT
    c.claimnumber
  , c.claimstate
  , c.lobcode
  , c.lossdate
  , c.policynumber
  , c.insured_name
  , activity.key as activity_id
  , json_extract_scalar(activity.value, '$.subject') as activity_subject
  , json_extract_scalar(activity.value, '$.description') as activity_description
  , json_extract_scalar(activity.value, '$.status') as activity_status
  , json_extract_scalar(activity.value, '$.priority') as activity_priority
  , json_extract_scalar(activity.value, '$.dueDate') as activity_duedate
  , json_extract_scalar(activity.value, '$.assignedUser.displayName') as activity_assigneduser
  , json_extract_scalar(activity.value, '$.activityType') as activity_type
  , c.year
  , c.month
  , c.day
FROM
    gwclaimcenter.claims c
CROSS JOIN UNNEST(cast(json_parse(c.activities) as map(varchar, json))) as t(activity)
;

-- Athena view: Flattened claim exposures from nested JSON
-- Query exposures using: SELECT * FROM gwclaimcenter.vw_claim_exposures WHERE claimnumber = 'CLM-123'
CREATE OR REPLACE VIEW gwclaimcenter.vw_claim_exposures AS
SELECT
    c.claimnumber
  , c.claimstate
  , c.lobcode
  , c.lossdate
  , c.policynumber
  , json_extract_scalar(exposure, '$.id') as exposure_id
  , json_extract_scalar(exposure, '$.coverageType') as coverage_type
  , json_extract_scalar(exposure, '$.state') as exposure_state
  , json_extract_scalar(exposure, '$.claimantType') as claimant_type
  , json_extract_scalar(exposure, '$.segment') as exposure_segment
  , json_extract_scalar(exposure, '$.coverageSubType') as coverage_subtype
  , json_extract_scalar(exposure, '$.lossParty') as loss_party
  , c.year
  , c.month
  , c.day
FROM
    gwclaimcenter.claims c
CROSS JOIN UNNEST(cast(json_parse(c.exposures) as array(json))) as t(exposure)
;

-- Athena view: Flattened claim reserves from nested JSON
-- Query reserves using: SELECT * FROM gwclaimcenter.vw_claim_reserves WHERE claimnumber = 'CLM-123'
CREATE OR REPLACE VIEW gwclaimcenter.vw_claim_reserves AS
SELECT
    c.claimnumber
  , c.claimstate
  , c.lobcode
  , c.lossdate
  , c.policynumber
  , json_extract_scalar(reserve, '$.id') as reserve_id
  , cast(json_extract_scalar(reserve, '$.reserveAmount') as double) as reserve_amount
  , json_extract_scalar(reserve, '$.costType') as cost_type
  , json_extract_scalar(reserve, '$.costCategory') as cost_category
  , json_extract_scalar(reserve, '$.exposure.id') as exposure_id
  , json_extract_scalar(reserve, '$.reserveLine.name') as reserve_line
  , c.year
  , c.month
  , c.day
FROM
    gwclaimcenter.claims c
CROSS JOIN UNNEST(cast(json_parse(c.reserves) as array(json))) as t(reserve)
;

-- Athena view: Flattened claim contacts from nested JSON
-- Query contacts using: SELECT * FROM gwclaimcenter.vw_claim_contacts WHERE claimnumber = 'CLM-123'
CREATE OR REPLACE VIEW gwclaimcenter.vw_claim_contacts AS
SELECT
    c.claimnumber
  , c.claimstate
  , c.policynumber
  , contact.key as contact_id
  , json_extract_scalar(contact.value, '$.displayName') as contact_name
  , json_extract_scalar(contact.value, '$.contactType') as contact_type
  , json_extract_scalar(contact.value, '$.primaryAddress.city') as contact_city
  , json_extract_scalar(contact.value, '$.primaryAddress.state.code') as contact_state
  , json_extract_scalar(contact.value, '$.emailAddress1') as contact_email
  , json_extract_scalar(contact.value, '$.primaryPhoneNumber') as contact_phone
  , c.year
  , c.month
  , c.day
FROM
    gwclaimcenter.claims c
CROSS JOIN UNNEST(cast(json_parse(c.contacts) as map(varchar, json))) as t(contact)
;

-- Athena view: Flattened vehicle incidents from nested JSON
-- Query incidents using: SELECT * FROM gwclaimcenter.vw_claim_vehicle_incidents WHERE claimnumber = 'CLM-123'
CREATE OR REPLACE VIEW gwclaimcenter.vw_claim_vehicle_incidents AS
SELECT
    c.claimnumber
  , c.claimstate
  , c.lobcode
  , c.lossdate
  , c.policynumber
  , json_extract_scalar(incident, '$.id') as incident_id
  , json_extract_scalar(incident, '$.severity') as severity
  , json_extract_scalar(incident, '$.vehicleType') as vehicle_type
  , json_extract_scalar(incident, '$.lossParty') as loss_party
  , json_extract_scalar(incident, '$.vehicle.make') as vehicle_make
  , json_extract_scalar(incident, '$.vehicle.model') as vehicle_model
  , json_extract_scalar(incident, '$.vehicle.year') as vehicle_year
  , json_extract_scalar(incident, '$.vehicle.vin') as vehicle_vin
  , c.year
  , c.month
  , c.day
FROM
    gwclaimcenter.claims c
CROSS JOIN UNNEST(cast(json_parse(c.vehicle_incidents) as array(json))) as t(incident)
;

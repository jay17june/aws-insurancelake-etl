-- Athena view: Flattened claim activities from nested JSON
-- Query: SELECT * FROM gwclaimcenter.vw_claim_activities WHERE claimnumber = '000-00-009898'
CREATE OR REPLACE VIEW gwclaimcenter.vw_claim_activities AS
SELECT
    c.claimnumber
  , c.claimstate
  , c.lobcode
  , c.lossdate
  , c.policynumber
  , c.insured_name
  , activity_key as activity_id
  , json_extract_scalar(activity_value, '$.subject') as activity_subject
  , json_extract_scalar(activity_value, '$.description') as activity_description
  , json_extract_scalar(activity_value, '$.status.code') as activity_status
  , json_extract_scalar(activity_value, '$.priority.code') as activity_priority
  , json_extract_scalar(activity_value, '$.dueDate') as activity_duedate
  , json_extract_scalar(activity_value, '$.assignedUser.displayName') as activity_assigneduser
  , json_extract_scalar(activity_value, '$.activityType.code') as activity_type
  , c.year
  , c.month
  , c.day
FROM
    gwclaimcenter.claims c
CROSS JOIN UNNEST(cast(json_parse(c.activities) as map(varchar, json))) as t(activity_key, activity_value)
;

-- Athena view: Flattened claim exposures from nested JSON
-- Query: SELECT * FROM gwclaimcenter.vw_claim_exposures WHERE claimnumber = '000-00-009898'
CREATE OR REPLACE VIEW gwclaimcenter.vw_claim_exposures AS
SELECT
    c.claimnumber
  , c.claimstate
  , c.lobcode
  , c.lossdate
  , c.policynumber
  , exposure_key as exposure_id
  , json_extract_scalar(exposure_value, '$.coverageType') as coverage_type
  , json_extract_scalar(exposure_value, '$.state.code') as exposure_state
  , json_extract_scalar(exposure_value, '$.claimantType') as claimant_type
  , json_extract_scalar(exposure_value, '$.segment.code') as exposure_segment
  , json_extract_scalar(exposure_value, '$.lossParty.code') as loss_party
  , c.year
  , c.month
  , c.day
FROM
    gwclaimcenter.claims c
CROSS JOIN UNNEST(cast(json_parse(c.exposures) as map(varchar, json))) as t(exposure_key, exposure_value)
;

-- Athena view: Flattened claim reserves from nested JSON
-- Query: SELECT * FROM gwclaimcenter.vw_claim_reserves WHERE claimnumber = '000-00-009898'
CREATE OR REPLACE VIEW gwclaimcenter.vw_claim_reserves AS
SELECT
    c.claimnumber
  , c.claimstate
  , c.lobcode
  , c.lossdate
  , c.policynumber
  , reserve_key as reserve_id
  , json_extract_scalar(reserve_value, '$.costType.code') as cost_type
  , json_extract_scalar(reserve_value, '$.costCategory.code') as cost_category
  , json_extract_scalar(reserve_value, '$.exposure.displayName') as exposure_name
  , json_extract_scalar(reserve_value, '$.reserveLine.displayName') as reserve_line
  , json_extract_scalar(reserve_value, '$.reservingAmount.amount') as reserving_amount
  , json_extract_scalar(reserve_value, '$.reservingAmount.currency') as reserving_currency
  , json_extract_scalar(reserve_value, '$.status.code') as reserve_status
  , c.year
  , c.month
  , c.day
FROM
    gwclaimcenter.claims c
CROSS JOIN UNNEST(cast(json_parse(c.reserves) as map(varchar, json))) as t(reserve_key, reserve_value)
;

-- Athena view: Flattened claim contacts from nested JSON
-- Query: SELECT * FROM gwclaimcenter.vw_claim_contacts WHERE claimnumber = '000-00-009898'
CREATE OR REPLACE VIEW gwclaimcenter.vw_claim_contacts AS
SELECT
    c.claimnumber
  , c.claimstate
  , c.policynumber
  , contact_key as contact_id
  , json_extract_scalar(contact_value, '$.displayName') as contact_name
  , json_extract_scalar(contact_value, '$.contactType') as contact_type
  , json_extract_scalar(contact_value, '$.primaryAddress.city') as contact_city
  , json_extract_scalar(contact_value, '$.primaryAddress.state.code') as contact_state
  , json_extract_scalar(contact_value, '$.emailAddress1') as contact_email
  , json_extract_scalar(contact_value, '$.primaryPhoneNumber') as contact_phone
  , c.year
  , c.month
  , c.day
FROM
    gwclaimcenter.claims c
CROSS JOIN UNNEST(cast(json_parse(c.contacts) as map(varchar, json))) as t(contact_key, contact_value)
;

-- Athena view: Flattened vehicle incidents from nested JSON
-- Query: SELECT * FROM gwclaimcenter.vw_claim_vehicle_incidents WHERE claimnumber = '000-00-009898'
CREATE OR REPLACE VIEW gwclaimcenter.vw_claim_vehicle_incidents AS
SELECT
    c.claimnumber
  , c.claimstate
  , c.lobcode
  , c.lossdate
  , c.policynumber
  , incident_key as incident_id
  , json_extract_scalar(incident_value, '$.lossParty.code') as loss_party
  , json_extract_scalar(incident_value, '$.vehicle.make') as vehicle_make
  , json_extract_scalar(incident_value, '$.vehicle.model') as vehicle_model
  , json_extract_scalar(incident_value, '$.vehicle.year') as vehicle_year
  , json_extract_scalar(incident_value, '$.vehicle.vin') as vehicle_vin
  , json_extract_scalar(incident_value, '$.vehicle.color') as vehicle_color
  , json_extract_scalar(incident_value, '$.vehicle.licensePlate') as vehicle_licenseplate
  , c.year
  , c.month
  , c.day
FROM
    gwclaimcenter.claims c
CROSS JOIN UNNEST(cast(json_parse(c.vehicle_incidents) as map(varchar, json))) as t(incident_key, incident_value)
;

-- Athena views for Guidewire ClaimCenter consume (gold copy) layer
-- These views flatten nested JSON columns into queryable tables
-- Run each CREATE VIEW statement individually in the Athena console
--
-- Prerequisites: The consume table gwclaimcenter_consume.claims must exist
-- and contain rows with the referenced nested columns (activities, exposures, etc.)
-- Check available columns: DESCRIBE gwclaimcenter_consume.claims;


-- View 1: Flattened claim activities
-- Query: SELECT * FROM gwclaimcenter_consume.vw_claim_activities WHERE claimnumber = '000-00-066666'
CREATE OR REPLACE VIEW gwclaimcenter_consume.vw_claim_activities AS
SELECT
    c.claimnumber
  , c.id as claimid
  , c.lobcode
  , c.lossdate
  , c.policynumber
  , activity_key as activity_id
  , json_extract_scalar(activity_value, '$.subject') as activity_subject
  , json_extract_scalar(activity_value, '$.description') as activity_description
  , json_extract_scalar(activity_value, '$.status') as activity_status
  , json_extract_scalar(activity_value, '$.priority') as activity_priority
  , json_extract_scalar(activity_value, '$.dueDate') as activity_duedate
  , json_extract_scalar(activity_value, '$.assignedUser.displayName') as activity_assigneduser
  , c.year
  , c.month
  , c.day
FROM
    gwclaimcenter_consume.claims c
CROSS JOIN UNNEST(cast(json_parse(c.activities) as map(varchar, json))) as t(activity_key, activity_value)
WHERE c.activities IS NOT NULL AND c.activities != '{}';


-- View 2: Flattened claim exposures
-- Query: SELECT * FROM gwclaimcenter_consume.vw_claim_exposures WHERE claimnumber = '000-00-066666'
CREATE OR REPLACE VIEW gwclaimcenter_consume.vw_claim_exposures AS
SELECT
    c.claimnumber
  , c.id as claimid
  , c.lobcode
  , c.lossdate
  , c.policynumber
  , exposure_key as exposure_id
  , json_extract_scalar(exposure_value, '$.state') as exposure_state
  , json_extract_scalar(exposure_value, '$.claimantType') as claimant_type
  , json_extract_scalar(exposure_value, '$.lossParty') as loss_party
  , json_extract_scalar(exposure_value, '$.segment') as exposure_segment
  , c.year
  , c.month
  , c.day
FROM
    gwclaimcenter_consume.claims c
CROSS JOIN UNNEST(cast(json_parse(c.exposures) as map(varchar, json))) as t(exposure_key, exposure_value)
WHERE c.exposures IS NOT NULL AND c.exposures != '{}';


-- View 3: Flattened claim reserves
-- Query: SELECT * FROM gwclaimcenter_consume.vw_claim_reserves WHERE claimnumber = '000-00-066666'
CREATE OR REPLACE VIEW gwclaimcenter_consume.vw_claim_reserves AS
SELECT
    c.claimnumber
  , c.id as claimid
  , c.lobcode
  , c.policynumber
  , reserve_key as reserve_id
  , json_extract_scalar(reserve_value, '$.costType') as cost_type
  , json_extract_scalar(reserve_value, '$.costCategory') as cost_category
  , json_extract_scalar(reserve_value, '$.exposure.displayName') as exposure_name
  , json_extract_scalar(reserve_value, '$.reserveLine.displayName') as reserve_line
  , json_extract_scalar(reserve_value, '$.reservingAmount.amount') as reserving_amount
  , json_extract_scalar(reserve_value, '$.status') as reserve_status
  , c.year
  , c.month
  , c.day
FROM
    gwclaimcenter_consume.claims c
CROSS JOIN UNNEST(cast(json_parse(c.reserves) as map(varchar, json))) as t(reserve_key, reserve_value)
WHERE c.reserves IS NOT NULL AND c.reserves != '{}';


-- View 4: Flattened claim contacts
-- Query: SELECT * FROM gwclaimcenter_consume.vw_claim_contacts WHERE claimnumber = '000-00-066666'
CREATE OR REPLACE VIEW gwclaimcenter_consume.vw_claim_contacts AS
SELECT
    c.claimnumber
  , c.id as claimid
  , c.policynumber
  , contact_key as contact_id
  , json_extract_scalar(contact_value, '$.displayName') as contact_name
  , json_extract_scalar(contact_value, '$.contactType') as contact_type
  , json_extract_scalar(contact_value, '$.primaryAddress.city') as contact_city
  , json_extract_scalar(contact_value, '$.primaryAddress.state.code') as contact_state
  , json_extract_scalar(contact_value, '$.emailAddress1') as contact_email
  , c.year
  , c.month
  , c.day
FROM
    gwclaimcenter_consume.claims c
CROSS JOIN UNNEST(cast(json_parse(c.contacts) as map(varchar, json))) as t(contact_key, contact_value)
WHERE c.contacts IS NOT NULL AND c.contacts != '{}';

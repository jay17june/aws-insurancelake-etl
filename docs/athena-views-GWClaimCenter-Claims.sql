-- Athena views for Guidewire ClaimCenter consume (gold copy) layer
-- These views flatten nested JSON columns into queryable tables
--
-- ⚠️  IMPORTANT: Only create views for columns that exist in your table
-- Check available columns first: DESCRIBE gwclaimcenter_consume.claims;
--
-- Create each view individually based on what nested JSON columns are available:


-- View 1: Claim Contacts (if 'contacts' column exists)
-- Prerequisites: gwclaimcenter_consume.claims must have 'contacts' column
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


-- View 2: Claim Exposures (if 'exposures' column exists)
-- Prerequisites: gwclaimcenter_consume.claims must have 'exposures' column
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


-- View 3: Policy Addresses (if 'policyaddresses' column exists)
-- Prerequisites: gwclaimcenter_consume.claims must have 'policyaddresses' column
CREATE OR REPLACE VIEW gwclaimcenter_consume.vw_claim_policy_addresses AS
SELECT
    c.claimnumber
  , c.id as claimid
  , c.policynumber
  , json_extract_scalar(address_value, '$.addressLine1') as address_line1
  , json_extract_scalar(address_value, '$.city') as city
  , json_extract_scalar(address_value, '$.state.code') as state_code
  , json_extract_scalar(address_value, '$.postalCode') as postal_code
  , c.year
  , c.month
  , c.day
FROM
    gwclaimcenter_consume.claims c
CROSS JOIN UNNEST(cast(json_parse(c.policyaddresses) as array(json))) as t(address_value)
WHERE c.policyaddresses IS NOT NULL AND c.policyaddresses != '{}';


-- View 4: Vehicle Incidents (if 'vehicle-incidents' column exists)
-- Prerequisites: gwclaimcenter_consume.claims must have 'vehicle-incidents' column
CREATE OR REPLACE VIEW gwclaimcenter_consume.vw_claim_vehicle_incidents AS
SELECT
    c.claimnumber
  , c.id as claimid
  , c.lobcode
  , incident_key as incident_id
  , json_extract_scalar(incident_value, '$.lossParty') as loss_party
  , json_extract_scalar(incident_value, '$.vehicle.make') as vehicle_make
  , json_extract_scalar(incident_value, '$.vehicle.model') as vehicle_model
  , json_extract_scalar(incident_value, '$.vehicle.year') as vehicle_year
  , json_extract_scalar(incident_value, '$.vehicle.vin') as vehicle_vin
  , c.year
  , c.month
  , c.day
FROM
    gwclaimcenter_consume.claims c
CROSS JOIN UNNEST(cast(json_parse(c."vehicle-incidents") as map(varchar, json))) as t(incident_key, incident_value)
WHERE c."vehicle-incidents" IS NOT NULL AND c."vehicle-incidents" != '{}';


-- FUTURE VIEWS: Create these when data with these fields arrives
--
-- View 5: Claim Activities (when 'activities' column exists)
-- Uncomment and run when you have processed events containing activities:
/*
CREATE OR REPLACE VIEW gwclaimcenter_consume.vw_claim_activities AS
SELECT
    c.claimnumber
  , c.id as claimid
  , activity_key as activity_id
  , json_extract_scalar(activity_value, '$.subject') as activity_subject
  , json_extract_scalar(activity_value, '$.status') as activity_status
FROM
    gwclaimcenter_consume.claims c
CROSS JOIN UNNEST(cast(json_parse(c.activities) as map(varchar, json))) as t(activity_key, activity_value)
WHERE c.activities IS NOT NULL AND c.activities != '{}';
*/

-- View 6: Claim Reserves (when 'reserves' column exists)
-- Uncomment and run when you have processed events containing reserves:
/*
CREATE OR REPLACE VIEW gwclaimcenter_consume.vw_claim_reserves AS
SELECT
    c.claimnumber
  , c.id as claimid
  , reserve_key as reserve_id
  , json_extract_scalar(reserve_value, '$.costType') as cost_type
  , json_extract_scalar(reserve_value, '$.reservingAmount.amount') as reserving_amount
FROM
    gwclaimcenter_consume.claims c
CROSS JOIN UNNEST(cast(json_parse(c.reserves) as map(varchar, json))) as t(reserve_key, reserve_value)
WHERE c.reserves IS NOT NULL AND c.reserves != '{}';
*/
-- Dynamic schema consume SQL: select all available fields
-- InsuranceLake auto-cleans column names, so reference the cleaned versions
SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY claimnumber  -- Auto-cleaned from claimNumber
        ORDER BY reporteddate DESC  -- Auto-cleaned from reportedDate
    ) as rn
    FROM gwclaimcenter.claims
)
WHERE rn = 1

ORDER BY COALESCE(lossdate, date('1900-01-01')) DESC, claimnumber ASC
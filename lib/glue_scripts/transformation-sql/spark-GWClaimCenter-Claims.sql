-- Dynamic schema consume SQL: select all available fields
SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY claimnumber
        ORDER BY COALESCE(reporteddate, lossdate, current_date()) DESC
    ) as rn
    FROM gwclaimcenter.claims
)
WHERE rn = 1
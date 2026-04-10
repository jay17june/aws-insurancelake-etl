-- Dynamic schema consume SQL for exposures: select all available fields
SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY id
        ORDER BY COALESCE(reporteddate, lossdate, current_timestamp()) DESC
    ) as rn
    FROM gwclaimcenter.exposures
)
WHERE rn = 1
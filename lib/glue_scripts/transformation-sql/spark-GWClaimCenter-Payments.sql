-- Dynamic schema consume SQL for payments: select all available fields
SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY id
        ORDER BY COALESCE(createtime, current_timestamp()) DESC
    ) as rn
    FROM gwclaimcenter.payments
)
WHERE rn = 1
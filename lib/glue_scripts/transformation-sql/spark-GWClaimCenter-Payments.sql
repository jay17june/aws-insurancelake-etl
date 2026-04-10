-- Dynamic schema consume SQL for payments: select all available fields
SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY COALESCE(id, paymentid, claimid)  -- Try multiple ID fields
        ORDER BY COALESCE(createtime, issuedate, current_timestamp()) DESC
    ) as rn
    FROM gwclaimcenter.payments
)
WHERE rn = 1
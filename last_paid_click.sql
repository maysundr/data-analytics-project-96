WITH tab AS (
    SELECT
        s.visitor_id,
        s.visit_date,
        s.source AS utm_source,
        s.medium AS utm_medium,
        s.campaign AS utm_campaign,
        l.lead_id,
        l.created_at,
        l.amount,
        l.closing_reason,
        l.status_id
    FROM sessions AS s
    LEFT JOIN leads AS l
        ON
            s.visitor_id = l.visitor_id
            AND s.visit_date <= l.created_at
    WHERE medium <> 'organic'
    ORDER BY s.visit_date DESC
),

bat AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY tab.visitor_id order by tab.visit_date desc) AS rn
    FROM tab
)

SELECT
    visitor_id,
    to_char(visit_date, 'YYYY-MM-DD') as visit_date,
    utm_source,
    utm_medium,
    utm_campaign,
    lead_id,
    created_at,
    amount,
    closing_reason,
    status_id
FROM bat
WHERE rn = '1'
ORDER BY
    amount DESC NULLS LAST, visit_date ASC, utm_source ASC, utm_medium ASC, utm_campaign ASC
--limit 10;
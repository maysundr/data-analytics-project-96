WITH end_cost AS (
    SELECT *
    FROM (
        SELECT
            TO_CHAR(campaign_date, 'YYYY-MM-DD') AS visit_date,
            SUM(va.daily_spent) AS cost,
            utm_source,
            utm_medium,
            utm_campaign
        FROM vk_ads AS va
        GROUP BY
            campaign_date, utm_source,
            utm_medium,
            utm_campaign

        UNION ALL

        SELECT
            TO_CHAR(campaign_date, 'YYYY-MM-DD') AS visit_date,
            SUM(ya.daily_spent) AS cost,
            utm_source,
            utm_medium,
            utm_campaign
        FROM ya_ads AS ya
        GROUP BY
            campaign_date, utm_source,
            utm_medium,
            utm_campaign
    ) AS tab_1
    ORDER BY visit_date

)
,

tab AS (
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
        l.status_id,
        ROW_NUMBER()
            OVER (PARTITION BY s.visitor_id ORDER BY visit_date DESC)
            AS rn
    FROM sessions AS s
    LEFT JOIN leads AS l
        ON
            s.visitor_id = l.visitor_id
            AND s.visit_date <= l.created_at
    WHERE medium <> 'organic'
    ORDER BY s.visit_date DESC
),

first_step AS (

    SELECT
        visitor_id,
        utm_source,
        utm_medium,
        utm_campaign,
        lead_id,
        created_at,
        amount,
        closing_reason,
        status_id,
        TO_CHAR(visit_date, 'YYYY-MM-DD') AS visit_date
    FROM tab
    WHERE rn = '1'
    ORDER BY
        amount DESC NULLS LAST,
        visit_date ASC,
        utm_source ASC,
        utm_medium ASC,
        utm_campaign ASC
),

second_step AS (
    SELECT
        first_step.visit_date,
        first_step.utm_source,
        first_step.utm_medium,
        first_step.utm_campaign,
        COUNT(first_step.visitor_id) AS visitors_count,
        COUNT(first_step.lead_id) AS leads_count,
        SUM(CASE WHEN first_step.status_id = '142' THEN 1 ELSE 0 END)
            AS purchases_count,
        SUM(
            CASE
                WHEN first_step.status_id = '142' THEN first_step.amount ELSE 0
            END
        ) AS revenue
    FROM first_step
    GROUP BY
        first_step.visit_date,
        first_step.utm_source,
        first_step.utm_medium,
        first_step.utm_campaign
)

SELECT
    second_step.visit_date,
    visitors_count,
    second_step.utm_source,
    second_step.utm_medium,
    second_step.utm_campaign,
    end_cost.cost AS total_cost,
    leads_count,
    purchases_count,
    revenue
FROM second_step
LEFT JOIN end_cost
    ON
        second_step.visit_date = end_cost.visit_date
        AND second_step.utm_source = end_cost.utm_source
        AND second_step.utm_medium = end_cost.utm_medium
        AND second_step.utm_campaign = end_cost.utm_campaign
GROUP BY
    second_step.visit_date,
    second_step.utm_source,
    second_step.utm_medium,
    second_step.utm_campaign,
    end_cost.cost,
    visitors_count,
    leads_count,
    purchases_count,
    revenue
ORDER BY
    revenue DESC NULLS LAST,
	visit_date ASC,
	visitors_count DESC,
	second_step.utm_source ASC,
	utm_medium ASC,
	utm_campaign ASC;
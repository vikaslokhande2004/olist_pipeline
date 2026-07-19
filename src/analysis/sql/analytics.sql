-- ---------------------------------------------
-- REPORT 1 — ORDER STATUS DASHBOARD
-- ---------------------------------------------

WITH order_stats AS (
    SELECT
        order_status,
        COUNT(*)                    AS total,
        ROUND(AVG(delivery_days),1) AS avg_days,
        SUM(CASE WHEN is_late = 1
            THEN 1 ELSE 0 END)      AS late_count,
        ROUND(
            SUM(CASE WHEN is_late=1
                THEN 1 ELSE 0 END)
            * 100.0 / COUNT(*), 2
        )                           AS late_pct
    FROM orders
    GROUP BY order_status
),
ranked_status AS (
    SELECT *,
        RANK() OVER(ORDER BY total DESC)
            AS popularity_rank
    FROM order_stats
)
SELECT *
FROM   ranked_status
ORDER BY popularity_rank;

-- ---------------------------------------------
-- REPORT 2 — MONTHLY REVENUE TREND
--            WITH LAG COMPARISON
-- ---------------------------------------------

WITH monthly_rev AS (
    SELECT
        purchase_year   AS yr,
        purchase_month  AS mth,
        COUNT(*)        AS orders,
        ROUND(SUM(order_total),2) AS revenue
    FROM orders
    WHERE order_status = 'delivered'
      AND order_total IS NOT NULL
    GROUP BY purchase_year, purchase_month
),
monthly_with_lag AS (
    SELECT
        yr, mth, orders, revenue,
        LAG(revenue) OVER(
            ORDER BY yr, mth
        )                  AS prev_month_rev,
        ROUND(revenue - LAG(revenue) OVER(
            ORDER BY yr, mth
        ), 2)              AS revenue_change,
        ROUND(
            (revenue - LAG(revenue) OVER(
                ORDER BY yr, mth))
            * 100.0
            / NULLIF(LAG(revenue) OVER(
                ORDER BY yr, mth), 0)
        , 2)               AS growth_pct
    FROM monthly_rev
)
SELECT * FROM monthly_with_lag
ORDER BY yr, mth;

-- ---------------------------------------------
-- REPORT 3 — CUSTOMER STATE PERFORMANCE
--            RANKED BY REVENUE
-- ---------------------------------------------

WITH state_stats AS (
    SELECT
        customer_state,
        COUNT(DISTINCT order_id) AS orders,
        COUNT(DISTINCT customer_id)
            AS customers,
        ROUND(SUM(order_total),2)
            AS total_revenue,
        ROUND(AVG(order_total),2)
            AS avg_order_value,
        ROUND(AVG(delivery_days),1)
            AS avg_delivery_days
    FROM orders
    WHERE order_status = 'delivered'
    GROUP BY customer_state
),
state_ranked AS (
    SELECT *,
        DENSE_RANK() OVER(
            ORDER BY total_revenue DESC
        ) AS revenue_rank,
        NTILE(4) OVER(
            ORDER BY total_revenue DESC
        ) AS revenue_quartile
    FROM state_stats
),
final_state AS (
    SELECT *,
        CASE revenue_quartile
            WHEN 1 THEN 'Top Performer'
            WHEN 2 THEN 'Strong'
            WHEN 3 THEN 'Average'
            WHEN 4 THEN 'Below Average'
        END AS performance_tier,
        SUM(total_revenue) OVER()
            AS grand_total,
        ROUND(
            total_revenue * 100.0
            / SUM(total_revenue) OVER()
        , 2) AS revenue_share_pct
    FROM state_ranked
)
SELECT * FROM final_state
ORDER BY revenue_rank;

-- ---------------------------------------------
-- REPORT 4 — RUNNING TOTAL AND CUMULATIVE
--            REVENUE BY DATE
-- ---------------------------------------------

WITH daily_rev AS (
    SELECT
        TRUNC(order_purchase_timestamp)
            AS order_date,
        COUNT(*) AS daily_orders,
        ROUND(SUM(order_total),2)
            AS daily_revenue
    FROM orders
    WHERE order_status = 'delivered'
      AND order_total IS NOT NULL
    GROUP BY TRUNC(order_purchase_timestamp)
)
SELECT
    order_date,
    daily_orders,
    daily_revenue,
    SUM(daily_revenue) OVER(
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING
                 AND CURRENT ROW
    )                        AS cumulative_revenue,
    ROUND(daily_revenue * 100.0
        / SUM(daily_revenue) OVER()
    , 3)                     AS pct_of_total,
    AVG(daily_revenue) OVER(
        ORDER BY order_date
        ROWS BETWEEN 6 PRECEDING
                 AND CURRENT ROW
    )                        AS rolling_7day_avg
FROM daily_rev
ORDER BY order_date;

-- ---------------------------------------------
-- REPORT 5 — LATE DELIVERY ANALYSIS
--            WITH WINDOW + CASE
-- ---------------------------------------------

SELECT
    customer_state,
    order_status,
    delivery_days,
    CASE
        WHEN delivery_days <= 5  THEN 'Fast'
        WHEN delivery_days <= 15 THEN 'Normal'
        WHEN delivery_days <= 30 THEN 'Slow'
        WHEN delivery_days > 30  THEN 'Very Slow'
        ELSE 'Unknown'
    END AS delivery_speed,
    RANK() OVER(
        PARTITION BY customer_state
        ORDER BY delivery_days DESC
    ) AS slowest_in_state,
    ROUND(AVG(delivery_days) OVER(
        PARTITION BY customer_state
    ), 1) AS state_avg_days,
    delivery_days - ROUND(
        AVG(delivery_days) OVER(
            PARTITION BY customer_state
        ), 1) AS vs_state_avg
FROM orders
WHERE order_status = 'delivered'
  AND delivery_days IS NOT NULL
ORDER BY customer_state, delivery_days DESC;

Create this as a view for reuse:

CREATE OR REPLACE VIEW delivery_analysis_vw AS
SELECT ... (paste full query above) ...;

SELECT * FROM delivery_analysis_vw
WHERE delivery_speed = 'Very Slow'
ORDER BY delivery_days DESC
FETCH FIRST 20 ROWS ONLY;

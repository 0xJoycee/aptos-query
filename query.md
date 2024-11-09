1. Weekly NFT Sales and Buyer Analysis
This query calculates weekly sales, unique buyers, and cumulative totals for the last 30 days.

```sql
WITH recent_sales AS (
    SELECT
        BLOCK_TIMESTAMP,
        TOTAL_PRICE,
        BUYER_ADDRESS
    FROM
        aptos.nft.ez_nft_sales
    WHERE
        BLOCK_TIMESTAMP >= DATEADD(day, -30, CURRENT_DATE)
)
SELECT
    DATE_TRUNC('week', BLOCK_TIMESTAMP) AS week,
    COUNT(*) AS weekly_nft_sales,
    COUNT(DISTINCT BUYER_ADDRESS) AS weekly_nft_buyers,
    SUM(TOTAL_PRICE) AS weekly_sales_volume,
    SUM(COUNT(*)) OVER (ORDER BY DATE_TRUNC('week', BLOCK_TIMESTAMP)) AS total_nft_sales,
    SUM(COUNT(DISTINCT BUYER_ADDRESS)) OVER (ORDER BY DATE_TRUNC('week', BLOCK_TIMESTAMP)) AS total_nft_buyers
FROM
    recent_sales
GROUP BY
    DATE_TRUNC('week', BLOCK_TIMESTAMP)
ORDER BY
    week DESC;
```

NFT Price Analysis
This query calculates the average, median, and maximum distinct NFT prices for the last 30 days.

```sql
WITH price_data AS (
    SELECT
        TOTAL_PRICE
    FROM
        aptos.nft.ez_nft_sales
    WHERE
        PROJECT_NAME NOT IN ('Cellana Voting Tokens')
        AND BLOCK_TIMESTAMP >= DATEADD(day, -30, CURRENT_DATE)
),
distinct_price_data AS (
    SELECT DISTINCT
        TOTAL_PRICE
    FROM
        price_data
)
SELECT
    AVG(TOTAL_PRICE) AS "Avg Price (APT)",
    MEDIAN(TOTAL_PRICE) AS "Median Price (APT)",
    MAX(TOTAL_PRICE) AS "Max Price (APT)"
FROM
    distinct_price_data;
```

---

Weekly Average Price of APT
This query calculates the weekly average price for the 'APT' token.

```sql
WITH recent_prices AS (
    SELECT
        DATE_TRUNC('week', hour) AS week,
        symbol,
        AVG(DISTINCT price) AS avg_weekly_price
    FROM
        aptos.price.ez_prices_hourly
    WHERE
        hour >= DATEADD(day, -30, CURRENT_DATE)
        AND symbol = 'APT'
    GROUP BY
        week, symbol
)
SELECT
    week,
    avg_weekly_price
FROM
    recent_prices
ORDER BY
    week DESC;
```

Daily NFT Sales and Volume
This query calculates daily sales, volume, and unique buyers.

```sql
WITH nft_sales_data AS (
    SELECT
        DATE_TRUNC('day', TO_TIMESTAMP(BLOCK_TIMESTAMP)) AS date,
        EVENT_DATA:price / 1e8 AS sale_price,
        EVENT_DATA:buyer AS buyer_address,
        EVENT_DATA:purchaser AS purchaser_address,
        TX_HASH
    FROM
        aptos.core.fact_events
    WHERE
        EVENT_RESOURCE IN (
            'BuyEvent', 'SellEvent', 'FillCollectionBidEvent', 
            'TokenOfferFilledEvent', 'CollectionOfferFilledEvent', 
            'ListingFilledEvent'
        )
        AND TO_TIMESTAMP(BLOCK_TIMESTAMP) >= DATEADD(day, -30, CURRENT_DATE)
)
SELECT
    DATE_TRUNC('day', date) AS day,
    COUNT(DISTINCT TX_HASH) AS daily_nft_sales,
    SUM(DISTINCT sale_price) AS daily_nft_volume,
    COUNT(DISTINCT COALESCE(buyer_address, purchaser_address)) AS daily_unique_nft_buyers
FROM
    nft_sales_data
GROUP BY
    day
ORDER BY
    day ASC;
```

---

 New and Active User Activity
This query compares user activity and calculates the percentage change in new users for the last 30 and previous 30-day periods.

```sql
WITH date_ranges AS (
    SELECT 
        CURRENT_DATE - INTERVAL '30 DAY' AS start_date,
        CURRENT_DATE AS end_date,
        'Last 30 Days' AS label
    UNION ALL
    SELECT
        CURRENT_DATE - INTERVAL '60 DAY' AS start_date,
        CURRENT_DATE - INTERVAL '30 DAY' AS end_date,
        'Previous 30 Days' AS label
),
full_table AS (
    SELECT 
        BLOCK_TIMESTAMP AS datetime,
        SENDER,
        CASE 
            WHEN BLOCK_TIMESTAMP = MIN(BLOCK_TIMESTAMP) OVER (PARTITION BY SENDER) THEN 1 
            ELSE 0 
        END AS flag
    FROM 
        aptos.core.fact_transactions
),
filtered_activity AS (
    SELECT 
        ft.datetime,
        ft.sender,
        ft.flag,
        dr.label
    FROM 
        full_table ft
    JOIN 
        date_ranges dr ON ft.datetime BETWEEN dr.start_date AND dr.end_date
),
aggregated_results AS (
    SELECT 
        label,
        COUNT(DISTINCT sender) AS active_users,
        SUM(flag) AS new_users
    FROM 
        filtered_activity
    GROUP BY 
        label
)
SELECT
    a.label AS current_period,
    b.label AS previous_period,
    a.new_users AS current_new_users,
    b.new_users AS previous_new_users,
    (a.new_users - b.new_users) * 100.0 / b.new_users AS percent_change_new_users
FROM
    aggregated_results a
JOIN
    aggregated_results b 
ON
    a.label = 'Last 30 Days' AND b.label = 'Previous 30 Days';
```

---

6. Total Active, New, and Returning Users
This query provides a summary of user activity, dividing users into active, new, and returning users.

```sql
WITH transaction_analysis AS (
    SELECT 
        DATE(block_timestamp) AS transaction_date,
        sender,
        CASE 
            WHEN block_timestamp = MIN(block_timestamp) OVER (PARTITION BY sender) THEN 'New'
            ELSE 'Returning'
        END AS user_status
    FROM 
        aptos.core.fact_transactions
    WHERE 
        block_timestamp >= DATEADD(day, -30, CURRENT_DATE)
),
summary AS (
    SELECT 
        transaction_date,
        COUNT(DISTINCT sender) AS total_active_users,
        COUNT(DISTINCT CASE WHEN user_status = 'New' THEN sender END) AS total_new_users,
        COUNT(DISTINCT CASE WHEN user_status = 'Returning' THEN sender END) AS total_returning_users
    FROM 
        transaction_analysis
    GROUP BY 
        transaction_date
)
SELECT
    SUM(total_active_users) AS "Total Active Users",
    SUM(total_new_users) AS "Total New Users",
    SUM(total_returning_users) AS "Total Returning Users"
FROM 
    summary;
```

---


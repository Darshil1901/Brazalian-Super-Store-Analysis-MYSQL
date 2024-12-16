-- top 10 cities with the highest number of orders 
SELECT 
    customer_city AS customer_city,
    customer_city AS city,
    COUNT(od.order_id) as city_order_count
FROM 
    cd
    JOIN od USING (customer_id)
GROUP BY customer_city
ORDER BY city_order_count DESC
LIMIT 10;





-- Summary of Top 5 Product Categories: Orders and Revenue Analysis
WITH t1 AS(
SELECT
    cd.customer_id,
    cd.customer_unique_id,
    pd.product_category_name,
    od.order_id,
    pd.product_id,
    oid.price,
    extract(year from od.order_purchase_timestamp) as year
FROM cd
    INNER JOIN od
        ON od.customer_id = cd.customer_id
    INNER JOIN oid
        ON oid.order_id = od.order_id
    INNER JOIN pd
        ON pd.product_id = oid.product_id
ORDER BY od.order_purchase_timestamp
        )
SELECT
    product_category_name,
    year,
    count(customer_unique_id) as total_orders,
    sum(price) as revenue,
   round(avg(price),2) as avg_revenue
FROM t1
WHERE
    product_category_name is not null
GROUP BY
    1, 2
ORDER BY 
    3 desc 
    limit 5;
    




--  Sales Analysis: Monthly Totals by Year
SELECT
Extract(month from od.order_purchase_timestamp) AS MON,
Extract(year from od.order_purchase_timestamp) AS YR,
    SUM(oid.price) AS total_sales
FROM
    od
JOIN
	oid ON 
    oid.order_id = od.order_id
GROUP BY
    YR,MON
ORDER BY
    total_sales DESC;




-- Order Price Statistics
  SELECT
    MIN(order_price) AS min_order_price,
    ROUND(AVG(order_price), 2) AS avg_order_price,
    MAX(order_price) AS max_order_price
FROM (
    SELECT
        oid.order_id,
        SUM(oid.price + oid.freight_value) AS order_price
    FROM od
        JOIN oid
        On od.order_id = oid.order_id
    GROUP BY od.order_id
) as order_summary
         -- Cost Breakdown for Delivered Orders 
	select od.order_id,
		SUM(price) AS product_cost,
		SUM(freight_value) AS shipping_cost
    FROM
		od
		JOIN oid
		on od.order_id = oid.order_id
		WHERE order_status = 'delivered'
		GROUP BY od.order_id;



-- Review Score Summary: Total Counts and Proportions by Rating Type
WITH reviews as (
SELECT
    od.order_id,
    pd.product_id,
    pd.product_category_name,
    oid.price,
    orevd.review_score,
    od.order_purchase_timestamp,
    (CASE WHEN
            orevd.review_score = 5 THEN 'Very Good' 
            WHEN orevd.review_score = 4 THEN 'Good' 
            WHEN orevd.review_score = 3 THEN 'Fair' 
            WHEN orevd.review_score = 2 THEN 'Bad'
            ELSE 'Very Bad' END) as rating_type

FROM
    od
LEFT JOIN
    cd ON od.customer_id = cd.customer_id
LEFT JOIN
    orevd ON od.order_id = orevd.order_id
LEFT JOIN
    oid ON od.order_id = oid.order_id
LEFT JOIN
    pd ON oid.product_id = pd.product_id
WHERE
    orevd.review_score IS NOT NULL
ORDER BY
    od.order_purchase_timestamp)
    
    SELECT 
    count(review_score) as total_review,    
    rating_type,
    (round(count(review_score) * 1.0 / (SELECT COUNT(review_score) FROM reviews),2)) AS percentage
    FROM reviews
    GROUP BY 2
    ORDER BY 1 DESC;
    
    
    
    
    -- Yearly Financial Overview: Total Orders and Gross Margin by Year 
    WITH t2 AS (
    SELECT
        cd.customer_id,
        cd.customer_unique_id,
        pd.product_category_name,
        od.order_id,
        pd.product_id,
        oid.price,
        EXTRACT(YEAR FROM od.order_purchase_timestamp) AS yr,
        cd.customer_state
    FROM cd   
    INNER JOIN od ON od.customer_id = cd.customer_id
    INNER JOIN oid ON oid.order_id = od.order_id
    INNER JOIN pd ON pd.product_id = oid.product_id
    ORDER BY od.order_purchase_timestamp
)
				-- Analysis of Customer Orders and Revenue Trends by State 
					WITH t3 AS (
							SELECT
								cd.customer_id,
								cd.customer_unique_id,
								pd.product_category_name,
								od.order_id,
								pd.product_id,
								oid.price,
								EXTRACT(YEAR FROM od.order_purchase_timestamp) AS yr,
								cd.customer_state
							FROM cd   
							INNER JOIN od ON od.customer_id = cd.customer_id
                            INNER JOIN oid ON oid.order_id = od.order_id
							INNER JOIN pd ON pd.product_id = oid.product_id
    
)

			SELECT
				yr,
				customer_state,
				COUNT(customer_unique_id) AS total_orders,
				SUM(price) AS gross_margin,
				ROUND(AVG(price), 2) AS avg_gross_margin,
				ROUND(SUM(price) / NULLIF(COUNT(customer_unique_id), 0), 2) AS avg_order_value
			FROM t3
            GROUP BY yr, customer_state
            ORDER BY yr ASC, customer_state;
            
-- Ranked Product Categories by Sales with Summary of Remaining Categories
		
        WITH RankedCategories AS (
    SELECT
        product_category_name_english AS category,
        SUM(oid.price) AS sales, 
        RANK() OVER (ORDER BY SUM(oid.price) DESC) AS ranks
    FROM oid
    JOIN od ON oid.order_id = od.order_id
    JOIN pd ON oid.product_id = pd.product_id
    JOIN product_category_name_translation pcat ON pcat.product_category_name = pd.product_category_name
    WHERE od.order_status = 'delivered'
    GROUP BY product_category_name_english
)

SELECT
    category,
    sales
FROM RankedCategories
WHERE ranks <= 10

UNION ALL

SELECT
    'Other categories' AS category,
    SUM(sales) AS sales
FROM RankedCategories
WHERE ranks > 10;	    



-- Yearly and Monthly Review Score Analysis with Positive Review
SELECT
    extract(month from orevd.review_creation_date) AS months,
    extract(year from orevd.review_creation_date) AS years,
    ROUND(AVG(orevd.review_score), 2) AS avg_review_score,
    COUNT(orevd.review_id) AS total_reviews,
    SUM(CASE WHEN orevd.review_score >= 4 THEN 1 ELSE 0 END) * 100.0 / COUNT(orevd.review_id) AS positive_review_perc
FROM
    orevd
GROUP BY
    2,1
ORDER by
    2,1 DESC;
    
    
    
-- Summary of Orders and Payments by Payment Type
SELECT
    opd.payment_type,
    COUNT(DISTINCT opd.order_id) AS total_orders,
    SUM(opd.payment_value) AS total_payment_value,
    ROUND(AVG(opd.payment_value), 2) AS avg_payment_value,
    round(COUNT(DISTINCT opd.order_id) * 100.0 / (SELECT COUNT(DISTINCT order_id) FROM opd),2)AS percentage_of_orders
FROM
    opd
GROUP BY
    payment_type
ORDER BY
    total_payment_value DESC;
    
-- Analyzing Seller Performance 
WITH 
reviews AS (
    SELECT 
        od.order_id, 
        orevd.review_id, 
        orevd.review_score 
    FROM 
        orevd  
    JOIN
        od ON orevd.order_id = od.order_id  
),
order_info AS (
    SELECT 
        oid.seller_id, 
        orevd.order_id, 
        orevd.review_id, 
        orevd.review_score 
    FROM 
        orevd 
    JOIN 
        oid 
        ON orevd.order_id = oid.order_id  
)

-- Analyzing Seller Revenue
WITH order_det AS (
    SELECT 
        oid.order_id,  
        oid.seller_id,  
        oid.price AS Revenue,  
        od.order_purchase_timestamp 
    FROM 
        oid 
    JOIN 
        od ON oid.order_id = od.order_id
),  
running_total_revenue AS (
    SELECT 
        seller_id, 
        order_purchase_timestamp, 
		SUM(Revenue) OVER (PARTITION BY seller_id ORDER BY order_purchase_timestamp) AS rev_run_total, 
        SUM(Revenue) OVER (PARTITION BY seller_id) AS current_rev  
    FROM 
        order_det
)  
SELECT 
    seller_id, 
    MIN(order_purchase_timestamp) AS Date_Achieved_100k,  
    max(current_rev) AS Current_Total_Revenue  
FROM 
    running_total_revenue 
WHERE 
    rev_run_total >= 100000 
GROUP BY 
    seller_id 
ORDER BY 
    Date_Achieved_100k ASC;
    


-- Top 20 Sellers: Analyzing Sales Performance and Market Share
SELECT distinct
    seller_id, 
    SUM(price) OVER (PARTITION BY seller_id) AS sales, 
    COUNT(order_id) OVER (PARTITION BY seller_id) AS orders_fulfilled, 
    100 * (SUM(price) OVER (PARTITION BY seller_id) / (SELECT SUM(price) FROM oid)) AS market_share 
FROM 
    oid 
WHERE 
    seller_id IS NOT NULL 
ORDER BY 
    sales DESC 
LIMIT 10;



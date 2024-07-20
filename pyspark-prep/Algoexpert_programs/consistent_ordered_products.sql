WITH consecutive_months AS (
  SELECT 
    customerid,
    productid,
    DATE_TRUNC('month', orderdate) AS order_month,
    LEAD(DATE_TRUNC('month', orderdate), 5) OVER (
      PARTITION BY customerid, productid 
      ORDER BY DATE_TRUNC('month', orderdate)
    ) AS sixth_month
  FROM orders
  GROUP BY customerid, productid, DATE_TRUNC('month', orderdate)
)
SELECT DISTINCT 
  cm.customerid,
  cm.productid
FROM consecutive_months cm
WHERE cm.sixth_month IS NOT NULL
  AND cm.sixth_month = DATE_ADD('month', 5, cm.order_month)
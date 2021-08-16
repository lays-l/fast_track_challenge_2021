INSERT INTO analytic.fact_merchant_kpi 
	SELECT STR_TO_DATE(CONCAT(day, ',', month, ',', year), '%d,%m,%Y') AS 'date', 
	merchant AS 'id_merchant', 
	ROUND(SUM(amount),2) AS 'tpv', 
    COUNT(id) AS 'total_transactions' 
    FROM db.transactions 
	GROUP BY CONCAT(day, '/', month, '/', year), merchant;
    
INSERT INTO analytic.fact_customer_kpi 
	SELECT STR_TO_DATE(CONCAT(day, ',', month, ',', year), '%d,%m,%Y') AS 'date', 
	customer AS 'id_customer', 
	ROUND(SUM(amount),2) AS 'tpv', 
    COUNT(id) AS 'total_transactions' 
    FROM db.transactions  
	GROUP BY CONCAT(day, '/', month, '/', year), customer;
    
INSERT INTO analytic.dim_merchant_category SELECT merchant AS 'id_merchant', category FROM db.transactions  GROUP BY merchant;  
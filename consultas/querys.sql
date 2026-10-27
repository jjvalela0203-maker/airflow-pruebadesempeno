-- Pregunta 1: Evolución mensual de las ventas netas
SELECT 
    DATE_TRUNC('month', invoicedate) AS mes,
    SUM(netrevenue) AS ventas_netas_mensuales
FROM ventas
GROUP BY DATE_TRUNC('month', invoicedate)
ORDER BY mes;

-- Pregunta 2: Categorías con más revenue bruto y mayor proporción de devoluciones
-- Para el revenue bruto por categoría
SELECT 
    category, 
    SUM(bruterevenue) as total_revenue_bruto
FROM ventas
GROUP BY category
ORDER BY total_revenue_bruto DESC;

--Mayor proporcion de devoluciones por categoria de productos
SELECT 
    v.category,
    COUNT(d.id) AS cantidad_devoluciones
FROM ventas v
JOIN devoluciones d ON v.stockcode = d.stockcode AND DATE(v.invoicedate) = DATE(d.invoicedate)
GROUP BY v.category
ORDER BY cantidad_devoluciones DESC;

-- PREGUNTA 3: Top 10 países con mayor volumen de ventas netas
SELECT 
    country, 
    SUM(netrevenue) AS total_revenue_neto
FROM ventas
GROUP BY country
ORDER BY total_revenue_neto DESC
LIMIT 10;


-- PREGUNTA 4: Los 10 productos estrella (más vendidos por cantidad)
SELECT 
    stockcode, 
    description, 
    SUM(quantity) AS total_unidades_vendidas
FROM ventas
GROUP BY stockcode, description
ORDER BY total_unidades_vendidas DESC
LIMIT 10;


-- PREGUNTA 5: Top 10 mejores clientes (Mayor revenue generado)
SELECT 
    customerid, 
    SUM(netrevenue) AS valor_total_compras
FROM ventas
WHERE customerid IS NOT NULL
GROUP BY customerid
ORDER BY valor_total_compras DESC
LIMIT 10;



-- PREGUNTA 6: Ticket promedio mensual (Average Order Value)
SELECT 
    DATE_TRUNC('month', invoicedate) AS mes,
    SUM(netrevenue) / COUNT(DISTINCT invoiceno) AS ticket_promedio
FROM ventas
GROUP BY DATE_TRUNC('month', invoicedate)
ORDER BY mes;

-- PREGUNTA 7: Análisis de calidad de datos (Motivos de rechazo más frecuentes)
SELECT 
    motivo, 
    COUNT(*) AS cantidad_registros_rechazados
FROM log_rechazos
GROUP BY motivo
ORDER BY cantidad_registros_rechazados DESC;
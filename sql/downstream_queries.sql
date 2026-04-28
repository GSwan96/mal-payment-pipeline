-- 1. Daily payment volume by payment type
SELECT
    processing_date,
    payment_type,
    COUNT(*) AS txn_count,
    SUM(amount) AS total_amount
FROM unified_payments
GROUP BY processing_date, payment_type
ORDER BY processing_date, payment_type;

-- 2. Failed or pending payments
SELECT
    event_id,
    payment_type,
    customer_id,
    amount,
    currency,
    status
FROM unified_payments
WHERE status IN ('failed', 'pending', 'declined');

-- 3. Customer payment summary
SELECT
    customer_id,
    COUNT(*) AS payment_count,
    SUM(amount) AS total_amount
FROM unified_payments
GROUP BY customer_id
ORDER BY total_amount DESC;
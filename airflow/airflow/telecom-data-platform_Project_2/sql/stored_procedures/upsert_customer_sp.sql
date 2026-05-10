CREATE PROCEDURE sp_upsert_telecom_customer(
    IN p_customer_id INT,
    IN p_full_name VARCHAR(256),
    IN p_service_tier VARCHAR(50),
    IN p_region_code CHAR(1),
    IN p_score INT
)
BEGIN
    INSERT INTO telecom_customer_warehouse (customer_id, full_name, service_tier, region_code, score)
    VALUES (p_customer_id, p_full_name, p_service_tier, p_region_code, p_score)
    ON DUPLICATE KEY UPDATE
        full_name = VALUES(full_name),
        service_tier = VALUES(service_tier),
        region_code = VALUES(region_code),
        score = VALUES(score);
END;

INSERT INTO custody_position (account_id, asset_id, reference_date, quantity, amount)
VALUES
    ('ACC001', 'PETR4', '2025-01-15', 1000.0000, 25000.00),
    ('ACC001', 'VALE3', '2025-01-15', 500.0000, 15000.00),
    ('ACC002', 'ITUB4', '2025-01-15', 2000.0000, 70000.00)
ON CONFLICT (account_id, asset_id, reference_date) DO NOTHING;

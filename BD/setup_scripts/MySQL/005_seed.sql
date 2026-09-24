INSERT INTO machines
(
    id,
    machine_code,
    name,
    machine_type_id,
    manufacturer,
    model,
    timezone,
    created_at,
    updated_at
)
VALUES
    (1, 'TPA-01', 'TPA-01', 1, 'KEBA', NULL, 'Europe/Moscow', NOW(3), NOW(3)),
    (2, 'TPA-02', 'TPA-02', 1, 'KEBA', NULL, 'Europe/Moscow', NOW(3), NOW(3)),
    (3, 'TPA-03', 'TPA-03', 1, 'KEBA', NULL, 'Europe/Moscow', NOW(3), NOW(3)),
    (4, 'TPA-04', 'TPA-04', 1, 'KEBA', NULL, 'Europe/Moscow', NOW(3), NOW(3));
CREATE TABLE filter_criteria (
                                 id SERIAL PRIMARY KEY,
                                 filter_id INT NOT NULL REFERENCES filters(id) ON DELETE CASCADE,
                                 key VARCHAR(255) NOT NULL,            -- OID or source_ip
                                 operator VARCHAR(50) NOT NULL,        -- eq, gt, lt, like, regexp, etc.
                                 value VARCHAR(255) NOT NULL           -- The value to compare against
);

INSERT INTO filter_criteria (filter_id, key, operator, value)
VALUES
    (1, 'OID 1.2', 'eq', 'blalb'),
    (1, 'OID 1.3', 'like', '%blob%'),
    (1, 'Address', 'regexp', '^1\\.2\\.3\\.[0-9]+$'),
    (2, 'Address', 'eq', '172.29.4.2'),
    (2, 'Other', 'eq', '42');

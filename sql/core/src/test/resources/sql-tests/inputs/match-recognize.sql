-- Integration tests for MATCH_RECOGNIZE
-- These tests use real tables to trigger shuffle and sort operations

-- Create a table with multiple partitions to test partition isolation
CREATE TABLE stock_data (symbol STRING, ts BIGINT, price DECIMAL(10,2))
USING parquet;

INSERT INTO stock_data VALUES
  ('AAPL', 1, 100.00),
  ('AAPL', 2, 90.00),
  ('AAPL', 3, 110.00),
  ('GOOG', 1, 200.00),
  ('GOOG', 2, 180.00),
  ('GOOG', 3, 220.00),
  ('MSFT', 1, 50.00),
  ('MSFT', 2, 60.00),
  ('MSFT', 3, 55.00);

-- Tests qualified vs unqualified column references
-- LAST(A.ts) returns last ts from A rows only, LAST(ts) returns last ts from all matched rows
SELECT * FROM stock_data
MATCH_RECOGNIZE (
  PARTITION BY symbol
  ORDER BY ts
  MEASURES
    symbol AS sym,
    LAST(A.ts) AS last_a_ts,
    LAST(ts) AS last_ts
  PATTERN (A B)
  DEFINE
    A AS ts = 1,
    B AS ts = 2
);

-- Test without PARTITION BY - all rows as one partition, single match instead of 3
SELECT * FROM stock_data
MATCH_RECOGNIZE (
  ORDER BY ts
  MEASURES
    FIRST(ts) AS start_ts,
    LAST(ts) AS end_ts
  PATTERN (A B)
  DEFINE
    A AS ts = 1,
    B AS ts = 2
);

-- cleanup
DROP TABLE stock_data;

-- Test PARTITION BY alias shadows existing column
-- 'price' is an existing column, but PARTITION BY ts + 1 AS price should shadow it
CREATE TABLE test_shadow (ts BIGINT, price BIGINT) USING parquet;
INSERT INTO test_shadow VALUES (1, 100), (2, 100), (1, 200), (2, 200);

SELECT * FROM test_shadow
MATCH_RECOGNIZE (
  PARTITION BY ts + 1 AS price
  ORDER BY ts
  MEASURES
    price AS partition_val
  PATTERN (A)
  DEFINE
    A AS ts > 0
);

DROP TABLE test_shadow;

-- Test quantifier: A+ (one or more)
CREATE TABLE quant_test (ts BIGINT, price BIGINT) USING parquet;
INSERT INTO quant_test VALUES (1, 100), (2, 110), (3, 120), (4, 50);

SELECT * FROM quant_test
MATCH_RECOGNIZE (
  ORDER BY ts
  MEASURES
    FIRST(ts) AS start_ts,
    LAST(ts) AS end_ts
  PATTERN (A+)
  DEFINE
    A AS price > 80
);

-- Test quantifier followed by variable: A+ B
SELECT * FROM quant_test
MATCH_RECOGNIZE (
  ORDER BY ts
  MEASURES
    FIRST(ts) AS start_ts,
    LAST(ts) AS end_ts
  PATTERN (A+ B)
  DEFINE
    A AS price > 80,
    B AS price <= 80
);

DROP TABLE quant_test;

-- Test grouped pattern: (A B)+
-- Expected: rows 1-4 match (A=50, B=150), (A=60, B=200)
CREATE TABLE group_test (ts BIGINT, price BIGINT) USING parquet;
INSERT INTO group_test VALUES (1, 50), (2, 150), (3, 60), (4, 200), (5, 10);

SELECT * FROM group_test
MATCH_RECOGNIZE (
  ORDER BY ts
  MEASURES
    FIRST(ts) AS start_ts,
    LAST(ts) AS end_ts
  PATTERN ((A B)+)
  DEFINE
    A AS price < 100,
    B AS price >= 100
);

-- Test grouped pattern followed by variable: (A B)+ C
-- Expected: rows 1-5 match (A=50, B=150), (A=60, B=200), C=10
SELECT * FROM group_test
MATCH_RECOGNIZE (
  ORDER BY ts
  MEASURES
    FIRST(ts) AS start_ts,
    LAST(ts) AS end_ts
  PATTERN ((A B)+ C)
  DEFINE
    A AS price < 100,
    B AS price >= 100,
    C AS price < 50
);

DROP TABLE group_test;

-- Error cases

-- error: unsupported alternation
CREATE TABLE error_test (ts BIGINT, price DECIMAL(10,2)) USING parquet;
INSERT INTO error_test VALUES (1, 100.00);

SELECT * FROM error_test
MATCH_RECOGNIZE (
  ORDER BY ts
  MEASURES LAST(ts) AS end_ts
  PATTERN (A | B)
  DEFINE
    A AS price > 50,
    B AS price > 100
);

DROP TABLE error_test;

-- error: duplicate pattern variable names
CREATE TABLE error_test (ts BIGINT, price DECIMAL(10,2)) USING parquet;
INSERT INTO error_test VALUES (1, 100.00);

SELECT * FROM error_test
MATCH_RECOGNIZE (
  ORDER BY ts
  MEASURES LAST(ts) AS end_ts
  PATTERN (A B)
  DEFINE
    A AS price > 50,
    A AS price > 100
);

DROP TABLE error_test;

-- error: undefined pattern variable
CREATE TABLE error_test (ts BIGINT, price DECIMAL(10,2)) USING parquet;
INSERT INTO error_test VALUES (1, 100.00);

SELECT * FROM error_test
MATCH_RECOGNIZE (
  ORDER BY ts
  MEASURES LAST(ts) AS end_ts
  PATTERN (A B)
  DEFINE
    A AS price > 50
);

DROP TABLE error_test;

-- error: mixed qualifiers in aggregate
CREATE TABLE error_test (ts BIGINT, price DECIMAL(10,2)) USING parquet;
INSERT INTO error_test VALUES (1, 100.00);

SELECT * FROM error_test
MATCH_RECOGNIZE (
  ORDER BY ts
  MEASURES LAST(A.price - B.price) AS price_diff
  PATTERN (A B)
  DEFINE
    A AS price > 50,
    B AS price > 100
);

DROP TABLE error_test;

-- error: non-deterministic expression in DEFINE
CREATE TABLE error_test (ts BIGINT, price DECIMAL(10,2)) USING parquet;
INSERT INTO error_test VALUES (1, 100.00);

SELECT * FROM error_test
MATCH_RECOGNIZE (
  ORDER BY ts
  MEASURES LAST(ts) AS end_ts
  PATTERN (A B)
  DEFINE
    A AS price > RAND(),
    B AS price > 100
);

DROP TABLE error_test;

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
    LAST(A.ts) AS last_a_ts,
    LAST(ts) AS last_ts
  PATTERN (A B)
  DEFINE
    A AS ts = 1,
    B AS ts = 2
);

-- Test bare unqualified column references in ONE ROW PER MATCH
-- Per Oracle/SQL standard, bare columns are equivalent to LAST(col).
-- price is a non-partition attribute that varies across matched rows;
-- bare `price` returns the last row's value (same as LAST(price)).
SELECT * FROM stock_data
MATCH_RECOGNIZE (
  PARTITION BY symbol
  ORDER BY ts
  MEASURES
    price AS bare_price,
    LAST(price) AS last_price
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

-- Test bounded quantifier: A{2,4} B
-- With 3 consecutive rising rows followed by a drop, A{2,4} greedily matches 3, then B matches
CREATE TABLE bounded_test (ts BIGINT, price BIGINT) USING parquet;
INSERT INTO bounded_test VALUES (1, 100), (2, 110), (3, 120), (4, 50);

SELECT * FROM bounded_test
MATCH_RECOGNIZE (
  ORDER BY ts
  MEASURES
    FIRST(ts) AS start_ts,
    LAST(ts) AS end_ts
  PATTERN (A{2,4} B)
  DEFINE
    A AS price > 80,
    B AS price <= 80
);

-- Test exact quantifier: A{3}
-- Only matches if there are exactly 3 consecutive rows with price > 80
SELECT * FROM bounded_test
MATCH_RECOGNIZE (
  ORDER BY ts
  MEASURES
    FIRST(ts) AS start_ts,
    LAST(ts) AS end_ts
  PATTERN (A{3})
  DEFINE
    A AS price > 80
);

-- Test at-least quantifier: A{2,}
-- Greedily matches all consecutive rows with price > 80
SELECT * FROM bounded_test
MATCH_RECOGNIZE (
  ORDER BY ts
  MEASURES
    FIRST(ts) AS start_ts,
    LAST(ts) AS end_ts
  PATTERN (A{2,})
  DEFINE
    A AS price > 80
);

-- Test at-most quantifier: A{,3} B
-- Matches up to 3 consecutive rows with price > 80, then B
SELECT * FROM bounded_test
MATCH_RECOGNIZE (
  ORDER BY ts
  MEASURES
    FIRST(ts) AS start_ts,
    LAST(ts) AS end_ts
  PATTERN (A{,3} B)
  DEFINE
    A AS price > 80,
    B AS price <= 80
);

DROP TABLE bounded_test;

-- Test alternation pattern: (A | B) C
-- Partition 'p1': price=100 matches A (>80), then price=60 matches C (<=80)
-- Partition 'p2': price=70 matches B (>50 but not >80), then price=30 matches C (<=80)
CREATE TABLE alternation_test (grp STRING, ts BIGINT, price BIGINT) USING parquet;
INSERT INTO alternation_test VALUES ('p1', 1, 100), ('p1', 2, 60), ('p2', 1, 70), ('p2', 2, 30);

SELECT * FROM alternation_test
MATCH_RECOGNIZE (
  PARTITION BY grp
  ORDER BY ts
  MEASURES
    FIRST(ts) AS start_ts,
    LAST(ts) AS end_ts
  PATTERN ((A | B) C)
  DEFINE
    A AS price > 80,
    B AS price > 50,
    C AS price <= 80
);

DROP TABLE alternation_test;

-- Test PERMUTE pattern: PERMUTE(A, B)
-- Partition 'p1': price 50 then 100 -> matches B A ordering
-- Partition 'p2': price 100 then 50 -> matches A B ordering
CREATE TABLE permute_test (grp STRING, ts BIGINT, price BIGINT) USING parquet;
INSERT INTO permute_test VALUES ('p1', 1, 50), ('p1', 2, 100), ('p2', 1, 100), ('p2', 2, 50);

SELECT * FROM permute_test
MATCH_RECOGNIZE (
  PARTITION BY grp
  ORDER BY ts
  MEASURES
    FIRST(ts) AS start_ts,
    LAST(ts) AS end_ts
  PATTERN (PERMUTE(A, B))
  DEFINE
    A AS price >= 100,
    B AS price < 100
);

DROP TABLE permute_test;

-- Test multiple matches within a partition
-- Partition AA has a gap (val=-1) that splits it into two separate X+ matches
-- Expected: AA produces 2 rows (first match: 10,10 and second match: 20,30)
CREATE TABLE match_id_test (sym STRING, ts BIGINT, val BIGINT) USING parquet;
INSERT INTO match_id_test VALUES
  ('AA', 1, 10), ('AA', 2, -1), ('AA', 3, 20), ('AA', 4, 30),
  ('BB', 1, 1);

SELECT * FROM match_id_test
MATCH_RECOGNIZE (
  PARTITION BY sym
  ORDER BY ts
  MEASURES
    FIRST(val) AS first_val,
    LAST(val) AS last_val
  PATTERN (X+)
  DEFINE
    X AS val > 0
);

DROP TABLE match_id_test;

-- ALL ROWS PER MATCH tests
CREATE TABLE all_rows_test (sym STRING, ts BIGINT, price INT)
USING parquet;

INSERT INTO all_rows_test VALUES
  ('X', 1, 10),
  ('X', 2, 20),
  ('X', 3, 30);

-- ALL ROWS PER MATCH: output every matched row with running measures
SELECT * FROM all_rows_test
MATCH_RECOGNIZE (
  PARTITION BY sym
  ORDER BY ts
  MEASURES
    FIRST(price) AS first_price,
    LAST(price) AS last_price
  ALL ROWS PER MATCH
  PATTERN (A B C)
  DEFINE
    A AS ts = 1,
    B AS ts = 2,
    C AS ts = 3
);

-- Compare ONE ROW PER MATCH vs ALL ROWS PER MATCH on the same data
SELECT * FROM all_rows_test
MATCH_RECOGNIZE (
  PARTITION BY sym
  ORDER BY ts
  MEASURES
    FIRST(price) AS first_price,
    LAST(price) AS last_price
  ONE ROW PER MATCH
  PATTERN (A B C)
  DEFINE
    A AS ts = 1,
    B AS ts = 2,
    C AS ts = 3
);

-- ALL ROWS PER MATCH with qualified measure (LAST(A.price))
SELECT * FROM all_rows_test
MATCH_RECOGNIZE (
  PARTITION BY sym
  ORDER BY ts
  MEASURES
    LAST(A.price) AS a_price
  ALL ROWS PER MATCH
  PATTERN (A B)
  DEFINE
    A AS ts = 1,
    B AS ts = 2
);

DROP TABLE all_rows_test;

-- Error cases

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

-- error: qualified column outside aggregate in ALL ROWS PER MATCH
CREATE TABLE error_test (ts BIGINT, price DECIMAL(10,2)) USING parquet;
INSERT INTO error_test VALUES (1, 100.00);

SELECT * FROM error_test
MATCH_RECOGNIZE (
  ORDER BY ts
  MEASURES A.price AS a_price
  ALL ROWS PER MATCH
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

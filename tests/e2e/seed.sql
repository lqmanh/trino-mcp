-- Raw tpch has no partitioning, so it would exercise `profile_table` least
-- where it matters most. Materialising it into Iceberg gives hidden
-- partitioning, live $snapshots, and both statistics states in one cluster.
--
-- Every statement is idempotent: the fixture reseeds without caring whether a
-- previous run got this far.

CREATE SCHEMA IF NOT EXISTS iceberg.sales;

-- Partitioned by a transform, so `partition_columns` has to report `orderdate`
-- rather than the transform. ~15k rows over 1992-1998, well past the row cap.
CREATE TABLE IF NOT EXISTS iceberg.sales.orders
COMMENT 'One row per order, from tpch'
WITH (partitioning = ARRAY['month(orderdate)'])
AS SELECT * FROM tpch.tiny.orders;

-- Unpartitioned, and analyzed below. Iceberg may report statistics from its
-- manifests whether or not anyone ran ANALYZE, so this may not be the only table
-- carrying them — but it is the one always ensuring that. The genuine "no statistics"
-- case is the relational table.
CREATE TABLE IF NOT EXISTS iceberg.sales.customer
AS SELECT * FROM tpch.tiny.customer;

ANALYZE iceberg.sales.customer;

-- No location on the schema: the file metastore derives table paths from
-- `hive.metastore.catalog.dir`, and an explicit one is refused at write time.
-- The partition column has to come last for a Delta CTAS.
CREATE SCHEMA IF NOT EXISTS delta.sales;

CREATE TABLE IF NOT EXISTS delta.sales.shipments
COMMENT 'One row per shipment, from tpch'
WITH (partitioned_by = ARRAY['linestatus'])
AS SELECT orderkey, partkey, suppkey, shipdate, extendedprice, linestatus
   FROM tpch.tiny.lineitem;

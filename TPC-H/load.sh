#!/usr/bin/env bash

# Usage: ./load.sh <database> <sf>

MYSQL_CLIENT_CONNECT="mysql -uroot --host 127.0.0.1 --port 3307 default"
options="storage_format = 'parquet' compression = 'lz4'"

database="$1"
sf="$2"

for t in customer lineitem nation orders partsupp part region supplier; do
    echo "DROP TABLE IF EXISTS $database.$t" | $MYSQL_CLIENT_CONNECT
done

# Create tpch tables.
echo "CREATE TABLE IF NOT EXISTS $database.nation
(
    n_nationkey  INTEGER not null,
    n_name       STRING not null,
    n_regionkey  INTEGER not null,
    n_comment    STRING
) ${options}" | $MYSQL_CLIENT_CONNECT

echo "CREATE TABLE IF NOT EXISTS $database.region
(
    r_regionkey  INTEGER not null,
    r_name       STRING not null,
    r_comment    STRING
) ${options}" | $MYSQL_CLIENT_CONNECT

echo "CREATE TABLE IF NOT EXISTS $database.part
(
    p_partkey     BIGINT not null,
    p_name        STRING not null,
    p_mfgr        STRING not null,
    p_brand       STRING not null,
    p_type        STRING not null,
    p_size        INTEGER not null,
    p_container   STRING not null,
    p_retailprice DECIMAL(15, 2) not null,
    p_comment     STRING not null
) ${options}" | $MYSQL_CLIENT_CONNECT

echo "CREATE TABLE IF NOT EXISTS $database.supplier
(
    s_suppkey     BIGINT not null,
    s_name        STRING not null,
    s_address     STRING not null,
    s_nationkey   INTEGER not null,
    s_phone       STRING not null,
    s_acctbal     DECIMAL(15, 2) not null,
    s_comment     STRING not null
) ${options}" | $MYSQL_CLIENT_CONNECT

echo "CREATE TABLE IF NOT EXISTS $database.partsupp
(
    ps_partkey     BIGINT not null,
    ps_suppkey     BIGINT not null,
    ps_availqty    BIGINT not null,
    ps_supplycost  DECIMAL(15, 2)  not null,
    ps_comment     STRING not null
) ${options}" | $MYSQL_CLIENT_CONNECT

echo "CREATE TABLE IF NOT EXISTS $database.customer
(
    c_custkey     BIGINT not null,
    c_name        STRING not null,
    c_address     STRING not null,
    c_nationkey   INTEGER not null,
    c_phone       STRING not null,
    c_acctbal     DECIMAL(15, 2)   not null,
    c_mktsegment  STRING not null,
    c_comment     STRING not null
) ${options}" | $MYSQL_CLIENT_CONNECT

echo "CREATE TABLE IF NOT EXISTS $database.orders
(
    o_orderkey       BIGINT not null,
    o_custkey        BIGINT not null,
    o_orderstatus    STRING not null,
    o_totalprice     DECIMAL(15, 2) not null,
    o_orderdate      DATE not null,
    o_orderpriority  STRING not null,
    o_clerk          STRING not null,
    o_shippriority   INTEGER not null,
    o_comment        STRING not null
) ${options}" | $MYSQL_CLIENT_CONNECT

echo "CREATE TABLE IF NOT EXISTS $database.lineitem
(
    l_orderkey    BIGINT not null,
    l_partkey     BIGINT not null,
    l_suppkey     BIGINT not null,
    l_linenumber  BIGINT not null,
    l_quantity    DECIMAL(15, 2) not null,
    l_extendedprice  DECIMAL(15, 2) not null,
    l_discount    DECIMAL(15, 2) not null,
    l_tax         DECIMAL(15, 2) not null,
    l_returnflag  STRING not null,
    l_linestatus  STRING not null,
    l_shipdate    DATE not null,
    l_commitdate  DATE not null,
    l_receiptdate DATE not null,
    l_shipinstruct STRING not null,
    l_shipmode     STRING not null,
    l_comment      STRING not null
) ${options}" | $MYSQL_CLIENT_CONNECT

# Copy data to tables.
for t in customer lineitem nation orders partsupp part region supplier
do
    pwd=`pwd`
    echo "COPY INTO $database.$t FROM 'fs://${pwd}/data/sf_${sf}/${t}/' file_format = (type = Parquet) pattern = '.*.parquet'"
    echo "COPY INTO $database.$t FROM 'fs://${pwd}/data/sf_${sf}/${t}/' file_format = (type = Parquet) pattern = '.*.parquet'" | $MYSQL_CLIENT_CONNECT
done

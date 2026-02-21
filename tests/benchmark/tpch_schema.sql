CREATE TABLE region (
    r_regionkey  INT8 NOT NULL,
    r_name       TEXT NOT NULL,
    r_comment    TEXT
);
CREATE TABLE nation (
    n_nationkey  INT8 NOT NULL,
    n_name       TEXT NOT NULL,
    n_regionkey  INT8 NOT NULL,
    n_comment    TEXT
);
CREATE TABLE part (
    p_partkey      INT8 NOT NULL,
    p_name         TEXT NOT NULL,
    p_mfgr         TEXT NOT NULL,
    p_brand        TEXT NOT NULL,
    p_type         TEXT NOT NULL,
    p_size         INT8 NOT NULL,
    p_container    TEXT NOT NULL,
    p_retailprice  NUMERIC NOT NULL,
    p_comment      TEXT
);
CREATE TABLE supplier (
    s_suppkey    INT8 NOT NULL,
    s_name       TEXT NOT NULL,
    s_address    TEXT NOT NULL,
    s_nationkey  INT8 NOT NULL,
    s_phone      TEXT NOT NULL,
    s_acctbal    NUMERIC NOT NULL,
    s_comment    TEXT
);
CREATE TABLE partsupp (
    ps_partkey     INT8 NOT NULL,
    ps_suppkey     INT8 NOT NULL,
    ps_availqty    INT8 NOT NULL,
    ps_supplycost  NUMERIC NOT NULL,
    ps_comment     TEXT
);
CREATE TABLE customer (
    c_custkey     INT8 NOT NULL,
    c_name        TEXT NOT NULL,
    c_address     TEXT NOT NULL,
    c_nationkey   INT8 NOT NULL,
    c_phone       TEXT NOT NULL,
    c_acctbal     NUMERIC NOT NULL,
    c_mktsegment  TEXT NOT NULL,
    c_comment     TEXT
);
CREATE TABLE orders (
    o_orderkey      INT8 NOT NULL,
    o_custkey       INT8 NOT NULL,
    o_orderstatus   TEXT NOT NULL,
    o_totalprice    NUMERIC NOT NULL,
    o_orderdate     DATE NOT NULL,
    o_orderpriority TEXT NOT NULL,
    o_clerk         TEXT NOT NULL,
    o_shippriority  INT8 NOT NULL,
    o_comment       TEXT
);
CREATE TABLE lineitem (
    l_orderkey      INT8 NOT NULL,
    l_partkey       INT8 NOT NULL,
    l_suppkey       INT8 NOT NULL,
    l_linenumber    INT8 NOT NULL,
    l_quantity      NUMERIC NOT NULL,
    l_extendedprice NUMERIC NOT NULL,
    l_discount      NUMERIC NOT NULL,
    l_tax           NUMERIC NOT NULL,
    l_returnflag    TEXT NOT NULL,
    l_linestatus    TEXT NOT NULL,
    l_shipdate      DATE NOT NULL,
    l_commitdate    DATE NOT NULL,
    l_receiptdate   DATE NOT NULL,
    l_shipinstruct  TEXT NOT NULL,
    l_shipmode      TEXT NOT NULL,
    l_comment       TEXT
);

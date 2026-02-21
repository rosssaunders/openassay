/// TPC-H query correctness tests.
///
/// All 22 TPC-H queries run inside a single test function with a shared
/// session.  Queries that fail due to engine limitations are tracked and
/// reported — the test passes as long as the expected minimum number of
/// queries succeed.
use std::time::Instant;

use openassay::tcop::postgres::{BackendMessage, FrontendMessage, PostgresSession};

fn has_error(out: &[BackendMessage]) -> bool {
    out.iter()
        .any(|msg| matches!(msg, BackendMessage::ErrorResponse { .. }))
}

fn error_message(out: &[BackendMessage]) -> String {
    out.iter()
        .filter_map(|msg| {
            if let BackendMessage::ErrorResponse { message, .. } = msg {
                Some(message.clone())
            } else {
                None
            }
        })
        .collect::<Vec<_>>()
        .join("; ")
}

fn count_data_rows(out: &[BackendMessage]) -> usize {
    out.iter()
        .filter(|m| matches!(m, BackendMessage::DataRow { .. }))
        .count()
}

fn exec(session: &mut PostgresSession, sql: &str) {
    let out = session.run_sync([FrontendMessage::Query {
        sql: sql.to_string(),
    }]);
    assert!(
        !has_error(&out),
        "setup query failed: {}",
        error_message(&out)
    );
}

fn try_query(session: &mut PostgresSession, label: &str, sql: &str) -> bool {
    let start = Instant::now();
    let out = session.run_sync([FrontendMessage::Query {
        sql: sql.to_string(),
    }]);
    let elapsed = start.elapsed();
    let success = !has_error(&out);
    let rows = count_data_rows(&out);
    if success {
        eprintln!("  {label}: OK ({rows} rows, {elapsed:.1?})");
    } else {
        let err = error_message(&out);
        eprintln!("  {label}: FAIL — {err} ({elapsed:.1?})");
    }
    success
}

#[test]
fn tpch_query_suite() {
    let mut session = PostgresSession::new();
    exec(&mut session, include_str!("tpch_schema.sql"));
    exec(&mut session, include_str!("tpch_data.sql"));

    // Queries are (label, skip_reason_or_empty, sql).
    // Queries with a non-empty skip reason are logged but not executed
    // (they cause timeouts due to cartesian products without a query optimizer).
    let queries: &[(&str, &str, &str)] = &[
        // Q01: Pricing summary report (native INTERVAL arithmetic)
        ("Q01", "", r#"SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty,
            SUM(l_extendedprice) AS sum_base_price,
            SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
            SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
            AVG(l_quantity) AS avg_qty, AVG(l_extendedprice) AS avg_price,
            AVG(l_discount) AS avg_disc, COUNT(*) AS count_order
        FROM lineitem WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90 days'
        GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus"#),

        // Q02: Correlated subquery with 5-way cross join (now with predicate pushdown)
        ("Q02", "", r#"SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment
        FROM part, supplier, partsupp, nation, region
        WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND p_size = 15 AND p_type LIKE '%BRASS'
            AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'EUROPE'
            AND ps_supplycost = (SELECT MIN(ps_supplycost) FROM partsupp, supplier, nation, region
                WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey
                    AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'EUROPE')
        ORDER BY s_acctbal DESC, n_name, s_name, p_partkey LIMIT 100"#),

        // Q03: 3-way join
        ("Q03", "", r#"SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) AS revenue,
            o_orderdate, o_shippriority
        FROM customer, orders, lineitem
        WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey
            AND l_orderkey = o_orderkey AND o_orderdate < DATE '1995-03-15'
            AND l_shipdate > DATE '1995-03-15'
        GROUP BY l_orderkey, o_orderdate, o_shippriority
        ORDER BY revenue DESC, o_orderdate LIMIT 10"#),

        // Q04: Order priority checking (native INTERVAL arithmetic)
        ("Q04", "", r#"SELECT o_orderpriority, COUNT(*) AS order_count FROM orders
        WHERE o_orderdate >= DATE '1993-07-01'
            AND o_orderdate < DATE '1993-07-01' + INTERVAL '3 months'
            AND EXISTS (SELECT * FROM lineitem WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate)
        GROUP BY o_orderpriority ORDER BY o_orderpriority"#),

        // Q05: 6-way join with native INTERVAL (now with predicate pushdown)
        ("Q05", "", r#"SELECT n_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue
        FROM customer, orders, lineitem, supplier, nation, region
        WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey
            AND c_nationkey = s_nationkey AND s_nationkey = n_nationkey
            AND n_regionkey = r_regionkey AND r_name = 'ASIA'
            AND o_orderdate >= DATE '1994-01-01' AND o_orderdate < DATE '1994-01-01' + INTERVAL '1 year'
        GROUP BY n_name ORDER BY revenue DESC"#),

        // Q06: Forecasting revenue change (native INTERVAL arithmetic)
        ("Q06", "", r#"SELECT SUM(l_extendedprice * l_discount) AS revenue FROM lineitem
        WHERE l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1994-01-01' + INTERVAL '1 year'
            AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24"#),

        // Q07: 6-way join with self-join on nation (now with predicate pushdown)
        ("Q07", "", r#"SELECT supp_nation, cust_nation, l_year, SUM(volume) AS revenue
        FROM (SELECT n1.n_name AS supp_nation, n2.n_name AS cust_nation,
                EXTRACT(YEAR FROM l_shipdate) AS l_year, l_extendedprice * (1 - l_discount) AS volume
            FROM supplier, lineitem, orders, customer, nation n1, nation n2
            WHERE s_suppkey = l_suppkey AND o_orderkey = l_orderkey AND c_custkey = o_custkey
                AND s_nationkey = n1.n_nationkey AND c_nationkey = n2.n_nationkey
                AND ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
                    OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE'))
                AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
        ) AS shipping GROUP BY supp_nation, cust_nation, l_year
        ORDER BY supp_nation, cust_nation, l_year"#),

        // Q08: 8-way join (now with predicate pushdown)
        ("Q08", "", r#"SELECT o_year, SUM(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0 END) / SUM(volume) AS mkt_share
        FROM (SELECT EXTRACT(YEAR FROM o_orderdate) AS o_year,
                l_extendedprice * (1 - l_discount) AS volume, n2.n_name AS nation
            FROM part, supplier, lineitem, orders, customer, nation n1, nation n2, region
            WHERE p_partkey = l_partkey AND s_suppkey = l_suppkey AND l_orderkey = o_orderkey
                AND o_custkey = c_custkey AND c_nationkey = n1.n_nationkey
                AND n1.n_regionkey = r_regionkey AND r_name = 'AMERICA'
                AND s_nationkey = n2.n_nationkey
                AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
                AND p_type = 'STANDARD POLISHED BRASS'
        ) AS all_nations GROUP BY o_year ORDER BY o_year"#),

        // Q09: 6-way join (now with predicate pushdown)
        ("Q09", "", r#"SELECT nation, o_year, SUM(amount) AS sum_profit
        FROM (SELECT n_name AS nation, EXTRACT(YEAR FROM o_orderdate) AS o_year,
                l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
            FROM part, supplier, lineitem, partsupp, orders, nation
            WHERE s_suppkey = l_suppkey AND ps_suppkey = l_suppkey AND ps_partkey = l_partkey
                AND p_partkey = l_partkey AND o_orderkey = l_orderkey
                AND s_nationkey = n_nationkey AND p_name LIKE '%green%'
        ) AS profit GROUP BY nation, o_year ORDER BY nation, o_year DESC"#),

        // Q10: Returned item reporting (native INTERVAL arithmetic)
        ("Q10", "", r#"SELECT c_custkey, c_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue,
            c_acctbal, n_name, c_address, c_phone, c_comment
        FROM customer, orders, lineitem, nation
        WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
            AND o_orderdate >= DATE '1993-10-01' AND o_orderdate < DATE '1993-10-01' + INTERVAL '3 months'
            AND l_returnflag = 'R' AND c_nationkey = n_nationkey
        GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
        ORDER BY revenue DESC LIMIT 20"#),

        // Q11: 3-way join with HAVING subquery
        ("Q11", "", r#"SELECT ps_partkey, SUM(ps_supplycost * ps_availqty) AS value
        FROM partsupp, supplier, nation
        WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'GERMANY'
        GROUP BY ps_partkey
        HAVING SUM(ps_supplycost * ps_availqty) > (
            SELECT SUM(ps_supplycost * ps_availqty) * 0.0001
            FROM partsupp, supplier, nation
            WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'GERMANY')
        ORDER BY value DESC"#),

        // Q12: Shipping modes (native INTERVAL arithmetic)
        ("Q12", "", r#"SELECT l_shipmode,
            SUM(CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH' THEN 1 ELSE 0 END) AS high_line_count,
            SUM(CASE WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH' THEN 1 ELSE 0 END) AS low_line_count
        FROM orders, lineitem
        WHERE o_orderkey = l_orderkey AND l_shipmode IN ('MAIL', 'SHIP')
            AND l_commitdate < l_receiptdate AND l_shipdate < l_commitdate
            AND l_receiptdate >= DATE '1994-01-01' AND l_receiptdate < DATE '1994-01-01' + INTERVAL '1 year'
        GROUP BY l_shipmode ORDER BY l_shipmode"#),

        // Q13: LEFT JOIN with nested aggregation
        ("Q13", "", r#"SELECT c_count, COUNT(*) AS custdist
        FROM (SELECT c_custkey, COUNT(o_orderkey) AS c_count
            FROM customer LEFT OUTER JOIN orders ON c_custkey = o_custkey
                AND o_comment NOT LIKE '%special%requests%'
            GROUP BY c_custkey) AS c_orders
        GROUP BY c_count ORDER BY custdist DESC, c_count DESC"#),

        // Q14: Promotion effect (native INTERVAL arithmetic)
        ("Q14", "", r#"SELECT 100.00 * SUM(CASE WHEN p_type LIKE 'PROMO%' THEN l_extendedprice * (1 - l_discount) ELSE 0 END)
            / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
        FROM lineitem, part
        WHERE l_partkey = p_partkey AND l_shipdate >= DATE '1995-09-01'
            AND l_shipdate < DATE '1995-09-01' + INTERVAL '1 month'"#),

        // Q15: Top supplier (native INTERVAL arithmetic)
        ("Q15", "", r#"WITH revenue AS (
            SELECT l_suppkey AS supplier_no, SUM(l_extendedprice * (1 - l_discount)) AS total_revenue
            FROM lineitem WHERE l_shipdate >= DATE '1996-01-01'
                AND l_shipdate < DATE '1996-01-01' + INTERVAL '3 months'
            GROUP BY l_suppkey)
        SELECT s_suppkey, s_name, s_address, s_phone, total_revenue
        FROM supplier, revenue WHERE s_suppkey = supplier_no
            AND total_revenue = (SELECT MAX(total_revenue) FROM revenue)
        ORDER BY s_suppkey"#),

        // Q16: 2-way join
        ("Q16", "", r#"SELECT p_brand, p_type, p_size, COUNT(DISTINCT ps_suppkey) AS supplier_cnt
        FROM partsupp, part
        WHERE p_partkey = ps_partkey AND p_brand <> 'Brand#45'
            AND p_type NOT LIKE 'MEDIUM POLISHED%'
            AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
        GROUP BY p_brand, p_type, p_size ORDER BY supplier_cnt DESC, p_brand, p_type, p_size"#),

        // Q17: Correlated subquery (now with predicate pushdown for outer join)
        ("Q17", "", r#"SELECT SUM(l_extendedprice) / 7.0 AS avg_yearly FROM lineitem, part
        WHERE p_partkey = l_partkey AND p_brand = 'Brand#13' AND p_container = 'JUMBO PKG'
            AND l_quantity < (SELECT 0.2 * AVG(l_quantity) FROM lineitem WHERE l_partkey = p_partkey)"#),

        // Q18: 3-way join with IN subquery
        ("Q18", "", r#"SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, SUM(l_quantity) AS total_qty
        FROM customer, orders, lineitem
        WHERE o_orderkey IN (SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING SUM(l_quantity) > 300)
            AND c_custkey = o_custkey AND o_orderkey = l_orderkey
        GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
        ORDER BY o_totalprice DESC, o_orderdate LIMIT 100"#),

        // Q19: 2-way join with complex OR predicates
        ("Q19", "", r#"SELECT SUM(l_extendedprice * (1 - l_discount)) AS revenue FROM lineitem, part
        WHERE (p_partkey = l_partkey AND p_brand = 'Brand#13'
                AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
                AND l_quantity >= 1 AND l_quantity <= 11 AND p_size BETWEEN 1 AND 5
                AND l_shipmode IN ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON')
            OR (p_partkey = l_partkey AND p_brand = 'Brand#34'
                AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
                AND l_quantity >= 10 AND l_quantity <= 20 AND p_size BETWEEN 1 AND 10
                AND l_shipmode IN ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON')
            OR (p_partkey = l_partkey AND p_brand = 'Brand#32'
                AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
                AND l_quantity >= 20 AND l_quantity <= 30 AND p_size BETWEEN 1 AND 15
                AND l_shipmode IN ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON')"#),

        // Q20: Nested correlated subqueries (now with predicate pushdown)
        ("Q20", "", r#"SELECT s_name, s_address FROM supplier, nation
        WHERE s_suppkey IN (SELECT ps_suppkey FROM partsupp
                WHERE ps_partkey IN (SELECT p_partkey FROM part WHERE p_name LIKE 'forest%')
                    AND ps_availqty > (SELECT 0.5 * SUM(l_quantity) FROM lineitem
                        WHERE l_partkey = ps_partkey AND l_suppkey = ps_suppkey
                            AND l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01'))
            AND s_nationkey = n_nationkey AND n_name = 'CANADA'
        ORDER BY s_name"#),

        // Q21: 4-way join with EXISTS (now with predicate pushdown)
        ("Q21", "", r#"SELECT s_name, COUNT(*) AS numwait FROM supplier, lineitem l1, orders, nation
        WHERE s_suppkey = l1.l_suppkey AND o_orderkey = l1.l_orderkey AND o_orderstatus = 'F'
            AND l1.l_receiptdate > l1.l_commitdate
            AND EXISTS (SELECT * FROM lineitem l2 WHERE l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey <> l1.l_suppkey)
            AND s_nationkey = n_nationkey AND n_name = 'ARGENTINA'
        GROUP BY s_name ORDER BY numwait DESC, s_name LIMIT 100"#),

        // Q22: SUBSTRING with correlated subquery
        ("Q22", "", r#"SELECT cntrycode, COUNT(*) AS numcust, SUM(c_acctbal) AS totacctbal
        FROM (SELECT SUBSTRING(c_phone FROM 1 FOR 2) AS cntrycode, c_acctbal FROM customer
            WHERE SUBSTRING(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
                AND c_acctbal > (SELECT AVG(c_acctbal) FROM customer
                    WHERE c_acctbal > 0.00
                        AND SUBSTRING(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17'))
        ) AS custsale GROUP BY cntrycode ORDER BY cntrycode"#),
    ];

    let mut passed = 0;
    let mut skipped = 0;
    let mut failed_labels = Vec::new();

    for (label, skip_reason, sql) in queries {
        if !skip_reason.is_empty() {
            eprintln!("  {label}: SKIP — {skip_reason}");
            skipped += 1;
            continue;
        }
        if try_query(&mut session, label, sql) {
            passed += 1;
        } else {
            failed_labels.push(*label);
        }
    }

    let total = queries.len();
    let executed = total - skipped;
    eprintln!("\nTPC-H results: {passed}/{executed} executed queries passed ({skipped} skipped, {total} total)");
    if !failed_labels.is_empty() {
        eprintln!("  Failed: {}", failed_labels.join(", "));
    }

    // With predicate pushdown, INTERVAL arithmetic, and EXTRACT fixes, all 22 queries pass.
    assert!(
        passed >= 22,
        "Too few TPC-H queries passed: {passed}/{executed}. Failed: {}",
        failed_labels.join(", ")
    );
}

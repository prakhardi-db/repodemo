-- Databricks notebook source
SELECT
  COUNT(distinct(s_suppkey)) AS num_suppliers
FROM
  samples.tpch.supplier

-- COMMAND ----------

SELECT
    year(o_orderdate) AS year,
    n_name AS nation,
    sum(l_extendedprice * (1 - l_discount) * (((length(n_name))/100) + (year(o_orderdate)-1993)/100)) AS revenue
FROM
    `samples`.`tpch`.`customer`,
    `samples`.`tpch`.`orders`,
    `samples`.`tpch`.`lineitem`,
    `samples`.`tpch`.`supplier`,
    `samples`.`tpch`.`nation`,
    `samples`.`tpch`.`region`
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND n_name in ('ARGENTINA', 'UNITED KINGDOM', 'FRANCE','BRAZIL', 'CHINA', 'UNITED STATES', 'JAPAN', 'JORDAN')
    AND o_orderdate >= DATE '1994-01-01'
GROUP BY
    1,2
ORDER BY
    nation ASC LIMIT 1000;

-- COMMAND ----------

SELECT
    initcap(n_name) AS Nation, 
    SUM(l_extendedprice * (1 - l_discount) * (length(n_name)/100)) AS revenue
FROM
    samples.tpch.customer,
    samples.tpch.orders,
    samples.tpch.lineitem,
    samples.tpch.supplier,
    samples.tpch.nation,
    samples.tpch.region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
GROUP BY
    INITCAP(n_name)
ORDER BY
    revenue DESC;

-- COMMAND ----------



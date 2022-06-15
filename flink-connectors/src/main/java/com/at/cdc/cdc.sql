
SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '1s';
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'table.sql-dialect' = 'default';



create table if not exists source_order_tbl
(
    db_name STRING METADATA FROM 'database_name' VIRTUAL,
    table_name STRING METADATA  FROM 'table_name' VIRTUAL,
    operation_ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL,
    order_id      int,
    order_date    TIMESTAMP(0),
    customer_name string,
    price         decimal(10, 5),
    product_id    int,
    order_status  boolean,
    primary key (order_id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'hadoop102',
    'port' = '3306',
    'username' = 'root',
    'password' = 'root',
    'connect.timeout' = '30s',
    'connect.max-retries' = '3',
    'connection.pool.size' = '5',
    'jdbc.properties.useSSL' = 'false',
    'jdbc.properties.characterEncoding' = 'utf-8',
    'database-name' = 'gmall_report',
    'table-name' = 'orders_tbl'
)

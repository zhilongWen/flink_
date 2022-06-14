
/*
 {
  "orderId": "20211122103434136000001",
  "userId": "300000971",
  "orderTime": "2021-11-22 10:34:34.136",
  "ip": "123.232.118.98",
  "orderMoney": 485.48,
  "orderStatus": 0
}

{"orderId": "20211122103434136000001","userId": "300000971","orderTime": "2021-11-22 10:34:34.136","ip": "123.232.118.98","orderMoney": 485.48,"orderStatus": 0}


 */

CREATE TABLE if not exists order_kafka_source
(
    orderId STRING,
    userId STRING,
    orderTime STRING,
    ip STRING,
    orderMoney  DOUBLE,
    orderStatus INT
)
WITH (
    'connector' = 'kafka',
    'topic' = 'order-topic',
    'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',
    'properties.group.id' = 'test-group-id',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
)

SELECT orderId, userId, orderTime, ip, orderMoney, orderStatus FROM order_kafka_source


CREATE TABLE order_hudi_sink (
     orderId STRING PRIMARY KEY NOT ENFORCED,
     userId STRING,
     orderTime STRING,
     ip STRING,
     orderMoney DOUBLE,
     orderStatus INT,
     ts STRING,
     partition_day STRING
)
    PARTITIONED BY (partition_day)
WITH (
    'connector' = 'hudi',
    'path' = 'file:///D:/workspace/flink_/files/order_hudi_sink',
    'table.type' = 'MERGE_ON_READ',
    'write.operation' = 'upsert',
    'hoodie.datasource.write.recordkey.field'= 'orderId',
    'write.precombine.field' = 'ts',
    'write.tasks'= '1'
)

INSERT INTO order_hudi_sink
SELECT
    orderId, userId, orderTime, ip, orderMoney, orderStatus,
    substring(orderId, 0, 17) AS ts, substring(orderTime, 0, 10) AS partition_day
FROM order_kafka_source


SELECT * FROM order_hudi_sink

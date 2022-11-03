CREATE TABLE UpsetKafkaTable (
    userId INT,
    itemId BIGINT,
    behavior STRING,
    ts BIGINT
    PRIMARY KEY (`userId`,`ts`) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',
    'topic' = 'test-2',
    'key.format' = 'json',
    'key.json.ignore-parse-errors' = 'true',
    'value.format' = 'json',
    'value.json.fail-on-missing-field' = 'false',
    'value.fields-include' = 'EXCEPT_KEY'
)


CREATE TABLE KafkaTable (
    userId INT,
    itemId BIGINT,
    behavior STRING,
    ts BIGINT
) WITH (
    'connector' = 'upsert-kafka',
    'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',
    'topic' = 'test-1',
    'key.format' = 'json',
    'key.json.ignore-parse-errors' = 'true',
    'value.format' = 'json',
    'value.json.fail-on-missing-field' = 'false'
)
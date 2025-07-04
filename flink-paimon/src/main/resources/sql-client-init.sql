create catalog hive_iceberg_catalog with (
  'type'='iceberg',
  'catalog-type'='hive',
  'uri'='thrift://hadoop102:9083',
  'clients'='5',
  'property-version'='1',
  'warehouse'='hdfs://hadoop102:8020/user/hive/warehouse'
)
;

create catalog hadoop_catalog with (
  'type'='iceberg',
  'catalog-type'='hadoop',
  'warehouse'='hdfs://10.211.55.102:8020/user/hive/warehouse/iceberg_db.db/iceberg_hadoop',
  'property-version'='1'
)
;

create catalog fs_paimon_catalog with (
  'type'='paimon',
  'warehouse'='hdfs://10.211.55.102:8020/user/hive/warehouse/paimon_db.db/fs'
)
;

create catalog hive_paimon_catalog with (
    'type' = 'paimon',
    'metastore' = 'hive',
    'uri'=  'thrift://hadoop102:9083',
    'hive-conf-dir'='/opt/module/hive/conf',
    'warehouse' = 'hdfs://10.211.55.102:8020/user/hive/warehouse'
);

use catalog hive_paimon_catalog;

-- https://paimon.apache.org/docs/1.2/flink/sql-ddl/
set 'hive.metastore.disallow.incompatible.col.type.changes'='false';
set 'hive.strict.managed.tables'='false';
set 'hive.create.as.insert.only'='false';
set 'metastore.create.as.acid'='false';

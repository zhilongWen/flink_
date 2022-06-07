

-- https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/connectors/table/hive/hive_dialect/
create catalog myhive with (
    'type' = 'hive',
    'hive-conf-dir' = '/opt/module/hive-3.1.2/conf',
    'default-database'='default');

use catalog myhive;
load module hive;
use modules hive,core;
set table.sql-dialect=hive;
SET 'execution.runtime-mode' = 'batch';




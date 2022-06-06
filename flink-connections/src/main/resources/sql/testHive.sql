

-- https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/connectors/table/hive/hive_dialect/
Flink SQL> create catalog myhive with ('type' = 'hive', 'hive-conf-dir' = '/opt/hive-conf');
[INFO] Execute statement succeed.

Flink SQL> use catalog myhive;
[INFO] Execute statement succeed.

Flink SQL> load module hive;
[INFO] Execute statement succeed.

Flink SQL> use modules hive,core;
[INFO] Execute statement succeed.

Flink SQL> set table.sql-dialect=hive;
[INFO] Session property has been set.

Flink SQL> select explode(array(1,2,3)); -- call hive udtf
+-----+
| col |
+-----+
|   1 |
|   2 |
|   3 |
+-----+
3 rows in set

    Flink SQL> create table tbl (key int,value string);
[INFO] Execute statement succeed.

Flink SQL> insert overwrite table tbl values (5,'e'),(1,'a'),(1,'a'),(3,'c'),(2,'b'),(3,'c'),(3,'c'),(4,'d');
[INFO] Submitting SQL update statement to the cluster...
                          [INFO] SQL update statement has been successfully submitted to the cluster:

                                         Flink SQL> select * from tbl cluster by key; -- run cluster by
2021-04-22 16:13:57,005 INFO  org.apache.hadoop.mapred.FileInputFormat                     [] - Total input paths to process : 1
+-----+-------+
| key | value |
+-----+-------+
|   1 |     a |
|   1 |     a |
|   5 |     e |
|   2 |     b |
|   3 |     c |
|   3 |     c |
|   3 |     c |
|   4 |     d |
+-----+-------+
8 rows in set



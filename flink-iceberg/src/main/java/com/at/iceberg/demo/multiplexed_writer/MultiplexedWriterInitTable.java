package com.at.iceberg.demo.multiplexed_writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;

public class MultiplexedWriterInitTable {
    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME", "root");

        Configuration conf = new Configuration();
        HadoopCatalog hadoopCatalog = new HadoopCatalog(
                conf,
                "hdfs://10.211.55.102:8020/user/hive/warehouse/iceberg_db.db/iceberg_hadoop"
        );

        TableIdentifier tableIdentifier = TableIdentifier.of("test_db", "user_actions");

        org.apache.iceberg.Schema tableSchema = new org.apache.iceberg.Schema(
                Types.NestedField.required(1, "user_id", Types.IntegerType.get()),
                Types.NestedField.required(2, "action", Types.StringType.get()),
                Types.NestedField.required(3, "timestamp", Types.TimestampType.withZone()),
                Types.NestedField.optional(4, "details", Types.StringType.get())
        );

        PartitionSpec partitionSpec = PartitionSpec.builderFor(tableSchema)
                .hour("timestamp")
                .build();

        hadoopCatalog
                .buildTable(
                        tableIdentifier,
                        tableSchema
                )
                .withPartitionSpec(partitionSpec)
                .create();
    }
}

package com.at.iceberg.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;

public class MaintenanceIcebergTable {
    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME", "root");

        // 1.获取 Table对象
        // 1.1 创建 catalog对象
        Configuration conf = new Configuration();
        HadoopCatalog hadoopCatalog = new HadoopCatalog(
                conf,
                "hdfs://10.211.55.102:8020/user/hive/warehouse/iceberg_db.db/iceberg_hadoop"
        );

        // 1.2 通过 catalog加载 Table对象
        Table table = hadoopCatalog.loadTable(TableIdentifier.of("default", "merged_table_source"));

        // 有Table对象，就可以获取元数据、进行维护表的操作
        System.out.println(table.history());
//        System.out.println(table.expireSnapshots().expireOlderThan());

//        // 2.通过 Actions 来操作 合并
//        Actions.forTable(table)
//                .rewriteDataFiles()
//                .targetSizeInBytes(1024L)
//                .execute();
    }
}

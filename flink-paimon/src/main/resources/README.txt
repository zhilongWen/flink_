
flink lib 添加如下 jar 包：

    flink-connector-hive_2.12-1.16.0.jar
    flink-connector-kafka-1.16.0.jar
    flink-runtime-web-1.16.0.jar
    flink-shaded-hadoop-2-uber-2.8.3-10.0.jar
    link-sql-connector-hive-3.1.2_2.12-1.16.2.jar
    hadoop-mapreduce-client-core-3.3.6.jar
    hive-exec-3.1.2.jar
    libfb303-0.9.3.jar
    paimon-flink-1.16-1.2.0.jar


配置 sql-client-init.sql

上传 flink 运行时依赖到 hdfs：
    hadoop fs -mkdir /flink-dir
    hadoop fs -put $FLINK_HOME/lib /flink-dir/
    hadoop fs -put $FLINK_HOME/opt /flink-dir/
    hadoop fs -put $FLINK_HOME/plugins /flink-dir/

yarn session 模式启动 flink 集群：
    ./bin/yarn-session.sh -nm flink-job -jm 1024m -tm 4096 -s 1 -Dyarn.provided.lib.dirs='hdfs://hadoop102:8020/flink-dir/lib' -d

启动 sql-client：
    ./bin/sql-client.sh -i /opt/module/flink-1.16.0/conf/sql-client-init.sql -s yarn-session -j ./lib/*


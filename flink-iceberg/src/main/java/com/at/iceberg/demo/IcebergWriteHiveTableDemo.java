package com.at.iceberg.demo;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.hadoop.mapred.utils.HadoopUtils;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import static org.apache.flink.table.catalog.hive.HiveCatalog.isEmbeddedMetastore;
import static org.apache.flink.table.catalog.hive.util.HiveTableUtil.getHadoopConfiguration;
import static org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly;

public class IcebergWriteHiveTableDemo {
    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME", "root");

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        classLoader.setClassAssertionStatus("jdk.internal.loader.ClassLoaders$AppClassLoader", true);

        Configuration defaultConfig = new Configuration();
        defaultConfig.setString("rest.bind-port", "8081");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(defaultConfig);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        env.setRestartStrategy(
                org.apache.flink.api.common.restartstrategy.RestartStrategies.fixedDelayRestart(
                        3,
                        // 10 seconds restart window
                        10000
                )
        );

        // start a checkpoint every 1000 ms
        env.enableCheckpointing(1000);
        // advanced options:
        // set mode to exactly-once (this is the default)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // make sure 500 ms of progress happen between checkpoints
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(60 * 1000);
        // checkpoints have to complete within one minute, or are discarded
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000);
        // only two consecutive checkpoint failures are tolerated
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        // allow only one checkpoint to be in progress at the same time
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // enable externalized checkpoints which are retained
        // after job cancellation
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // enables the unaligned checkpoints
//        env.getCheckpointConfig().enableUnalignedCheckpoints();
        Configuration checkpointConfig = new Configuration();
        checkpointConfig.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
        checkpointConfig.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        checkpointConfig.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "hdfs://10.211.55.102:8020/tmp/checkpoints/IcebergWriteHiveTableDemo");
        env.configure(checkpointConfig);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
                .setTopics("user_behaviors")
                .setGroupId("IcebergWriteHiveTableDemo")
//                .setStartingOffsets(OffsetsInitializer.earliest())
                .setStartingOffsets(OffsetsInitializer.timestamp(1750525590262L))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<UserBehavior> sourceStream = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "user_behaviors")
                .uid("user_behaviors_Kafka_Source")
                .name("user_behaviors_Kafka_Source")
                .setParallelism(4)
                .map(new RichMapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        return JSON.parseObject(value, UserBehavior.class);
                    }
                })
                .uid("user_behaviors_parse")
                .name("user_behaviors_parse")
                .setParallelism(4);

        Schema tableSchema = Schema.newBuilder()
                .column("userId", DataTypes.INT())
                .column("itemId", DataTypes.BIGINT())
                .column("categoryId", DataTypes.INT())
                .column("behavior", DataTypes.STRING())
                .column("ts", DataTypes.BIGINT())
                // 把 ts 转换成 TIMESTAMP(3)，作为事件时间字段
                .columnByExpression("event_time", "TO_TIMESTAMP_LTZ(ts, 3)")
                // 定义 watermark 策略（这里是延迟 5 秒）
                .watermark("event_time", "event_time - INTERVAL '5' SECOND")
                .build();

        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        String catalogName = "hive_catalog";
        String defaultDatabase = "default";
        String hiveConfDir = "/Users/wenzhilong/warehouse/space/flink_/conf";
        String version = "3.1.2";

//        HiveCatalog hive = new HiveCatalog(catalogName, defaultDatabase, hiveConfDir, version);

        HiveConf hiveConf = createHiveConf(hiveConfDir, null);
        hiveConf.set("hive.metastore.uris", "thrift://hadoop102:9083");
        HiveCatalog hive = new HiveCatalog(catalogName, defaultDatabase, hiveConf, version);

        tableEnv.registerCatalog(catalogName, hive);

        tableEnv.useCatalog(catalogName);
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.useDatabase("iceberg_db");

        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.executeSql("create table if not exists hive_stream_table_user_behaviors(\n"
                + "        user_id int,\n"
                + "        item_id bigint,\n"
                + "        category_id int,\n"
                + "        behavior string,\n"
                + "        ts bigint\n"
                + ")COMMENT 'flink create table hive_stream_table_user_behaviors'\n"
                + "PARTITIONED BY (`dt` string,`hm` string,`mm` string)\n"
                + "STORED AS PARQUET\n"
                + "LOCATION '/warehouse/iceberg_db.db/hive_stream_table_user_behaviors'\n"
                + "TBLPROPERTIES (\n"
                + "        -- using default partition-name order to load the latest partition every 12h (the most recommended and convenient way)\n"
                + "        'streaming-source.enable' = 'true',\n"
                + "        'streaming-source.partition.include' = 'latest',\n"
                + "        'streaming-source.monitor-interval' = '1 min',\n"
                + "        'streaming-source.partition-order' = 'partition-name',  -- option with default value, can be ignored.\n"
                + "\n"
                + "        -- Parquet + Snappy 配置\n"
                + "        'parquet.compression' = 'SNAPPY',\n"
                + "        'partition.time-extractor.timestamp-pattern' = '$dt $hm:$mm:00',\n"
                + "        'sink.partition-commit.trigger'='partition-time',\n"
                + "        'sink.partition-commit.delay'='1 min',\n"
                + "        'sink.partition-commit.watermark-time-zone'='Asia/Shanghai',\n"
                + "        'sink.partition-commit.policy.kind'='metastore,success-file'\n"
                + ")\n");

        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
//        Table table = tableEnv.fromDataStream(sourceStream, tableSchema);
//        tableEnv.createTemporaryView("user_behaviors", table);
        tableEnv.createTemporaryView("user_behaviors_view", sourceStream, tableSchema);

//        tableEnv.executeSql("select * from user_behaviors_view").print();
//
        tableEnv.executeSql("insert into hive_stream_table_user_behaviors\n"
                + "select userId as user_id,\n"
                + "       itemId as item_id,\n"
                + "       categoryId as category_id,\n"
                + "       behavior,\n"
                + "       ts,\n"
                + "       DATE_FORMAT(event_time, 'yyyy-MM-dd') as dt,\n"
                + "       DATE_FORMAT(event_time, 'HH') as hm,\n"
                + "       DATE_FORMAT(event_time, 'mm') as mm\n"
                + "from user_behaviors_view");

//        env.execute("IcebergWriteHdfsFileDemo");
    }

    public static final String HIVE_SITE_FILE = "hive-site.xml";

    static HiveConf createHiveConf(@Nullable String hiveConfDir, @Nullable String hadoopConfDir) {
        // create HiveConf from hadoop configuration with hadoop conf directory configured.
        org.apache.hadoop.conf.Configuration hadoopConf = null;
        if (isNullOrWhitespaceOnly(hadoopConfDir)) {
            for (String possibleHadoopConfPath :
                    HadoopUtils.possibleHadoopConfPaths(
                            new org.apache.flink.configuration.Configuration())) {
                hadoopConf = getHadoopConfiguration(possibleHadoopConfPath);
                if (hadoopConf != null) {
                    break;
                }
            }
        } else {
            hadoopConf = getHadoopConfiguration(hadoopConfDir);
            if (hadoopConf == null) {
                String possiableUsedConfFiles =
                        "core-site.xml | hdfs-site.xml | yarn-site.xml | mapred-site.xml";
                throw new CatalogException(
                        "Failed to load the hadoop conf from specified path:" + hadoopConfDir,
                        new FileNotFoundException(
                                "Please check the path none of the conf files ("
                                        + possiableUsedConfFiles
                                        + ") exist in the folder."));
            }
        }
        if (hadoopConf == null) {
            hadoopConf = new org.apache.hadoop.conf.Configuration();
        }
        // ignore all the static conf file URLs that HiveConf may have set
        HiveConf.setHiveSiteLocation(null);
        HiveConf.setLoadMetastoreConfig(false);
        HiveConf.setLoadHiveServer2Config(false);
        HiveConf hiveConf = new HiveConf(hadoopConf, HiveConf.class);

        System.out.println("Setting hive conf dir as " + hiveConfDir);

        if (hiveConfDir != null) {
            Path hiveSite = new Path(hiveConfDir, HIVE_SITE_FILE);
            if (!hiveSite.toUri().isAbsolute()) {
                // treat relative URI as local file to be compatible with previous behavior
                hiveSite = new Path(new File(hiveSite.toString()).toURI());
            }
            try (InputStream inputStream = hiveSite.getFileSystem(hadoopConf).open(hiveSite)) {
                hiveConf.addResource(inputStream, hiveSite.toString());
                // trigger a read from the conf so that the input stream is read
                isEmbeddedMetastore(hiveConf);
            } catch (IOException e) {
                throw new CatalogException(
                        "Failed to load hive-site.xml from specified path:" + hiveSite, e);
            }
        } else {
            // user doesn't provide hive conf dir, we try to find it in classpath
            URL hiveSite =
                    Thread.currentThread().getContextClassLoader().getResource(HIVE_SITE_FILE);
            if (hiveSite != null) {
                System.out.println("Found " + HIVE_SITE_FILE + " in classpath: " + hiveSite);
                hiveConf.addResource(hiveSite);
            }
        }
        return hiveConf;
    }
}

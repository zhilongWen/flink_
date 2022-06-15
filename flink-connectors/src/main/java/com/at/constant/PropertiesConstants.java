package com.at.constant;

/**
 * @create 2022-06-04
 */
public class PropertiesConstants {

    // --default.parallelism 1 --enable.checkpoint true --checkpoint.type fs --checkpoint.dir file:///D:\\workspace\\flink_\\files\\ck --checkpoint.interval 60000 --enable.table.env true

    // flink

    public static final String EXECUTE_MODE = "execute.mode";
    public static final String BATCH_MODE = "batch";
    public static final String STREAMING_MODE = "stream";

    public static final String DEFAULT_PARALLELISM = "default.parallelism";

    public static final String ENABLE_CHECKPOINT = "enable.checkpoint";
    public static final String CHECKPOINT_TYPE = "checkpoint.type";
    public static final String CHECKPOINT_DIR = "checkpoint.dir";
    public static final String CHECKPOINT_INTERVAL = "checkpoint.interval";

    public static final String MEMORY = "memory";
    public static final String FS = "fs";
    public static final String ROCKSDB = "rocksdb";

    public static final String ENABLE_TABLE_ENV = "enable.table.env";
    public static final String TABLE_MIN_BATCH = "table.min.batch";

    public static final String ENABLE_HIVE_ENV = "enable.hive.env";

    public static final String DEFAULT_PROPERTIES = "/application.properties";

    // kafka
    public static final String KAFKA_BOOTSTRAP_SERVERS = "bootstrap.servers";






}

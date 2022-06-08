package com.at.util;

import com.at.constant.PropertiesConstants;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.parquet.Preconditions;


import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

/**
 * @create 2022-06-04
 */
public class EnvironmentUtil {


    private static ParameterTool buildParameterTool(final String[] args) {
        try {
            return ParameterTool
                    .fromPropertiesFile(EnvironmentUtil.class.getResourceAsStream(PropertiesConstants.DEFAULT_PROPERTIES))
                    .mergeWith(ParameterTool.fromArgs(args))
                    .mergeWith(ParameterTool.fromSystemProperties());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ParameterTool.fromArgs(args).mergeWith(ParameterTool.fromSystemProperties());
    }

    public static Environment getExecutionEnvironment(String[] args) {

        // get ParameterTool
        ParameterTool parameterTool = buildParameterTool(args);

        // create stream environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //set default parallel
        Optional.ofNullable(parameterTool.get(PropertiesConstants.DEFAULT_PARALLELISM)).ifPresent(p -> env.setParallelism(Integer.parseInt(p)));

        // set restart strategy
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 60000));

        // set checkpoint config
        if (checkArgument(parameterTool.get(PropertiesConstants.ENABLE_CHECKPOINT))) {
            CheckpointUtil.enableCheckpoint(env, parameterTool);
        }

        // set global param
        env.getConfig().setGlobalJobParameters(parameterTool);


        // if not table environment, return Environment
        if (!checkArgument(parameterTool.get(PropertiesConstants.ENABLE_TABLE_ENV))) return new Environment(env);

        // add table environment

        // build environment setting
        EnvironmentSettings.Builder settingsBuilder = null;

        // if not batch mode
        if (PropertiesConstants.BATCH_MODE.equalsIgnoreCase(parameterTool.get(PropertiesConstants.EXECUTE_MODE))) {
            settingsBuilder = EnvironmentSettings.newInstance().inStreamingMode();
        } else {
            settingsBuilder = EnvironmentSettings.newInstance().inStreamingMode();
        }

        // if not table min batch
        if (!checkArgument(parameterTool.get(PropertiesConstants.TABLE_MIN_BATCH)))
            return new Environment(env, StreamTableEnvironment.create(env, settingsBuilder.build()));


        // add min batch config

        // instantiate table environment
        Configuration configuration = new Configuration();
        // set low-level key-value options
        configuration.setString("table.exec.mini-batch.enabled", "true");
        configuration.setString("table.exec.mini-batch.allow-latency", "5 s");
        configuration.setString("table.exec.mini-batch.size", "5000");
        EnvironmentSettings settings = settingsBuilder.withConfiguration(configuration).build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // access flink configuration after table environment instantiation
        // set low-level key-value options
        tableEnv.getConfig().set("table.exec.mini-batch.enabled", "true");
        tableEnv.getConfig().set("table.exec.mini-batch.allow-latency", "5 s");
        tableEnv.getConfig().set("table.exec.mini-batch.size", "5000");

        return new Environment(env, tableEnv);


    }

    public static StreamTableEnvironment enableHiveEnv(StreamTableEnvironment tableEnv) {

        String name = "myhive";
        String defaultDatabase = "default";
        String hiveConfDir = "./conf";
        String version = "3.1.2";

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
        tableEnv.registerCatalog("myhive", hive);


        tableEnv.useCatalog("myhive");
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.useDatabase("default");

        return tableEnv;

    }

    public static boolean checkArgument(String condition) {

        if (condition == null) {
            return false;
        } else {
            return Boolean.parseBoolean(condition);
        }

    }

    public static class Environment {

        private StreamExecutionEnvironment env;
        private StreamTableEnvironment tableEnv;
        private boolean isTableEnv;

        public Environment(StreamExecutionEnvironment env) {
            this(env, null, false);
        }

        public Environment(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
            this(env, tableEnv, true);
        }

        public Environment(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, boolean isTableEnv) {
            this.env = Preconditions.checkNotNull(env, "StreamExecutionEnvironment must not be null");
            this.tableEnv = tableEnv;
            this.isTableEnv = isTableEnv;
        }

        public Environment() {
        }

        public StreamExecutionEnvironment getEnv() {
            return env;
        }

        public void setEnv(StreamExecutionEnvironment env) {
            this.env = env;
        }

        public StreamTableEnvironment getTableEnv() {
            return tableEnv;
        }

        public void setTableEnv(StreamTableEnvironment tableEnv) {
            this.tableEnv = tableEnv;
        }

        public boolean isTableEnv() {
            return isTableEnv;
        }

        public void setTableEnv(boolean tableEnv) {
            isTableEnv = tableEnv;
        }

        @Override
        public String toString() {
            return "Environment{" +
                    "env=" + env +
                    ", tableEnv=" + tableEnv +
                    ", isTableEnv=" + isTableEnv +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Environment that = (Environment) o;
            return isTableEnv == that.isTableEnv && Objects.equals(env, that.env) && Objects.equals(tableEnv, that.tableEnv);
        }

        @Override
        public int hashCode() {
            return Objects.hash(env, tableEnv, isTableEnv);
        }
    }


}
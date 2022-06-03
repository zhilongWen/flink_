package com.at.util;

import com.at.constant.PropertiesConstants;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.orc.OrcFilters;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.parquet.Preconditions;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.zookeeper.Op;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

/**
 * @create 2022-06-04
 */
public class EnvironmentUtil {


    public static ParameterTool buildParameterTool(final String[] args) {
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

    public static Environment getStreamExecutionEnvironment(String[] args) {

        ParameterTool parameterTool = buildParameterTool(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Optional.ofNullable(parameterTool.get(PropertiesConstants.DEFAULT_PARALLELISM)).ifPresent(p -> env.setParallelism(Integer.parseInt(p)));
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 60000));

        Optional.ofNullable(parameterTool.get(PropertiesConstants.ENABLE_CHECKPOINT)).filter(t -> Boolean.getBoolean(t)).ifPresent(t -> CheckpointUtil.enableCheckpoint(env, parameterTool));

        env.getConfig().setGlobalJobParameters(parameterTool);

        if (parameterTool.get(PropertiesConstants.ENABLE_TABLE_ENV) != null && parameterTool.getBoolean(PropertiesConstants.ENABLE_TABLE_ENV)) {

            //table 环境
            EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
            StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

            return new Environment(env, tableEnv);
        }


        return new Environment(env);
    }

    static class Environment {

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
            this.env = Preconditions.checkNotNull(env,"StreamExecutionEnvironment must not be null");
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

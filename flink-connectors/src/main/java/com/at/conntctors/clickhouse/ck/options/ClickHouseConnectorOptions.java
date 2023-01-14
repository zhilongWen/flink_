package com.at.conntctors.clickhouse.ck.options;

import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Optional;

/**
 * @author zero
 * @create 2022-11-13
 */
public class ClickHouseConnectorOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    protected final String url;
    protected final int connectionCheckTimeoutSeconds;
    @Nullable
    protected final String username;
    @Nullable
    protected final String password;

    // 本地表模式选项

    protected final boolean writeLocal;
    // SELECT shard_num, host_address, port FROM system.clusters WHERE cluster = 'gmall_cluster' ORDER BY shard_num, replica_num ASC
    protected final String cluster;
    protected final String tableName;

    public ClickHouseConnectorOptions(String url,
                                      int connectionCheckTimeoutSeconds,
                                      @Nullable String username,
                                      @Nullable String password,
                                      @Nullable boolean writeLocal,
                                      @Nullable String cluster,
                                      @Nullable String tableName) {
        this.url = Preconditions.checkNotNull(url);
        this.connectionCheckTimeoutSeconds = connectionCheckTimeoutSeconds;
        this.username = username;
        this.password = password;
        this.writeLocal = writeLocal;
        this.cluster = cluster;
        this.tableName = tableName;
    }

    public String getUrl() {
        return url;
    }

    public Optional<String> getUsername() {
        return Optional.ofNullable(username);
    }

    public Optional<String> getPassword() {
        return Optional.ofNullable(password);
    }

    public int getConnectionCheckTimeoutSeconds() {
        return connectionCheckTimeoutSeconds;
    }

    public Optional<Boolean> isWriteLocal() {
        return Optional.ofNullable(writeLocal);
    }

    public Optional<String> getCluster() {
        return Optional.ofNullable(cluster);
    }

    public Optional<String> getTableName() {
        return Optional.ofNullable(tableName);
    }


    public static class ClickHouseConnectorOptionsBuilder {

        private String url;
        private String username;
        private String password;
        private int connectionCheckTimeoutSeconds = 60;

        private boolean writeLocal;
        private String cluster;
        private String tableName;

        public ClickHouseConnectorOptionsBuilder withUrl(String url) {
            this.url = url;
            return this;
        }

        public ClickHouseConnectorOptionsBuilder withUsername(String username) {
            this.username = username;
            return this;
        }

        public ClickHouseConnectorOptionsBuilder withPassword(String password) {
            this.password = password;
            return this;
        }

        public ClickHouseConnectorOptionsBuilder withConnectionCheckTimeoutSeconds(
                int connectionCheckTimeoutSeconds) {
            this.connectionCheckTimeoutSeconds = connectionCheckTimeoutSeconds;
            return this;
        }

        public ClickHouseConnectorOptionsBuilder withWriteLocal(
                boolean writeLocal) {
            this.writeLocal = writeLocal;
            return this;
        }

        public ClickHouseConnectorOptionsBuilder withCluster(
                String cluster) {
            this.cluster = cluster;
            return this;
        }

        public ClickHouseConnectorOptionsBuilder withTableName(
                String tableName) {
            this.tableName = tableName;
            return this;
        }

        public ClickHouseConnectorOptions build() {
            return new ClickHouseConnectorOptions(
                    url, connectionCheckTimeoutSeconds, username, password, writeLocal, cluster, tableName);
        }

    }


}

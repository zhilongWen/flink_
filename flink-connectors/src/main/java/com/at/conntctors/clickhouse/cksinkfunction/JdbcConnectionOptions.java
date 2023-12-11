package com.at.conntctors.clickhouse.cksinkfunction;

import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Optional;


/**
 * @create 2023-12-09
 */
public class JdbcConnectionOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    protected final String url;
    @Nullable
    protected final String driverName;
    protected final int connectionCheckTimeoutSeconds;
    @Nullable
    protected final String username;
    @Nullable
    protected final String password;

    protected final String database;
    protected final String port;

    protected JdbcConnectionOptions(
            String url,
            @Nullable String driverName,
            @Nullable String username,
            @Nullable String password,
            int connectionCheckTimeoutSeconds) {
        Preconditions.checkArgument(connectionCheckTimeoutSeconds > 0);
        this.url = Preconditions.checkNotNull(url, "jdbc url is empty");
        this.driverName = driverName;
        this.username = username;
        this.password = password;
        this.connectionCheckTimeoutSeconds = connectionCheckTimeoutSeconds;

         this.port = url.substring(url.indexOf("//") + 2, url.lastIndexOf("/")).split(":")[1];
         this.database = url.substring(url.lastIndexOf("/") + 1);
    }

    public String getDbURL() {
        return url;
    }

    @Nullable
    public String getDriverName() {
        return driverName;
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

    public String getUrl() {
        return url;
    }

    public String getDatabase() {
        return database;
    }

    public String getPort() {
        return port;
    }

    public static final class Builder {

        private String url;
        private String driverName;
        private String username;
        private String password;
        private int connectionCheckTimeoutSeconds = 60;

        public Builder withUrl(String url) {
            this.url = url;
            return this;
        }

        public Builder withDriverName(String driverName) {
            this.driverName = driverName;
            return this;
        }

        public Builder withUsername(String username) {
            this.username = username;
            return this;
        }

        public Builder withPassword(String password) {
            this.password = password;
            return this;
        }

        /**
         * Set the maximum timeout between retries, default is 60 seconds.
         *
         * @param connectionCheckTimeoutSeconds the timeout seconds, shouldn't smaller than 1
         *     second.
         */
        public Builder withConnectionCheckTimeoutSeconds(
                int connectionCheckTimeoutSeconds) {
            this.connectionCheckTimeoutSeconds = connectionCheckTimeoutSeconds;
            return this;
        }

        public JdbcConnectionOptions build() {
            return new JdbcConnectionOptions(
                    url, driverName, username, password, connectionCheckTimeoutSeconds);
        }
    }

}

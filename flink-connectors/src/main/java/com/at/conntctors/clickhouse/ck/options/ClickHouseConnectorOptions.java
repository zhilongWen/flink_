package com.at.conntctors.clickhouse.ck.options;

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

    public ClickHouseConnectorOptions(String url, int connectionCheckTimeoutSeconds, @Nullable String username, @Nullable String password) {
        this.url = url;
        this.connectionCheckTimeoutSeconds = connectionCheckTimeoutSeconds;
        this.username = username;
        this.password = password;
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

    public static class ClickHouseConnectorOptionsBuilder {

        private String url;
        private String username;
        private String password;
        private int connectionCheckTimeoutSeconds = 60;

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

        public ClickHouseConnectorOptions build() {
            return new ClickHouseConnectorOptions(
                    url, connectionCheckTimeoutSeconds, username, password);
        }

    }


}

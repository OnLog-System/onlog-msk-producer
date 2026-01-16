package onlog.consumer.ingest;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;

public class DbConnectionPool {

    private static final HikariDataSource ds;

    static {
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(System.getenv("DB_URL"));
        cfg.setUsername(System.getenv("DB_USER"));
        cfg.setPassword(System.getenv("DB_PASS"));

        cfg.setMaximumPoolSize(10);
        cfg.setMinimumIdle(2);
        cfg.setConnectionTimeout(5000);
        cfg.setIdleTimeout(60000);
        cfg.setMaxLifetime(600000);

        ds = new HikariDataSource(cfg);
    }

    public static DataSource dataSource() {
        return ds;
    }
}

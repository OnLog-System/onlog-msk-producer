package onlog.consumer.ingest;

import java.sql.Connection;
import java.sql.DriverManager;

public class DbConnectionPool {

    private static final String URL  = System.getenv("DB_URL");
    private static final String USER = System.getenv("DB_USER");
    private static final String PASS = System.getenv("DB_PASS");

    public static Connection get() throws Exception {
        return DriverManager.getConnection(URL, USER, PASS);
    }
}

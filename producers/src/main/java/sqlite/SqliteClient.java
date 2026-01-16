package sqlite;

import java.sql.Connection;
import java.sql.DriverManager;

public class SqliteClient {

    public static Connection connect(String path) throws Exception {
        String url = "jdbc:sqlite:" + path;
        return DriverManager.getConnection(url);
    }
}

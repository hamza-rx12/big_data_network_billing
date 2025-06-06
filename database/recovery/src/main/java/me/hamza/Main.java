package me.hamza;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapListHandler;

import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

public class Main {
    public static void main(String[] args) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:postgresql://postgresdb:5432/mydatabase");
        config.setUsername("admin");
        config.setPassword("admin");
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

        while (true) {
            try {
                DataSource dataSource = new HikariDataSource(config);
                QueryRunner queryRunner = new QueryRunner(dataSource);

                System.out.println("Connection to PostgreSQL established successfully.");
                String sql = "SELECT 1";
                List<Map<String, Object>> results = queryRunner.query(sql, new MapListHandler());
                for (Map<String, Object> row : results) {
                    System.out.println(row);
                }
                break;
            } catch (Exception e) {
                System.out.println("Waiting for database to startup!");
                try {
                    Thread.sleep(1000);
                } catch (Exception ex) {
                    System.err.println("Cannot put thread to sleep!");
                }

            }
        }
    }
}
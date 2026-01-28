package com.testcontainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.time.Duration;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import com.github.dockerjava.api.model.Capability;

public class KineticaContainerTest {

    public static void main(String[] args) {
        GenericContainer<?> kinetica = new GenericContainer<>(DockerImageName.parse("kinetica/kinetica-cpu:latest"))
                .withExposedPorts(9191)         
                .withEnv("GPUDB_START_ALL", "1")    
                .waitingFor(
                    Wait.forHttp("/")
                        .forPort(9191)
                        .forStatusCode(200)
                        .withStartupTimeout(Duration.ofMinutes(10))
                )
                .withCreateContainerCmdModifier(cmd -> {
                    cmd.getHostConfig().withCapAdd(Capability.SYS_NICE);
                })
                .withReuse(true);  // useful during development – keeps container alive between test runs

        kinetica.start();
        
        try {
            // === Build JDBC URL ===
            String host = kinetica.getHost();
            int port = kinetica.getMappedPort(9191);
            String jdbcUrl = String.format("jdbc:kinetica://%s:%d", host, port);

            System.out.println("JDBC URL: " + jdbcUrl);

            // === Connect and run test ===
            try (Connection conn = DriverManager.getConnection(jdbcUrl, "admin", "admin")) {
                System.out.println("Connection successful!");

                try (Statement stmt = conn.createStatement()) {
                    // Create table
                    stmt.execute("CREATE TABLE IF NOT EXISTS test_table (id INT, name VARCHAR(50))");
                    System.out.println("Table created or already exists.");

                    // Insert data
                    stmt.execute("INSERT INTO test_table VALUES (1, 'Testcontainers1')");
                    System.out.println("Inserted test row 1.");
                    
                    stmt.execute("INSERT INTO test_table VALUES (2, 'Testcontainers2')");
                    System.out.println("Inserted test row 2.");
                    
                    // Query
                    //try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_table")) {
                    try (PreparedStatement pstmt = conn.prepareStatement("SELECT * FROM test_table")) {   
                        ResultSet rs = pstmt.executeQuery();
                        System.out.println("Statement select executed = " +  rs);
                        
                     // 1. Get Metadata to find out how many columns we have
                        ResultSetMetaData rsmd = rs.getMetaData();
                        int columnCount = rsmd.getColumnCount();

                        // 2. Iterate through each row
                        while (rs.next()) {
                            // 3. Iterate through each column in the current row
                            for (int i = 1; i <= columnCount; i++) {
                                String columnName = rsmd.getColumnName(i);
                                Object columnValue = rs.getObject(i); // getObject works for any type
                                
                                System.out.print(columnName + ": " + columnValue + " | ");
                            }
                            System.out.println(); // Move to next line for the next row
                        }
                    }

                    // Cleanup
                    stmt.execute("DROP TABLE test_table");
                    System.out.println("Table dropped.");
                }
            }
        } catch (Exception e) {
            System.err.println("Error during test:");
            e.printStackTrace();
        } finally {
            // Optional: stop container (comment out if you want to keep it running)
            // kinetica.stop();
            System.out.println("Test finished. Container is " + (kinetica.isRunning() ? "still running" : "stopped"));
        }
    }
}
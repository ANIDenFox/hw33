package org.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class Database {
    private static final String URL = "jdbc:postgresql://localhost:5432/mydatabase";
    private static final String USER = "postgres";
    private static final String PASSWORD = "admin";

    public static void main(String[] args) {
        try {
            Connection connection = DriverManager.getConnection(URL, USER, PASSWORD);

            String createUsersTable = "CREATE TABLE IF NOT EXISTS users (" +
                    "id SERIAL PRIMARY KEY, " +
                    "name VARCHAR(100) NOT NULL, " +
                    "email VARCHAR(100) UNIQUE NOT NULL" +
                    ")";
            String createOrdersTable = "CREATE TABLE IF NOT EXISTS orders (" +
                    "id SERIAL PRIMARY KEY, " +
                    "user_id INT REFERENCES users(id), " +
                    "product VARCHAR(100) NOT NULL, " +
                    "amount DECIMAL(10, 2) NOT NULL" +
                    ")";

            try (Statement stmt = connection.createStatement()) {
                stmt.execute(createUsersTable);
                stmt.execute(createOrdersTable);
            }

            Map<String, Integer> userIds = new HashMap<>();
            String insertUsers = "INSERT INTO users (name, email) VALUES (?, ?) ON CONFLICT (email) DO NOTHING RETURNING id, email";
            String getUserId = "SELECT id FROM users WHERE email = ?";
            try (PreparedStatement insertStmt = connection.prepareStatement(insertUsers);
                 PreparedStatement selectStmt = connection.prepareStatement(getUserId)) {

                insertUserAndGetId("Alice Johnson", "alice.johnson@example.com", insertStmt, selectStmt, userIds);
                insertUserAndGetId("Bob Smith", "bob.smith@example.com", insertStmt, selectStmt, userIds);
                insertUserAndGetId("Carol White", "carol.white@example.com", insertStmt, selectStmt, userIds);
            }

            String insertOrders = "INSERT INTO orders (user_id, product, amount) VALUES (?, ?, ?)";
            try (PreparedStatement pstmt = connection.prepareStatement(insertOrders)) {
                pstmt.setInt(1, userIds.get("alice.johnson@example.com"));
                pstmt.setString(2, "Smartphone");
                pstmt.setBigDecimal(3, new java.math.BigDecimal("800.00"));
                pstmt.executeUpdate();

                pstmt.setInt(1, userIds.get("bob.smith@example.com"));
                pstmt.setString(2, "Tablet");
                pstmt.setBigDecimal(3, new java.math.BigDecimal("300.00"));
                pstmt.executeUpdate();

                pstmt.setInt(1, userIds.get("carol.white@example.com"));
                pstmt.setString(2, "Headphones");
                pstmt.setBigDecimal(3, new java.math.BigDecimal("150.00"));
                pstmt.executeUpdate();

                pstmt.setInt(1, userIds.get("alice.johnson@example.com"));
                pstmt.setString(2, "Monitor");
                pstmt.setBigDecimal(3, new java.math.BigDecimal("200.00"));
                pstmt.executeUpdate();

                pstmt.setInt(1, userIds.get("bob.smith@example.com"));
                pstmt.setString(2, "Keyboard");
                pstmt.setBigDecimal(3, new java.math.BigDecimal("100.00"));
                pstmt.executeUpdate();
            }

            String selectUsers = "SELECT * FROM users";
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(selectUsers)) {
                while (rs.next()) {
                    System.out.println(rs.getInt("id") + " " + rs.getString("name") + " " + rs.getString("email"));
                }
            }

            String selectOrders = "SELECT * FROM orders";
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(selectOrders)) {
                while (rs.next()) {
                    System.out.println(rs.getInt("id") + " " + rs.getInt("user_id") + " " + rs.getString("product") + " " + rs.getBigDecimal("amount"));
                }
            }

            String updateUser = "UPDATE users SET email = ? WHERE id = ?";
            String checkEmailExists = "SELECT COUNT(*) FROM users WHERE email = ?";
            try (PreparedStatement pstmt = connection.prepareStatement(updateUser);
                 PreparedStatement checkStmt = connection.prepareStatement(checkEmailExists)) {

                updateUserEmail(userIds.get("alice.johnson@example.com"), "alice.new@example.com", pstmt, checkStmt);
                updateUserEmail(userIds.get("bob.smith@example.com"), "bob.new@example.com", pstmt, checkStmt);
            }

            String updateOrder = "UPDATE orders SET product = ?, amount = ? WHERE id = ?";
            try (PreparedStatement pstmt = connection.prepareStatement(updateOrder)) {
                pstmt.setString(1, "Gaming Laptop");
                pstmt.setBigDecimal(2, new java.math.BigDecimal("2500.00"));
                pstmt.setInt(3, 1);
                pstmt.executeUpdate();

                pstmt.setString(1, "E-Reader");
                pstmt.setBigDecimal(2, new java.math.BigDecimal("150.00"));
                pstmt.setInt(3, 3);
                pstmt.executeUpdate();
            }

            String deleteOrdersForUser = "DELETE FROM orders WHERE user_id = ?";
            try (PreparedStatement pstmt = connection.prepareStatement(deleteOrdersForUser)) {
                pstmt.setInt(1, userIds.get("carol.white@example.com"));
                pstmt.executeUpdate();
            }

            String deleteUser = "DELETE FROM users WHERE id = ?";
            try (PreparedStatement pstmt = connection.prepareStatement(deleteUser)) {
                pstmt.setInt(1, userIds.get("carol.white@example.com"));
                pstmt.executeUpdate();
            }

            executeJoinQueries(connection);

            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void insertUserAndGetId(String name, String email, PreparedStatement insertStmt, PreparedStatement selectStmt, Map<String, Integer> userIds) throws Exception {
        insertStmt.setString(1, name);
        insertStmt.setString(2, email);
        ResultSet rs = insertStmt.executeQuery();
        if (rs.next()) {
            userIds.put(email, rs.getInt("id"));
        } else {
            selectStmt.setString(1, email);
            try (ResultSet rs2 = selectStmt.executeQuery()) {
                if (rs2.next()) {
                    userIds.put(email, rs2.getInt("id"));
                }
            }
        }
    }

    private static void updateUserEmail(Integer userId, String newEmail, PreparedStatement updateStmt, PreparedStatement checkStmt) throws Exception {
        checkStmt.setString(1, newEmail);
        ResultSet rs = checkStmt.executeQuery();
        if (rs.next() && rs.getInt(1) == 0) {
            updateStmt.setString(1, newEmail);
            updateStmt.setInt(2, userId);
            updateStmt.executeUpdate();
        } else {
            System.out.println("Email " + newEmail + " already exists.");
        }
    }

    private static void executeJoinQueries(Connection connection) throws Exception {

        String joinQuery1 = "SELECT users.name, orders.product, orders.amount FROM orders JOIN users ON orders.user_id = users.id";
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(joinQuery1)) {
            while (rs.next()) {
                System.out.println(rs.getString("name") + " " + rs.getString("product") + " " + rs.getBigDecimal("amount"));
            }
        }

        String joinQuery2 = "SELECT users.name, SUM(orders.amount) as total_amount FROM orders JOIN users ON orders.user_id = users.id GROUP BY users.name";
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(joinQuery2)) {
            while (rs.next()) {
                System.out.println(rs.getString("name") + " " + rs.getBigDecimal("total_amount"));
            }
        }

        String joinQuery3 = "SELECT DISTINCT users.name, orders.product, orders.amount FROM orders JOIN users ON orders.user_id = users.id WHERE orders.amount > 500";
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(joinQuery3)) {
            while (rs.next()) {
                System.out.println(rs.getString("name") + " " + rs.getString("product") + " " + rs.getBigDecimal("amount"));
            }
        }

        String joinQuery4 = "SELECT users.id, users.name, users.email, orders.product, orders.amount FROM orders JOIN users ON orders.user_id = users.id";
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(joinQuery4)) {
            while (rs.next()) {
                System.out.println(rs.getInt("id") + " " + rs.getString("name") + " " + rs.getString("email") + " " + rs.getString("product") + " " + rs.getBigDecimal("amount"));
            }
        }
    }
}

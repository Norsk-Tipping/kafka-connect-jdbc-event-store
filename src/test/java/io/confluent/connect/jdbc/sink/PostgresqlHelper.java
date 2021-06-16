/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.sink;

import java.io.IOException;
import java.sql.*;
import java.util.HashSet;
import java.util.Properties;

public final class PostgresqlHelper {

  static {
    try {
      Class.forName("org.postgresql.Driver");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public interface ResultSetReadCallback {
    void read(final ResultSet rs) throws SQLException;
  }

  public final String dbPath;

  public Connection connection;

  public PostgresqlHelper(String testId) {
    dbPath = testId.toLowerCase();
  }

  public String postgreSQL() {
    return "jdbc:postgresql:" + dbPath;
  }

  public static final HashSet<String> tablesUsed = new HashSet<>();
  private static Properties props;

  public Connection connection() throws SQLException {
    return DriverManager.getConnection(("jdbc:postgresql:"+dbPath.toString().toLowerCase()), props);
  }
  public void setUp() throws SQLException, IOException {
    Properties props = new Properties();
    props.setProperty("user","postgres");
    props.setProperty("password","password123");
    props.setProperty("ssl","false");
    PostgresqlHelper.props = props;
    connection = DriverManager.getConnection(("jdbc:postgresql:"+"postgres"), props);
    try {
      Statement statement = connection.createStatement();
      StringBuilder sb = new StringBuilder();
      sb.append("CREATE DATABASE ");
      sb.append(dbPath);
      statement.executeUpdate(sb.toString());
      System.out.printf("Database %s created!\n", dbPath);
    } catch (SQLException sqlException) {
      if (sqlException.getSQLState().equals("42P04")) {
        // Database already exists error
        System.out.println(sqlException.getMessage());
      } else {
        throw sqlException;
      }
    }
    connection.close();
    connection = DriverManager.getConnection(("jdbc:postgresql:"+dbPath.toString().toLowerCase()), props);
    connection.setAutoCommit(false);
  }

  public void tearDown() throws SQLException, IOException {
    tablesUsed.forEach(table -> {
      try {
        System.out.format("Deleting table %s from database %s", table, dbPath);
        deleteTable(table);
      } catch (SQLException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    });
    /*connection.setAutoCommit(true);
    Statement statement = connection.createStatement();
    StringBuilder sb = new StringBuilder();
    sb.append("DROP DATABASE IF EXISTS ");
    sb.append(dbPath);
    statement.executeUpdate(sb.toString());
    System.out.printf("Database %s dropped!\n", dbPath);*/
    connection.close();
  }

  public void createTable(final String createSql) throws SQLException {
    execute(createSql);
  }

  public void deleteTable(final String table) throws SQLException {
    execute("DROP TABLE IF EXISTS " + table);

    //random errors of table not being available happens in the unit tests
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public int select(final String query, final PostgresqlHelper.ResultSetReadCallback callback) throws SQLException {
    int count = 0;
    try (Statement stmt = connection.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(query)) {
        while (rs.next()) {
          callback.read(rs);
          count++;
        }
      }
    }
    return count;
  }

  public void execute(String sql) throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      stmt.executeUpdate(sql);
      connection.commit();
    }
  }

}

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

package io.confluent.connect.jdbc.dialect;

import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.util.*;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.data.Schema.Type;
import org.junit.Test;

import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class PostgreSqlDatabaseDialectTest extends BaseDialectTest<PostgreSqlDatabaseDialect> {

  @Override
  protected PostgreSqlDatabaseDialect createDialect() {
    return new PostgreSqlDatabaseDialect(sinkConfigWithUrl("jdbc:postgresql://something",
            JdbcSinkConfig.DISTRIBUTIONATTRIBUTES, "c1",
            JdbcSinkConfig.ZONEMAPATTRIBUTES, "c6,c7",
            JdbcSinkConfig.CLUSTEREDATTRIBUTES, "c1, c2"));
  }

  @Test
  public void shouldMapPrimitiveSchemaTypeToSqlTypes() {
    assertPrimitiveMapping(Type.INT8, "SMALLINT");
    assertPrimitiveMapping(Type.INT16, "SMALLINT");
    assertPrimitiveMapping(Type.INT32, "INT");
    assertPrimitiveMapping(Type.INT64, "BIGINT");
    assertPrimitiveMapping(Type.FLOAT32, "REAL");
    assertPrimitiveMapping(Type.FLOAT64, "DOUBLE PRECISION");
    assertPrimitiveMapping(Type.BOOLEAN, "BOOLEAN");
    assertPrimitiveMapping(Type.BYTES, "BYTEA");
    assertPrimitiveMapping(Type.STRING, "TEXT");
  }

  @Test
  public void shouldMapDecimalSchemaTypeToDecimalSqlType() {
    assertDecimalMapping(0, "DECIMAL");
    assertDecimalMapping(3, "DECIMAL");
    assertDecimalMapping(4, "DECIMAL");
    assertDecimalMapping(5, "DECIMAL");
  }

  @Test
  public void shouldMapDataTypesForAddingColumnToTable() {
    verifyDataTypeMapping("SMALLINT", Schema.INT8_SCHEMA);
    verifyDataTypeMapping("SMALLINT", Schema.INT16_SCHEMA);
    verifyDataTypeMapping("INT", Schema.INT32_SCHEMA);
    verifyDataTypeMapping("BIGINT", Schema.INT64_SCHEMA);
    verifyDataTypeMapping("REAL", Schema.FLOAT32_SCHEMA);
    verifyDataTypeMapping("DOUBLE PRECISION", Schema.FLOAT64_SCHEMA);
    verifyDataTypeMapping("BOOLEAN", Schema.BOOLEAN_SCHEMA);
    verifyDataTypeMapping("TEXT", Schema.STRING_SCHEMA);
    verifyDataTypeMapping("BYTEA", Schema.BYTES_SCHEMA);
    verifyDataTypeMapping("DECIMAL", Decimal.schema(0));
    verifyDataTypeMapping("DATE", Date.SCHEMA);
    verifyDataTypeMapping("TIME", Time.SCHEMA);
    verifyDataTypeMapping("TIMESTAMP", Timestamp.SCHEMA);
    assertJsonMapping("JSONB", Schema.STRING_SCHEMA);
  }

  @Test
  public void shouldMapDateSchemaTypeToDateSqlType() {
    assertDateMapping("DATE");
  }

  @Test
  public void shouldMapTimeSchemaTypeToTimeSqlType() {
    assertTimeMapping("TIME");
  }

  @Test
  public void shouldMapTimestampSchemaTypeToTimestampSqlType() {
    assertTimestampMapping("TIMESTAMP");
  }

  @Test
  public void shouldBuildCreateQueryStatement() {
    assertEquals(
        "CREATE TABLE \"myTable\" (" + System.lineSeparator() +
                "\"c1\" INT NOT NULL," + System.lineSeparator() +
                "\"c2\" BIGINT NOT NULL," + System.lineSeparator() +
                "\"c3\" TEXT NOT NULL," + System.lineSeparator() +
                "\"c4\" TEXT NULL," + System.lineSeparator() +
                "\"c5\" DATE DEFAULT '2001-03-15'," + System.lineSeparator() +
                "\"c6\" TIME DEFAULT '00:00:00.000'," + System.lineSeparator() +
                "\"c7\" TIMESTAMP DEFAULT '2001-03-15 00:00:00.000'," + System.lineSeparator() +
                "\"c8\" DECIMAL NULL," + System.lineSeparator() +
                "\"c9\" BOOLEAN DEFAULT TRUE," + System.lineSeparator() +
                "\"event\" JSONB NOT NULL)" + System.lineSeparator() +
                System.lineSeparator() +
                "PARTITION BY HASH (\"c1\");" + System.lineSeparator() +
                "CREATE TABLE myTable_h0 PARTITION OF \"myTable\" FOR VALUES WITH (modulus 10, remainder 0);" + System.lineSeparator() +
                "CREATE TABLE myTable_h1 PARTITION OF \"myTable\" FOR VALUES WITH (modulus 10, remainder 1);" + System.lineSeparator() +
                "CREATE TABLE myTable_h2 PARTITION OF \"myTable\" FOR VALUES WITH (modulus 10, remainder 2);" + System.lineSeparator() +
                "CREATE TABLE myTable_h3 PARTITION OF \"myTable\" FOR VALUES WITH (modulus 10, remainder 3);" + System.lineSeparator() +
                "CREATE TABLE myTable_h4 PARTITION OF \"myTable\" FOR VALUES WITH (modulus 10, remainder 4);" + System.lineSeparator() +
                "CREATE TABLE myTable_h5 PARTITION OF \"myTable\" FOR VALUES WITH (modulus 10, remainder 5);" + System.lineSeparator() +
                "CREATE TABLE myTable_h6 PARTITION OF \"myTable\" FOR VALUES WITH (modulus 10, remainder 6);" + System.lineSeparator() +
                "CREATE TABLE myTable_h7 PARTITION OF \"myTable\" FOR VALUES WITH (modulus 10, remainder 7);" + System.lineSeparator() +
                "CREATE TABLE myTable_h8 PARTITION OF \"myTable\" FOR VALUES WITH (modulus 10, remainder 8);" + System.lineSeparator() +
                "CREATE TABLE myTable_h9 PARTITION OF \"myTable\" FOR VALUES WITH (modulus 10, remainder 9);" + System.lineSeparator() +
                "CREATE INDEX brin_myTable_h0_c6 ON myTable_h0 USING brin(c6);" + System.lineSeparator() +
                "CREATE INDEX brin_myTable_h0_c7 ON myTable_h0 USING brin(c7);" + System.lineSeparator() +
                "CREATE INDEX brin_myTable_h1_c6 ON myTable_h1 USING brin(c6);" + System.lineSeparator() +
                "CREATE INDEX brin_myTable_h1_c7 ON myTable_h1 USING brin(c7);" + System.lineSeparator() +
                "CREATE INDEX brin_myTable_h2_c6 ON myTable_h2 USING brin(c6);" + System.lineSeparator() +
                "CREATE INDEX brin_myTable_h2_c7 ON myTable_h2 USING brin(c7);" + System.lineSeparator() +
                "CREATE INDEX brin_myTable_h3_c6 ON myTable_h3 USING brin(c6);" + System.lineSeparator() +
                "CREATE INDEX brin_myTable_h3_c7 ON myTable_h3 USING brin(c7);" + System.lineSeparator() +
                "CREATE INDEX brin_myTable_h4_c6 ON myTable_h4 USING brin(c6);" + System.lineSeparator() +
                "CREATE INDEX brin_myTable_h4_c7 ON myTable_h4 USING brin(c7);" + System.lineSeparator() +
                "CREATE INDEX brin_myTable_h5_c6 ON myTable_h5 USING brin(c6);" + System.lineSeparator() +
                "CREATE INDEX brin_myTable_h5_c7 ON myTable_h5 USING brin(c7);" + System.lineSeparator() +
                "CREATE INDEX brin_myTable_h6_c6 ON myTable_h6 USING brin(c6);" + System.lineSeparator() +
                "CREATE INDEX brin_myTable_h6_c7 ON myTable_h6 USING brin(c7);" + System.lineSeparator() +
                "CREATE INDEX brin_myTable_h7_c6 ON myTable_h7 USING brin(c6);" + System.lineSeparator() +
                "CREATE INDEX brin_myTable_h7_c7 ON myTable_h7 USING brin(c7);" + System.lineSeparator() +
                "CREATE INDEX brin_myTable_h8_c6 ON myTable_h8 USING brin(c6);" + System.lineSeparator() +
                "CREATE INDEX brin_myTable_h8_c7 ON myTable_h8 USING brin(c7);" + System.lineSeparator() +
                "CREATE INDEX brin_myTable_h9_c6 ON myTable_h9 USING brin(c6);" + System.lineSeparator() +
                "CREATE INDEX brin_myTable_h9_c7 ON myTable_h9 USING brin(c7);;" + System.lineSeparator() +
                "CREATE EXTENSION IF NOT EXISTS bloom;" + System.lineSeparator() +
                "CREATE INDEX bloom_myTable_h0 ON myTable_h0 USING bloom(\"c1\",\"c2\");" + System.lineSeparator() +
                "CREATE INDEX bloom_myTable_h1 ON myTable_h1 USING bloom(\"c1\",\"c2\");" + System.lineSeparator() +
                "CREATE INDEX bloom_myTable_h2 ON myTable_h2 USING bloom(\"c1\",\"c2\");" + System.lineSeparator() +
                "CREATE INDEX bloom_myTable_h3 ON myTable_h3 USING bloom(\"c1\",\"c2\");" + System.lineSeparator() +
                "CREATE INDEX bloom_myTable_h4 ON myTable_h4 USING bloom(\"c1\",\"c2\");" + System.lineSeparator() +
                "CREATE INDEX bloom_myTable_h5 ON myTable_h5 USING bloom(\"c1\",\"c2\");" + System.lineSeparator() +
                "CREATE INDEX bloom_myTable_h6 ON myTable_h6 USING bloom(\"c1\",\"c2\");" + System.lineSeparator() +
                "CREATE INDEX bloom_myTable_h7 ON myTable_h7 USING bloom(\"c1\",\"c2\");" + System.lineSeparator() +
                "CREATE INDEX bloom_myTable_h8 ON myTable_h8 USING bloom(\"c1\",\"c2\");" + System.lineSeparator() +
                "CREATE INDEX bloom_myTable_h9 ON myTable_h9 USING bloom(\"c1\",\"c2\");",
        dialect.buildCreateTableStatement(tableId, sinkRecordFields)
    );

    quoteIdentfiiers = QuoteMethod.NEVER;
    dialect = createDialect();

    assertEquals(
            "CREATE TABLE myTable (" + System.lineSeparator() +
                    "c1 INT NOT NULL," + System.lineSeparator() +
                    "c2 BIGINT NOT NULL," + System.lineSeparator() +
                    "c3 TEXT NOT NULL," + System.lineSeparator() +
                    "c4 TEXT NULL," + System.lineSeparator() +
                    "c5 DATE DEFAULT '2001-03-15'," + System.lineSeparator() +
                    "c6 TIME DEFAULT '00:00:00.000'," + System.lineSeparator() +
                    "c7 TIMESTAMP DEFAULT '2001-03-15 00:00:00.000'," + System.lineSeparator() +
                    "c8 DECIMAL NULL," + System.lineSeparator() +
                    "c9 BOOLEAN DEFAULT TRUE," + System.lineSeparator() +
                    "event JSONB NOT NULL)" + System.lineSeparator() +
                    System.lineSeparator() +
                    "PARTITION BY HASH (c1);" + System.lineSeparator() +
                    "CREATE TABLE myTable_h0 PARTITION OF myTable FOR VALUES WITH (modulus 10, remainder 0);" + System.lineSeparator() +
                    "CREATE TABLE myTable_h1 PARTITION OF myTable FOR VALUES WITH (modulus 10, remainder 1);" + System.lineSeparator() +
                    "CREATE TABLE myTable_h2 PARTITION OF myTable FOR VALUES WITH (modulus 10, remainder 2);" + System.lineSeparator() +
                    "CREATE TABLE myTable_h3 PARTITION OF myTable FOR VALUES WITH (modulus 10, remainder 3);" + System.lineSeparator() +
                    "CREATE TABLE myTable_h4 PARTITION OF myTable FOR VALUES WITH (modulus 10, remainder 4);" + System.lineSeparator() +
                    "CREATE TABLE myTable_h5 PARTITION OF myTable FOR VALUES WITH (modulus 10, remainder 5);" + System.lineSeparator() +
                    "CREATE TABLE myTable_h6 PARTITION OF myTable FOR VALUES WITH (modulus 10, remainder 6);" + System.lineSeparator() +
                    "CREATE TABLE myTable_h7 PARTITION OF myTable FOR VALUES WITH (modulus 10, remainder 7);" + System.lineSeparator() +
                    "CREATE TABLE myTable_h8 PARTITION OF myTable FOR VALUES WITH (modulus 10, remainder 8);" + System.lineSeparator() +
                    "CREATE TABLE myTable_h9 PARTITION OF myTable FOR VALUES WITH (modulus 10, remainder 9);" + System.lineSeparator() +
                    "CREATE INDEX brin_myTable_h0_c6 ON myTable_h0 USING brin(c6);" + System.lineSeparator() +
                    "CREATE INDEX brin_myTable_h0_c7 ON myTable_h0 USING brin(c7);" + System.lineSeparator() +
                    "CREATE INDEX brin_myTable_h1_c6 ON myTable_h1 USING brin(c6);" + System.lineSeparator() +
                    "CREATE INDEX brin_myTable_h1_c7 ON myTable_h1 USING brin(c7);" + System.lineSeparator() +
                    "CREATE INDEX brin_myTable_h2_c6 ON myTable_h2 USING brin(c6);" + System.lineSeparator() +
                    "CREATE INDEX brin_myTable_h2_c7 ON myTable_h2 USING brin(c7);" + System.lineSeparator() +
                    "CREATE INDEX brin_myTable_h3_c6 ON myTable_h3 USING brin(c6);" + System.lineSeparator() +
                    "CREATE INDEX brin_myTable_h3_c7 ON myTable_h3 USING brin(c7);" + System.lineSeparator() +
                    "CREATE INDEX brin_myTable_h4_c6 ON myTable_h4 USING brin(c6);" + System.lineSeparator() +
                    "CREATE INDEX brin_myTable_h4_c7 ON myTable_h4 USING brin(c7);" + System.lineSeparator() +
                    "CREATE INDEX brin_myTable_h5_c6 ON myTable_h5 USING brin(c6);" + System.lineSeparator() +
                    "CREATE INDEX brin_myTable_h5_c7 ON myTable_h5 USING brin(c7);" + System.lineSeparator() +
                    "CREATE INDEX brin_myTable_h6_c6 ON myTable_h6 USING brin(c6);" + System.lineSeparator() +
                    "CREATE INDEX brin_myTable_h6_c7 ON myTable_h6 USING brin(c7);" + System.lineSeparator() +
                    "CREATE INDEX brin_myTable_h7_c6 ON myTable_h7 USING brin(c6);" + System.lineSeparator() +
                    "CREATE INDEX brin_myTable_h7_c7 ON myTable_h7 USING brin(c7);" + System.lineSeparator() +
                    "CREATE INDEX brin_myTable_h8_c6 ON myTable_h8 USING brin(c6);" + System.lineSeparator() +
                    "CREATE INDEX brin_myTable_h8_c7 ON myTable_h8 USING brin(c7);" + System.lineSeparator() +
                    "CREATE INDEX brin_myTable_h9_c6 ON myTable_h9 USING brin(c6);" + System.lineSeparator() +
                    "CREATE INDEX brin_myTable_h9_c7 ON myTable_h9 USING brin(c7);;" + System.lineSeparator() +
                    "CREATE EXTENSION IF NOT EXISTS bloom;" + System.lineSeparator() +
                    "CREATE INDEX bloom_myTable_h0 ON myTable_h0 USING bloom(c1,c2);" + System.lineSeparator() +
                    "CREATE INDEX bloom_myTable_h1 ON myTable_h1 USING bloom(c1,c2);" + System.lineSeparator() +
                    "CREATE INDEX bloom_myTable_h2 ON myTable_h2 USING bloom(c1,c2);" + System.lineSeparator() +
                    "CREATE INDEX bloom_myTable_h3 ON myTable_h3 USING bloom(c1,c2);" + System.lineSeparator() +
                    "CREATE INDEX bloom_myTable_h4 ON myTable_h4 USING bloom(c1,c2);" + System.lineSeparator() +
                    "CREATE INDEX bloom_myTable_h5 ON myTable_h5 USING bloom(c1,c2);" + System.lineSeparator() +
                    "CREATE INDEX bloom_myTable_h6 ON myTable_h6 USING bloom(c1,c2);" + System.lineSeparator() +
                    "CREATE INDEX bloom_myTable_h7 ON myTable_h7 USING bloom(c1,c2);" + System.lineSeparator() +
                    "CREATE INDEX bloom_myTable_h8 ON myTable_h8 USING bloom(c1,c2);" + System.lineSeparator() +
                    "CREATE INDEX bloom_myTable_h9 ON myTable_h9 USING bloom(c1,c2);",
        dialect.buildCreateTableStatement(tableId, sinkRecordFields)
    );
  }

  @Test
  public void shouldBuildAlterTableStatement() {
    assertEquals(
        Arrays.asList(
            "ALTER TABLE \"myTable\" " + System.lineSeparator() +
             "ADD \"c1\" INT NOT NULL," + System.lineSeparator() +
             "ADD \"c2\" BIGINT NOT NULL," + System.lineSeparator() +
             "ADD \"c3\" TEXT NOT NULL," + System.lineSeparator() +
             "ADD \"c4\" TEXT NULL," + System.lineSeparator() +
             "ADD \"c5\" DATE DEFAULT '2001-03-15'," + System.lineSeparator() +
             "ADD \"c6\" TIME DEFAULT '00:00:00.000'," + System.lineSeparator() +
             "ADD \"c7\" TIMESTAMP DEFAULT '2001-03-15 00:00:00.000'," + System.lineSeparator() +
             "ADD \"c8\" DECIMAL NULL," + System.lineSeparator() +
             "ADD \"c9\" BOOLEAN DEFAULT TRUE," + System.lineSeparator() +
             "ADD \"event\" JSONB NOT NULL"
        ),
        dialect.buildAlterTable(tableId, sinkRecordFields)
    );

    quoteIdentfiiers = QuoteMethod.NEVER;
    dialect = createDialect();

    assertEquals(
        Arrays.asList(
                "ALTER TABLE myTable " + System.lineSeparator() +
                "ADD c1 INT NOT NULL," + System.lineSeparator() +
                "ADD c2 BIGINT NOT NULL," + System.lineSeparator() +
                "ADD c3 TEXT NOT NULL," + System.lineSeparator() +
                "ADD c4 TEXT NULL," + System.lineSeparator() +
                "ADD c5 DATE DEFAULT '2001-03-15'," + System.lineSeparator() +
                "ADD c6 TIME DEFAULT '00:00:00.000'," + System.lineSeparator() +
                "ADD c7 TIMESTAMP DEFAULT '2001-03-15 00:00:00.000'," + System.lineSeparator() +
                "ADD c8 DECIMAL NULL," + System.lineSeparator() +
                "ADD c9 BOOLEAN DEFAULT TRUE," + System.lineSeparator() +
                "ADD event JSONB NOT NULL"
        ),
        dialect.buildAlterTable(tableId, sinkRecordFields)
    );
  }

  @Test
  public void shouldBuildInsertStatement() {
    TableDefinitionBuilder builder = new TableDefinitionBuilder().withTable("myTable");
    builder.withColumn("columnA").type("varchar", JDBCType.VARCHAR, String.class);
    builder.withColumn("columnB").type("varchar", JDBCType.VARCHAR, String.class);
    builder.withColumn("columnC").type("varchar", JDBCType.VARCHAR, String.class);
    builder.withColumn("columnD").type("jsonb", JDBCType.OTHER, String.class);
    TableDefinition tableDefn = builder.build();
    assertEquals(
        "INSERT INTO \"myTable\" (\"columnA\",\"columnB\"," +
        "\"columnC\",\"columnD\") VALUES (?,?,?,?::jsonb)",
        dialect.buildInsertStatement(tableId, columnsAtoD, tableDefn)
    );

    quoteIdentfiiers = QuoteMethod.NEVER;
    dialect = createDialect();

    assertEquals(
        "INSERT INTO myTable (columnA,columnB," +
        "columnC,columnD) VALUES (?,?,?,?::jsonb)",
        dialect.buildInsertStatement(tableId, columnsAtoD, tableDefn)
    );

    builder = new TableDefinitionBuilder().withTable("myTable");
    builder.withColumn("columnA").type("varchar", JDBCType.VARCHAR, Integer.class);
    builder.withColumn("uuidColumn").type("uuid", JDBCType.OTHER, UUID.class);
    builder.withColumn("dateColumn").type("date", JDBCType.DATE, java.sql.Date.class);
    builder.withColumn("event").type("jsonb", JDBCType.OTHER, String.class);
    tableDefn = builder.build();
    List<ColumnId> nonPkColumns = new ArrayList<>();
    nonPkColumns.add(new ColumnId(tableId, "columnA"));
    nonPkColumns.add(new ColumnId(tableId, "uuidColumn"));
    nonPkColumns.add(new ColumnId(tableId, "dateColumn"));
    nonPkColumns.add(new ColumnId(tableId, "event"));
    assertEquals(
        "INSERT INTO myTable (" +
        "columnA,uuidColumn,dateColumn,event" +
        ") VALUES (?,?::uuid,?,?::jsonb)",
        dialect.buildInsertStatement(tableId, nonPkColumns, tableDefn)
    );
  }

  @Test
  public void shouldComputeValueTypeCast() {
    TableDefinitionBuilder builder = new TableDefinitionBuilder().withTable("myTable");
    builder.withColumn("id1").type("int", JDBCType.INTEGER, Integer.class);
    builder.withColumn("id2").type("int", JDBCType.INTEGER, Integer.class);
    builder.withColumn("columnA").type("varchar", JDBCType.VARCHAR, Integer.class);
    builder.withColumn("uuidColumn").type("uuid", JDBCType.OTHER, UUID.class);
    builder.withColumn("dateColumn").type("date", JDBCType.DATE, java.sql.Date.class);
    TableDefinition tableDefn = builder.build();
    ColumnId uuidColumn = tableDefn.definitionForColumn("uuidColumn").id();
    ColumnId dateColumn = tableDefn.definitionForColumn("dateColumn").id();
    assertEquals("", dialect.valueTypeCast(tableDefn, columnPK1));
    assertEquals("", dialect.valueTypeCast(tableDefn, columnPK2));
    assertEquals("", dialect.valueTypeCast(tableDefn, columnA));
    assertEquals("::uuid", dialect.valueTypeCast(tableDefn, uuidColumn));
    assertEquals("", dialect.valueTypeCast(tableDefn, dateColumn));
  }


  @Test
  public void alterAddOneCol() {
    verifyAlterAddOneCol("ALTER TABLE \"myTable\" ADD \"newcol1\" INT NULL");
  }

  @Test
  public void alterAddTwoCol() {
    verifyAlterAddTwoCols(
        "ALTER TABLE \"myTable\" " + System.lineSeparator() + "ADD \"newcol1\" INT NULL," +
        System.lineSeparator() + "ADD \"newcol2\" INT DEFAULT 42");
  }

  @Test
  public void shouldSanitizeUrlWithoutCredentialsInProperties() {
    assertSanitizedUrl(
        "jdbc:postgresql://localhost/test?user=fred&ssl=true",
        "jdbc:postgresql://localhost/test?user=fred&ssl=true"
    );
  }

  @Test
  public void shouldSanitizeUrlWithCredentialsInUrlProperties() {
    assertSanitizedUrl(
        "jdbc:postgresql://localhost/test?user=fred&password=secret&ssl=true",
        "jdbc:postgresql://localhost/test?user=fred&password=****&ssl=true"
    );
  }
}
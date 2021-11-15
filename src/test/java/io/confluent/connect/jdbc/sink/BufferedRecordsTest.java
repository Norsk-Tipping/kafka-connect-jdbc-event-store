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

import io.confluent.connect.jdbc.dialect.BulkLoadPreparedStatement;
import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialects;
import io.confluent.connect.jdbc.dialect.PostgreSqlDatabaseDialect;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.util.ColumnDefinition;
import io.confluent.connect.jdbc.util.TableDefinition;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BufferedRecordsTest {

  private final PostgresqlHelper postgresqlHelper = new PostgresqlHelper(getClass().getSimpleName());

  private Map<Object, Object> props;

  @Before
  public void setUp() throws IOException, SQLException {
    postgresqlHelper.setUp();
    props = new HashMap<>();
    props.put("name", "my-connector");
    props.put("connection.url", postgresqlHelper.postgreSQL());
    props.put(JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG, "postgres");
    props.put(JdbcSourceConnectorConfig.CONNECTION_PASSWORD_CONFIG, "password123");
    props.put("batch.size", 1000); // sufficiently high to not cause flushes due to buffer being full
    // We don't manually create the table, so let the connector do it
    props.put("auto.create", true);
    // We use various schemas, so let the connector add missing columns
    props.put("auto.evolve", true);
    props.put("input.format", "json");
    props.put("uppercase", "false");

  }

  @After
  public void tearDown() throws IOException, SQLException {
    postgresqlHelper.tearDown();
  }

  @Test
  public void correctBatching() throws SQLException {
    final JdbcSinkConfig config = new JdbcSinkConfig(props);

    final String url = postgresqlHelper.postgreSQL();
    final DatabaseDialect dbDialect = DatabaseDialects.findBestFor(url, config);
    final DbStructure dbStructure = new DbStructure(dbDialect);

    final TableId tableId = new TableId(null, null, "dummy");
    final BufferedRecords buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, postgresqlHelper.connection);

    final Schema schemaA = SchemaBuilder.struct().version(1)
        .field("event", Schema.STRING_SCHEMA)
        .field("age", Schema.OPTIONAL_INT32_SCHEMA)
        .build();
    final Struct valueA = new Struct(schemaA)
        .put("event", "{\"key\":\"cuba\"}");
    final SinkRecord recordA = new SinkRecord("dummy", 0, null, null, schemaA, valueA, 0);

    final Schema schemaB = SchemaBuilder.struct().version(2)
        .field("event", Schema.STRING_SCHEMA)
        .field("age", Schema.OPTIONAL_INT32_SCHEMA)
        .build();
    final Struct valueB = new Struct(schemaB)
        .put("event", "{\"key\":\"cuba\"}")
        .put("age", 4);
    final SinkRecord recordB = new SinkRecord("dummy", 1, null, null, schemaB, valueB, 1);

    // test records are batched correctly based on schema equality as records are added
    //   (schemaA,schemaA,schemaA,schemaB,schemaA) -> ([schemaA,schemaA,schemaA],[schemaB],[schemaA])

    assertEquals(Collections.emptyList(), buffer.add(recordA));
    assertEquals(Collections.emptyList(), buffer.add(recordA));
    assertEquals(Collections.emptyList(), buffer.add(recordA));

    assertEquals(Arrays.asList(recordA, recordA, recordA), buffer.add(recordB));

    assertEquals(Collections.singletonList(recordB), buffer.add(recordA));

    assertEquals(Collections.singletonList(recordA), buffer.flush());
  }

  @Test
  public void insertThenDeleteInBatchNoFlush() throws SQLException {
    props.put("delete.enabled", true);
    props.put("insert.mode", "insert");
    props.put("delete.keys", "id");
    final JdbcSinkConfig config = new JdbcSinkConfig(props);

    final String url = postgresqlHelper.postgreSQL();
    final DatabaseDialect dbDialect = DatabaseDialects.findBestFor(url, config);
    final DbStructure dbStructure = new DbStructure(dbDialect);

    final TableId tableId = new TableId(null, null, "dummy");
    final BufferedRecords buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, postgresqlHelper.connection);

    final Schema keySchemaA = SchemaBuilder.struct()
        .field("id", Schema.STRING_SCHEMA)
        .build();
    final Schema valueSchemaA = SchemaBuilder.struct()
        .field("event", Schema.STRING_SCHEMA)
        .field("id", Schema.STRING_SCHEMA)
        .build();
    final Struct keyA = new Struct(keySchemaA)
        .put("id", "1234L");
    final Struct valueA = new Struct(valueSchemaA)
        .put("id", "1234L")
        .put("event", "{\"key\":\"cuba\"}");

    final SinkRecord recordA = new SinkRecord("dummy", 0, keySchemaA, keyA, valueSchemaA, valueA, 0);
    final SinkRecord recordADelete = new SinkRecord("dummy", 0, keySchemaA, keyA, null, null, 0);

    final Schema schemaB = SchemaBuilder.struct()
        .field("event", Schema.STRING_SCHEMA)
        .field("id", Schema.STRING_SCHEMA)
        .build();
    final Struct valueB = new Struct(schemaB)
        .put("event", "{\"key\":\"cuba2\"}")
        .put("id", "1234L");
    final SinkRecord recordB = new SinkRecord("dummy", 1, keySchemaA, keyA, schemaB, valueB, 1);

    // test records are batched correctly based on schema equality as records are added
    //   (schemaA,schemaA,schemaA,schemaB,schemaA) -> ([schemaA,schemaA,schemaA],[schemaB],[schemaA])

    assertEquals(Collections.emptyList(), buffer.add(recordA));
    assertEquals(Collections.emptyList(), buffer.add(recordA));

    // delete should not cause a flush (i.e. not treated as a schema change)
    assertEquals(Collections.emptyList(), buffer.add(recordADelete));

    // schema change should trigger flush
    assertEquals(Arrays.asList(recordA, recordA, recordADelete), buffer.add(recordB));

    assertEquals(Collections.emptyList(), buffer.add(recordA));

    assertEquals(Arrays.asList(recordB, recordA), buffer.flush());
  }

  @Test
  public void insertThenTwoDeletesWithSchemaInBatchNoFlush() throws SQLException {
	    props.put("delete.enabled", true);
	    props.put("insert.mode", "insert");
	    props.put("delete.keys", "id");
	    final JdbcSinkConfig config = new JdbcSinkConfig(props);

	    final String url = postgresqlHelper.postgreSQL();
	    final DatabaseDialect dbDialect = DatabaseDialects.findBestFor(url, config);
	    final DbStructure dbStructure = new DbStructure(dbDialect);

	    final TableId tableId = new TableId(null, null, "dummy");
	    final BufferedRecords buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, postgresqlHelper.connection);

	    final Schema keySchemaA = SchemaBuilder.struct()
            .field("id", Schema.STRING_SCHEMA)
	        .build();
	    final Schema valueSchemaA = SchemaBuilder.struct().version(0)
            .field("id", Schema.STRING_SCHEMA)
            .field("age", Schema.OPTIONAL_INT32_SCHEMA)
	        .field("event", Schema.STRING_SCHEMA)
	        .build();
	    final Struct keyA = new Struct(keySchemaA)
	        .put("id", "1234L");
	    final Struct valueA = new Struct(valueSchemaA)
            .put("id", "1234L")
	        .put("event", "{\"key\":\"cuba\"}");
	    final SinkRecord recordA = new SinkRecord("dummy", 0, keySchemaA, keyA, valueSchemaA, valueA, 0);
	    final SinkRecord recordADeleteWithSchema = new SinkRecord("dummy", 0, keySchemaA, keyA, valueSchemaA, null, 0);
	    final SinkRecord recordADelete = new SinkRecord("dummy", 0, keySchemaA, keyA, null, null, 0);

	    final Schema schemaB = SchemaBuilder.struct().version(1)
            .field("id", Schema.STRING_SCHEMA)
            .field("age", Schema.OPTIONAL_INT32_SCHEMA)
            .field("event", Schema.STRING_SCHEMA)
            .build();
	    final Struct valueB = new Struct(schemaB)
            .put("id", "1234L")
	        .put("event", "{\"key\":\"cuba\"}")
	        .put("age", 4);
	    final SinkRecord recordB = new SinkRecord("dummy", 1, keySchemaA, keyA, schemaB, valueB, 1);

	    // test records are batched correctly based on schema equality as records are added
	    //   (schemaA,schemaA,schemaA,schemaB,schemaA) -> ([schemaA,schemaA,schemaA],[schemaB],[schemaA])

	    assertEquals(Collections.emptyList(), buffer.add(recordA));
	    assertEquals(Collections.emptyList(), buffer.add(recordA));

	    // delete should not cause a flush (i.e. not treated as a schema change)
	    assertEquals(Collections.emptyList(), buffer.add(recordADeleteWithSchema));

	    // delete should not cause a flush (i.e. not treated as a schema change)
	    assertEquals(Collections.emptyList(), buffer.add(recordADelete));
	    
	    // schema change and/or previous deletes should trigger flush
	    assertEquals(Arrays.asList(recordA, recordA, recordADeleteWithSchema, recordADelete), buffer.add(recordB));

	    // second schema change should trigger flush
	    assertEquals(Collections.singletonList(recordB), buffer.add(recordA));

	    assertEquals(Collections.singletonList(recordA), buffer.flush());
  }
  
  @Test
  public void insertThenDeleteThenInsertInBatchFlush() throws SQLException {
    props.put("delete.enabled", true);
    props.put("insert.mode", "insert");
    props.put("delete.keys", "id");
    final JdbcSinkConfig config = new JdbcSinkConfig(props);

    final String url = postgresqlHelper.postgreSQL();
    final DatabaseDialect dbDialect = DatabaseDialects.findBestFor(url, config);
    final DbStructure dbStructure = new DbStructure(dbDialect);

    final TableId tableId = new TableId(null, null, "dummy");
    final BufferedRecords buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, postgresqlHelper.connection);

    final Schema keySchemaA = SchemaBuilder.struct()
        .field("id", Schema.STRING_SCHEMA)
        .build();
    final Schema valueSchemaA = SchemaBuilder.struct().version(1)
        .field("id", Schema.STRING_SCHEMA)
        .field("event", Schema.STRING_SCHEMA)
        .field("age", Schema.OPTIONAL_INT32_SCHEMA)
        .build();
    final Struct keyA = new Struct(keySchemaA)
        .put("id", "1234L");
    final Struct valueA = new Struct(valueSchemaA)
        .put("id", "1234L")
        .put("event", "{\"key:\": \"cuba\"}");
    final SinkRecord recordA = new SinkRecord("dummy", 0, keySchemaA, keyA, valueSchemaA, valueA, 0);
    final SinkRecord recordADelete = new SinkRecord("dummy", 0, keySchemaA, keyA, null, null, 0);

    final Schema schemaB = SchemaBuilder.struct().version(2)
        .field("id", Schema.STRING_SCHEMA)
        .field("event", Schema.STRING_SCHEMA)
        .field("age", Schema.OPTIONAL_INT32_SCHEMA)
        .build();
    final Struct valueB = new Struct(schemaB)
        .put("id", "1234L")
        .put("event", "{\"key\":\"cuba\"}")
        .put("age", 4);
    final SinkRecord recordB = new SinkRecord("dummy", 1, keySchemaA, keyA, schemaB, valueB, 1);

    assertEquals(Collections.emptyList(), buffer.add(recordA));
    assertEquals(Collections.emptyList(), buffer.add(recordA));

    // delete should not cause a flush (i.e. not treated as a schema change)
    assertEquals(Collections.emptyList(), buffer.add(recordADelete));

    // insert after delete should flush to insure insert isn't lost in batching
    assertEquals(Arrays.asList(recordA, recordA, recordADelete), buffer.add(recordA));

    // schema change should trigger flush
    assertEquals(Collections.singletonList(recordA), buffer.add(recordB));

    // second schema change should trigger flush
    assertEquals(Collections.singletonList(recordB), buffer.add(recordA));

    assertEquals(Collections.singletonList(recordA), buffer.flush());
  }

  @Test
  public void insertThenDeleteWithSchemaThenInsertInBatchFlush() throws SQLException {
	    props.put("delete.enabled", true);
	    props.put("insert.mode", "upsert");
        props.put("delete.keys", "id");
        props.put("upsert.keys", "id");
	    final JdbcSinkConfig config = new JdbcSinkConfig(props);

	    final String url = postgresqlHelper.postgreSQL();
	    final DatabaseDialect dbDialect = DatabaseDialects.findBestFor(url, config);
	    final DbStructure dbStructure = new DbStructure(dbDialect);

	    final TableId tableId = new TableId(null, null, "dummy");
	    final BufferedRecords buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, postgresqlHelper.connection);

	    final Schema keySchemaA = SchemaBuilder.struct()
            .field("id", Schema.STRING_SCHEMA)
	        .build();
	    final Schema valueSchemaA = SchemaBuilder.struct().version(1)
            .field("id", Schema.STRING_SCHEMA)
	        .field("event", Schema.STRING_SCHEMA)
            .field("age", Schema.OPTIONAL_INT32_SCHEMA)
	        .build();
	    final Struct keyA = new Struct(keySchemaA)
	        .put("id", "1234L");
	    final Struct valueA = new Struct(valueSchemaA)
            .put("id", "1234L")
	        .put("event", "{\"key\":\"cuba\"}");
	    final SinkRecord recordA = new SinkRecord("dummy", 0, keySchemaA, keyA, valueSchemaA, valueA, 0);
	    final SinkRecord recordADeleteWithSchema = new SinkRecord("dummy", 0, keySchemaA, keyA, valueSchemaA, null, 0);

	    final Schema schemaB = SchemaBuilder.struct().version(2)
            .field("id", Schema.STRING_SCHEMA)
	        .field("event", Schema.STRING_SCHEMA)
	        .field("age", Schema.OPTIONAL_INT32_SCHEMA)
	        .build();
	    final Struct valueB = new Struct(schemaB)
            .put("id", "1234L")
	        .put("event", "{\"key\":\"cuba\"}")
	        .put("age", 4);
	    final SinkRecord recordB = new SinkRecord("dummy", 1, keySchemaA, keyA, schemaB, valueB, 1);

	    assertEquals(Collections.emptyList(), buffer.add(recordA));
	    assertEquals(Collections.emptyList(), buffer.add(recordA));

	    // delete should not cause a flush (i.e. not treated as a schema change)
	    assertEquals(Collections.emptyList(), buffer.add(recordADeleteWithSchema));

	    // insert after delete should flush to insure insert isn't lost in batching
	    assertEquals(Arrays.asList(recordA, recordA, recordADeleteWithSchema), buffer.add(recordA));

	    // schema change should trigger flush
	    assertEquals(Collections.singletonList(recordA), buffer.add(recordB));

	    // second schema change should trigger flush
	    assertEquals(Collections.singletonList(recordB), buffer.add(recordA));

	    assertEquals(Collections.singletonList(recordA), buffer.flush());
  }
  
  @Test
  public void testMultipleDeletesBatchedTogether() throws SQLException {
    props.put("delete.enabled", true);
    props.put("insert.mode", "upsert");
    props.put("delete.keys", "id");
    props.put("upsert.keys", "id");
    final JdbcSinkConfig config = new JdbcSinkConfig(props);

    final String url = postgresqlHelper.postgreSQL();
    final DatabaseDialect dbDialect = DatabaseDialects.findBestFor(url, config);
    final DbStructure dbStructure = new DbStructure(dbDialect);

    final TableId tableId = new TableId(null, null, "dummy");
    final BufferedRecords buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, postgresqlHelper.connection);

    final Schema keySchemaA = SchemaBuilder.struct()
        .field("id", Schema.STRING_SCHEMA)
        .build();
    final Schema valueSchemaA = SchemaBuilder.struct()
        .field("id", Schema.STRING_SCHEMA)
        .field("event", Schema.STRING_SCHEMA)
        .build();
    final Struct keyA = new Struct(keySchemaA)
        .put("id", "1234L");
    final Struct valueA = new Struct(valueSchemaA)
        .put("id", "1234L")
        .put("event", "{\"key\":\"cuba\"}");
    final SinkRecord recordA = new SinkRecord("dummy", 0, keySchemaA, keyA, valueSchemaA, valueA, 0);
    final SinkRecord recordADelete = new SinkRecord("dummy", 0, keySchemaA, keyA, null, null, 0);

    final Schema schemaB = SchemaBuilder.struct()
        .field("id", Schema.STRING_SCHEMA)
        .field("event", Schema.STRING_SCHEMA)
        .field("age", Schema.OPTIONAL_INT32_SCHEMA)
        .build();
    final Struct valueB = new Struct(schemaB)
        .put("id", "1234L")
        .put("event", "{\"key\":\"cuba\"}")
        .put("age", 4);
    final SinkRecord recordB = new SinkRecord("dummy", 1, keySchemaA, keyA, schemaB, valueB, 1);
    final SinkRecord recordBDelete = new SinkRecord("dummy", 1, keySchemaA, keyA, null, null, 1);

    assertEquals(Collections.emptyList(), buffer.add(recordA));

    // schema change should trigger flush
    assertEquals(Collections.singletonList(recordA), buffer.add(recordB));

    // deletes should not cause a flush (i.e. not treated as a schema change)
    assertEquals(Collections.emptyList(), buffer.add(recordADelete));
    assertEquals(Collections.emptyList(), buffer.add(recordBDelete));

    // insert after delete should flush to insure insert isn't lost in batching
    assertEquals(Arrays.asList(recordB, recordADelete, recordBDelete), buffer.add(recordB));

    assertEquals(Collections.singletonList(recordB), buffer.flush());
  }

  @Test
  public void testMultipleDeletesWithSchemaBatchedTogether() throws SQLException {
	    props.put("delete.enabled", true);
	    props.put("insert.mode", "insert");
        props.put("delete.keys", "id");
        props.put("upsert.keys", "id");
	    final JdbcSinkConfig config = new JdbcSinkConfig(props);

	    final String url = postgresqlHelper.postgreSQL();
	    final DatabaseDialect dbDialect = DatabaseDialects.findBestFor(url, config);
	    final DbStructure dbStructure = new DbStructure(dbDialect);

	    final TableId tableId = new TableId(null, null, "dummy");
	    final BufferedRecords buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, postgresqlHelper.connection);

	    final Schema keySchemaA = SchemaBuilder.struct()
            .field("id", Schema.STRING_SCHEMA)
	        .build();
	    final Schema valueSchemaA = SchemaBuilder.struct()
            .field("id", Schema.STRING_SCHEMA)
	        .field("event", Schema.STRING_SCHEMA)
	        .build();
	    final Struct keyA = new Struct(keySchemaA)
	        .put("id", "1234L");
	    final Struct valueA = new Struct(valueSchemaA)
            .put("id", "1234L")
	        .put("event", "{\"key\":\"cuba\"}");
	    final SinkRecord recordA = new SinkRecord("dummy", 0, keySchemaA, keyA, valueSchemaA, valueA, 0);
	    final SinkRecord recordADeleteWithSchema = new SinkRecord("dummy", 0, keySchemaA, keyA, valueSchemaA, null, 0);

	    final Schema schemaB = SchemaBuilder.struct()
            .field("id", Schema.STRING_SCHEMA)
	        .field("event", Schema.STRING_SCHEMA)
	        .field("age", Schema.OPTIONAL_INT32_SCHEMA)
	        .build();
	    final Struct valueB = new Struct(schemaB)
            .put("id", "1234L")
	        .put("event", "{\"key\":\"cuba\"}")
	        .put("age", 4);
	    final SinkRecord recordB = new SinkRecord("dummy", 1, keySchemaA, keyA, schemaB, valueB, 1);
	    final SinkRecord recordBDeleteWithSchema = new SinkRecord("dummy", 1, keySchemaA, keyA, schemaB, null, 1);

	    assertEquals(Collections.emptyList(), buffer.add(recordA));

	    // schema change should trigger flush
	    assertEquals(Collections.singletonList(recordA), buffer.add(recordB));

	    // schema change should trigger flush
	    assertEquals(Collections.singletonList(recordB), buffer.add(recordADeleteWithSchema));
	    
	    // schema change should trigger flush
	    assertEquals(Collections.singletonList(recordADeleteWithSchema), buffer.add(recordBDeleteWithSchema));

	    // insert after delete should flush to insure insert isn't lost in batching
	    assertEquals(Collections.singletonList(recordBDeleteWithSchema), buffer.add(recordB));

	    assertEquals(Collections.singletonList(recordB), buffer.flush());
  }
  
  @Test
  public void testFlushSuccessNoInfo() throws SQLException, IOException {
    final String url = postgresqlHelper.postgreSQL();
    final JdbcSinkConfig config = new JdbcSinkConfig(props);

    final DatabaseDialect dbDialect = DatabaseDialects.findBestFor(url, config);

    int[] batchResponse = new int[2];
    batchResponse[0] = Statement.SUCCESS_NO_INFO;
    batchResponse[1] = Statement.SUCCESS_NO_INFO;

    final ColumnDefinition colDefMock = mock(ColumnDefinition.class);
    when(colDefMock.type()).thenReturn(Types.VARCHAR);
    final TableDefinition tabDefMock = mock(TableDefinition.class);
    when(tabDefMock.definitionForColumn("event")).thenReturn(colDefMock);


    final DbStructure dbStructureMock = mock(DbStructure.class);
    when(dbStructureMock.createOrAmendIfNecessary(Matchers.any(JdbcSinkConfig.class),
                                                  Matchers.any(Connection.class),
                                                  Matchers.any(TableId.class),
                                                  Matchers.any(FieldsMetadata.class)))
        .thenReturn(true);
    when(dbStructureMock.tableDefinition(any(), any())).thenReturn(tabDefMock);

    PreparedStatement preparedStatementMock = mock(BulkLoadPreparedStatement.class);
    when(preparedStatementMock.executeBatch()).thenReturn(batchResponse);

    Connection connectionMock = mock(Connection.class);
    BaseConnection baseConnectionMock = mock(BaseConnection.class);
    when(connectionMock.unwrap(BaseConnection.class)).thenReturn(baseConnectionMock);
    CopyManager copyManagerMock = mock(CopyManager.class);
    when(copyManagerMock.copyIn(Matchers.anyString(), Matchers.any(InputStream.class))).thenReturn(2L);
    when(baseConnectionMock.getCopyAPI()).thenReturn(copyManagerMock);
    when(connectionMock.prepareStatement(Matchers.anyString())).thenReturn(preparedStatementMock);

    final TableId tableId = new TableId(null, null, "dummy");
    final BufferedRecords buffer = new BufferedRecords(config, tableId, dbDialect,
                                                       dbStructureMock, connectionMock);

    final Schema schemaA = SchemaBuilder.struct().field("event", Schema.STRING_SCHEMA).build();
    final Struct valueA = new Struct(schemaA).put("event", "{\"key\":\"cuba\"}");
    final SinkRecord recordA = new SinkRecord("dummy", 0, null, null, schemaA, valueA, 0);
    buffer.add(recordA);

    final Schema schemaB = SchemaBuilder.struct().field("event", Schema.STRING_SCHEMA).build();
    final Struct valueB = new Struct(schemaA).put("event", "{\"key\":\"cuba\"}");
    final SinkRecord recordB = new SinkRecord("dummy", 0, null, null, schemaB, valueB, 0);
    buffer.add(recordB);
    buffer.flush();

  }


  @Test
  public void testInsertModeUpdate() throws SQLException {
    final String url = postgresqlHelper.postgreSQL();
    props.put("insert.mode", "update");
    props.put("upsert.keys", "id");
    final JdbcSinkConfig config = new JdbcSinkConfig(props);

    final DatabaseDialect dbDialect = DatabaseDialects.findBestFor(url, config);
    assertTrue(dbDialect instanceof PostgreSqlDatabaseDialect);
    final DbStructure dbStructureMock = mock(DbStructure.class);
    when(dbStructureMock.createOrAmendIfNecessary(Matchers.any(JdbcSinkConfig.class),
                                                  Matchers.any(Connection.class),
                                                  Matchers.any(TableId.class),
                                                  Matchers.any(FieldsMetadata.class)))
        .thenReturn(true);

    final Connection connectionMock = mock(Connection.class);
    final PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
    when(connectionMock.prepareStatement(Matchers.anyString())).thenReturn(preparedStatementMock);
    final TableId tableId = new TableId(null, null, "dummy");
    final BufferedRecords buffer = new BufferedRecords(config, tableId, dbDialect, dbStructureMock,
            connectionMock);

    final Schema schemaA = SchemaBuilder.struct().field("id", Schema.STRING_SCHEMA)
            .field("event", Schema.STRING_SCHEMA).build();
    final Struct valueA = new Struct(schemaA).put("event", "{\"key\":\"cuba\"}");
    final SinkRecord recordA = new SinkRecord("dummy", 0, null, null, schemaA, valueA, 0);
    buffer.add(recordA);

    // Even though we're using the SQLite dialect, which uses backtick as the default quote
    // character, the SQLite JDBC driver does return double quote as the quote characters.
    Mockito.verify(
        connectionMock,
        Mockito.times(1)
    ).prepareStatement(Matchers.eq("update \"dummy\" set \"id\" = ?, \"event\" = ? where \"id\" = ?"));

  }

  @Test
  public void testAddRecordDeleteNotEnabledAndNonePkMode() throws SQLException {
    props.put("pk.mode", "none");

    // Delete is not enabled, so therefore require non-null value and value schema,
    // but any combination of key and key schema works
    assertValidRecord(true, true, true, true);
    assertValidRecord(false, true, true, true);
    assertValidRecord(true, false, true, true);
    assertValidRecord(false, false, true, true);

    // Fail when null value
    assertInvalidRecord(false, false, false, false, "with a null value and null value schema");
    assertInvalidRecord(true, false, false, false, "with a null value and null value schema");
    assertInvalidRecord(false, true, false, false, "with a null value and null value schema");
    assertInvalidRecord(true, true, false, false, "with a null value and null value schema");
    assertInvalidRecord(false, false, true, false, "with a null value and Struct value schema");
    assertInvalidRecord(true, false, true, false, "with a null value and Struct value schema");
    assertInvalidRecord(false, true, true, false, "with a null value and Struct value schema");
    assertInvalidRecord(true, true, true, false, "with a null value and Struct value schema");

    // Fail when null value schema but non-null value
    assertInvalidRecord(false, false, false, true, "with a Struct value and null value schema");
    assertInvalidRecord(true, false, false, true, "with a Struct value and null value schema");
    assertInvalidRecord(false, true, false, true, "with a Struct value and null value schema");
    assertInvalidRecord(true, true, false, true, "with a Struct value and null value schema");
  }


  @Test
  public void testAddRecordDeleteNotEnabledAndRecordValuePkMode() throws SQLException {

    // Delete is not enabled, so therefore require non-null value and value schema,
    // but any combination of key and key schema works
    assertValidRecord(true, true, true, true);
    assertValidRecord(false, true, true, true);
    assertValidRecord(true, false, true, true);
    assertValidRecord(false, false, true, true);

    // Fail when null value and null value schema
    assertInvalidRecord(true, true, false, false, "with a null value and null value schema");
    assertInvalidRecord(true, false, false, false, "with a null value and null value schema");
    assertInvalidRecord(false, true, false, false, "with a null value and null value schema");
    assertInvalidRecord(false, false, false, false, "with a null value and null value schema");

    // Fail when null value and non-null value schema
    assertInvalidRecord(true, true, true, false, "with a null value and Struct value schema");
    assertInvalidRecord(true, false, true, false, "with a null value and Struct value schema");
    assertInvalidRecord(false, true, true, false, "with a null value and Struct value schema");
    assertInvalidRecord(false, false, true, false, "with a null value and Struct value schema");

    // Fail when non-null value and null value schema
    assertInvalidRecord(true, true, false, true, "with a Struct value and null value schema");
    assertInvalidRecord(true, false, false, true, "with a Struct value and null value schema");
    assertInvalidRecord(false, true, false, true, "with a Struct value and null value schema");
    assertInvalidRecord(false, false, false, true, "with a Struct value and null value schema");
  }

  @Test
  public void testAddRecordDeleteNotEnabledAndKafkaPkMode() throws SQLException {

    // Delete is not enabled, so therefore allow all combinations of
    // null and non-null key, key schema, value, and value schema
    assertValidRecord(true, true, true, true);
    assertValidRecord(false, true, true, true);
    assertValidRecord(true, false, true, true);
    assertValidRecord(false, false, true, true);

    assertInvalidRecord(true, true, true, false, "with a non-null Struct value and non-null Struct schema");
    assertInvalidRecord(false, true, true, false, "with a non-null Struct value and non-null Struct schema");
    assertInvalidRecord(true, false, true, false, "with a non-null Struct value and non-null Struct schema");
    assertInvalidRecord(false, false, true, false, "with a non-null Struct value and non-null Struct schema");

    assertInvalidRecord(true, true, false, true, "with a non-null Struct value and non-null Struct schema");
    assertInvalidRecord(false, true, false, true, "with a non-null Struct value and non-null Struct schema");
    assertInvalidRecord(true, false, false, true, "with a non-null Struct value and non-null Struct schema");
    assertInvalidRecord(false, false, false, true, "with a non-null Struct value and non-null Struct schema");

    assertInvalidRecord(true, true, false, false, "with a non-null Struct value and non-null Struct schema");
    assertInvalidRecord(false, true, false, false, "with a non-null Struct value and non-null Struct schema");
    assertInvalidRecord(true, false, false, false, "with a non-null Struct value and non-null Struct schema");
    assertInvalidRecord(false, false, false, false, "with a non-null Struct value and non-null Struct schema");
  }

  @Test
  public void testAddRecordDeleteEnabledAndNonePkMode() throws SQLException {
    props.put("delete.enabled", true);
    ConfigException e = assertThrows(ConfigException.class, () -> new JdbcSinkConfig(props));
    assertEquals(
        "Delete keys configured in delete.keys must be configured with a matching key configured as <newname> in value converter configuration " +
                "i.e. value.converer.<schemaname>.<oldname>,<newname> when delete.enabled is true",
        e.getMessage()
    );
  }




  protected SinkRecord generateRecord(
      boolean includeKeySchema,
      boolean includeKey,
      boolean includeValueSchema,
      boolean includeValue
  ) {
    Schema keySchema = SchemaBuilder.struct()
                                      .field("id", Schema.INT32_SCHEMA)
                                      .build();
    Schema valueSchema = SchemaBuilder.struct()
                                      .field("name", Schema.STRING_SCHEMA)
                                      .build();
    Schema keySchemaForRecord = includeKeySchema ? keySchema : null;
    Schema valueSchemaForRecord = includeValueSchema ? valueSchema : null;
    final Object key = includeKey ? new Struct(keySchema).put("id", 100) : null;
    final Object valueA = includeValue ? new Struct(valueSchema).put("name", "cuba") : null;
    return new SinkRecord("dummy", 0, keySchemaForRecord, key, valueSchemaForRecord, valueA, 0);
  }

  protected void assertInvalidRecord(
      boolean includeKeySchema,
      boolean includeKey,
      boolean includeValueSchema,
      boolean includeValue,
      String errorMessageFragment
  ) {
    assertInvalidRecord(
        generateRecord(includeKeySchema, includeKey, includeValueSchema, includeValue),
        errorMessageFragment
    );
  }

  protected void assertInvalidRecord(SinkRecord record, String errorMessageFragment) {
    final JdbcSinkConfig config = new JdbcSinkConfig(props);

    final String url = postgresqlHelper.postgreSQL();
    final DatabaseDialect dbDialect = DatabaseDialects.findBestFor(url, config);
    final DbStructure dbStructure = new DbStructure(dbDialect);

    final TableId tableId = new TableId(null, null, "dummy");
    final BufferedRecords buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, postgresqlHelper.connection);

    ConnectException e = assertThrows(ConnectException.class, () -> {
      buffer.add(record);
      buffer.flush();
    });
    assertTrue(
        "Unexpected message: " + e.getMessage(),
        e.getMessage().contains(errorMessageFragment)
    );
  }

  protected void assertValidRecord(
      boolean includeKeySchema,
      boolean includeKey,
      boolean includeValueSchema,
      boolean includeValue
  ) throws SQLException {
    assertValidRecord(
        generateRecord(includeKeySchema, includeKey, includeValueSchema, includeValue)
    );
  }

  protected void assertValidRecord(SinkRecord record) throws SQLException {
    props.put("batch.size", 2);
    final JdbcSinkConfig config = new JdbcSinkConfig(props);

    final String url = postgresqlHelper.postgreSQL();
    final DatabaseDialect dbDialect = DatabaseDialects.findBestFor(url, config);
    final DbStructure dbStructure = new DbStructure(dbDialect);

    final TableId tableId = new TableId(null, null, "dummy");
    final BufferedRecords buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, postgresqlHelper.connection);

    List<SinkRecord> flushed = buffer.add(record);
    assertEquals(Collections.emptyList(), flushed);
  }
}

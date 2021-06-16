/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.connect.jdbc.sink.integration;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.connect.jdbc.dialect.PostgreSqlDatabaseDialect;
import io.confluent.connect.jdbc.integration.BaseConnectorIT;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.sink.PostgresqlHelper;
import io.confluent.connect.jdbc.util.TableDefinition;
import io.confluent.connect.jdbc.util.TableId;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.connect.runtime.errors.ToleranceType;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static io.confluent.connect.jdbc.sink.JdbcSinkConfig.MAX_RETRIES;
import static io.confluent.connect.jdbc.sink.PostgresqlHelper.tablesUsed;
import static org.apache.kafka.connect.runtime.ConnectorConfig.ERRORS_TOLERANCE_CONFIG;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.DLQ_TOPIC_NAME_CONFIG;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.DLQ_TOPIC_REPLICATION_FACTOR_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


/**
 * Integration tests for writing to Postgres with UUID columns.
 */
@Category(IntegrationTest.class)
public class PostgresDatatypeIT extends BaseConnectorIT {

  private static final Logger LOG = LoggerFactory.getLogger(PostgresDatatypeIT.class);
  private final PostgresqlHelper postgresqlHelper = new PostgresqlHelper(getClass().getSimpleName());
  private static String tableName;
  private static Map<String, String> props;

  @Before
  public void before() throws SQLException, IOException {
    startConnect();
    props = baseSinkProps();
    tableName = postgresqlHelper.dbPath;
    postgresqlHelper.setUp();
    tablesUsed.add(tableName);
    String jdbcURL = postgresqlHelper.postgreSQL();
    props.put("connection.url", jdbcURL);
    props.put("pk.mode", "none");
    props.put("connection.user", "postgres");
    props.put("connection.password", "password123");
    props.put("topics", tableName);
    props.put("value.converter.schema.registry.url", "http://mock:8081");
    props.put("partitions", "5");
    props.put("value.converter.payload.field.name", "event");
    props.put("value.converter.input.format", "json");

    // create topic in Kafka
    connect.kafka().createTopic(tableName, 1);
  }

  @After
  public void after() throws SQLException {
    try {
      postgresqlHelper.tearDown();
    } catch (IOException e) {
      System.out.println("Could not teardown postgres helper");
    } finally { ;
      stopConnect();
    }
  }

  @Test
  public void autoCreateWithAutoEvolveAvroFirstValueFromArrayAndMapMultiStructureAllowNonIndexCastingDTypes() throws SQLException, RestClientException, IOException, InterruptedException {
    props.put("auto.create", "true");
    props.put("auto.evolve", "true");
    props.put("zonemapattributes", "intkey");
    props.put("distributionattributes", "stringkey");
    props.put("clusteredattributes", "stringkey, intkey");
    props.put("transforms", "Cast");
    props.put("transforms.Cast.type", "org.apache.kafka.connect.transforms.Cast$Value");
    props.put("transforms.Cast.spec", "intkey:int32,mapvalue:int8");
    props.put("value.converter.schema.names", "ComplexSchemaName");
    props.put("value.converter.json.ComplexSchemaName.subrecord1.subrecord2.int32", "true");
    props.put("value.converter.ComplexSchemaName.subrecord1.subrecord2.int32", "intkey");
    props.put("value.converter.ComplexSchemaName.string", "stringkey");
    props.put("value.converter.ComplexSchemaName.subrecord1.array", "arrayitem");
    props.put("value.converter.ComplexSchemaName.map", "mapvalue");

    connect.configureConnector("jdbc-sink-connector", props);
    waitForConnectorToStart("jdbc-sink-connector", 1);

    org.apache.avro.Schema subrecord2Type = org.apache.avro.SchemaBuilder.builder().record("subrecord2").fields()
            .optionalInt("int32")
            .endRecord();

    org.apache.avro.Schema subrecord1Type = org.apache.avro.SchemaBuilder.builder().record("subrecord1").fields()
            .name("subrecord2").type(subrecord2Type).noDefault()
            .name("array").type().array().items().unionOf()
            .nullType().and().stringType().endUnion()
            .noDefault()
            .endRecord();

    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.record("ComplexSchemaName").fields()
            .nullableInt("int8", 2)
            .requiredInt("int16")
            .requiredInt("int32")
            .requiredInt("int64")
            .requiredFloat("float32")
            .requiredBoolean("boolean")
            .optionalString("string")
            .requiredBytes("bytes")
            .name("array").type().array().items().type(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING)).noDefault()
            .name("map").type().map().values().type(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT)).noDefault()
            .name("subrecord1").type(subrecord1Type).noDefault()
            .endRecord()
            ;

    GenericRecord record = new GenericRecordBuilder(avroSchema)
            .set("int8", (byte) 12)
            .set("int16", (short) 12)
            .set("int32", 12)
            .set("int64", 12L)
            .set("float32", 12.2f)
            .set("boolean", true)
            .set("string", "id45634")
            .set("bytes", ByteBuffer.wrap("foo".getBytes()))
            .set("array", Arrays.asList("a", "b", "c"))
            .set("map", Collections.singletonMap("field", 1))
            .set("subrecord1", new GenericRecordBuilder(subrecord1Type)
                    .set("subrecord2", new GenericRecordBuilder(subrecord2Type)
                            .set("int32", 199)
                            .build()
                    )
                    .set("array", Collections.singletonList("x"))
                    .build()
            )
            .build();

    connect.kafka().produce(tableName, record.toString());
    waitForCommittedRecords("jdbc-sink-connector", Collections.singleton(tableName), 1, 1,
            TimeUnit.MINUTES.toMillis(2));

    assertEquals(
            1,
            postgresqlHelper.select(
                    "SELECT * FROM " + tableName,
                    new PostgresqlHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals("199", rs.getString("intkey"));
                        assertEquals("id45634", rs.getString("stringkey"));
                        assertEquals("x", rs.getString("arrayitem"));
                        assertEquals("1", rs.getString("mapvalue"));
                        JSONObject expectedJsonObject = new JSONObject(record.toString());
                        JSONObject resultJsonObject = new JSONObject(rs.getString("event"));
                        expectedJsonObject.keys().forEachRemaining(k -> assertEquals(resultJsonObject.get(k).toString(), expectedJsonObject.get(k).toString()));
                      }
                    }
            )
    );

    final JdbcSinkConfig config = new JdbcSinkConfig(props);
    PostgreSqlDatabaseDialect dialect = new PostgreSqlDatabaseDialect(config);
    TableId tableId = new TableId(null, null, tableName);
    TableDefinition refreshedMetadata = dialect.describeTable(postgresqlHelper.connection(), tableId);
    assertEquals(refreshedMetadata.definitionForColumn("intkey").type(), Types.INTEGER);
    assertEquals(refreshedMetadata.definitionForColumn("mapvalue").type(), Types.SMALLINT);
    System.out.println(refreshedMetadata);
  }

  /**
   * Verifies that even when the connector encounters exceptions that would cause a connection
   * with an invalid transaction, the connector sends only the errant record to the error
   * reporter and establishes a valid transaction for subsequent correct records to be sent to
   * the actual database.
   */
  @Test
  public void testPrimaryKeyConstraintsSendsToErrorReporter() throws Exception {
    props.put(ERRORS_TOLERANCE_CONFIG, ToleranceType.ALL.value());
    props.put(DLQ_TOPIC_NAME_CONFIG, DLQ_TOPIC_NAME);
    props.put(DLQ_TOPIC_REPLICATION_FACTOR_CONFIG, "1");
    props.put(MAX_RETRIES, "0");
    props.put("value.converter.schema.names", "Person");
    props.put("value.converter.json.Person.firstname", "true");
    props.put("value.converter.Person.firstname", "firstname");

    createTableWithPrimaryKey();
    connect.configureConnector("jdbc-sink-connector", props);
    waitForConnectorToStart("jdbc-sink-connector", 1);

    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.record("com.example.Person")
            .fields()
            .requiredString("firstname")
            .requiredString("lastname")
            .endRecord();
    GenericRecord firstRecord = new GenericRecordBuilder(avroSchema)
            .set("firstname", "Christina")
            .set("lastname", "Brams")
            .build();

    GenericRecord secondRecord = new GenericRecordBuilder(avroSchema)
            .set("firstname", "Mick")
            .set("lastname", "Michaels")
            .build();

    connect.kafka().produce(tableName, firstRecord.toString());
    // Send the same record for a PK collision
    connect.kafka().produce(tableName, firstRecord.toString());

    connect.kafka().produce(tableName, secondRecord.toString());

    waitForCommittedRecords("jdbc-sink-connector", Collections.singleton(tableName), 3, 1,
        TimeUnit.MINUTES.toMillis(3));

    ConsumerRecords<byte[], byte[]> records = connect.kafka().consume(1, CONSUME_MAX_DURATION_MS,
        DLQ_TOPIC_NAME);

    records.forEach(r -> {
      System.out.println(r.headers());
      System.out.println(new String(r.value()));
    });

    assertEquals(1, records.count());
  }

  @Test
  public void autoCreateWithAutoEvolveAvroFirstValueFromArrayAndMapMultiStructureAllowNonIndexUpsertDifferentKeyDTypes() throws SQLException, RestClientException, IOException, InterruptedException {
    props.put("connection.url", postgresqlHelper.postgreSQL());
    props.put("auto.create", "true");
    props.put("auto.evolve", "true");
    props.put("insert.mode", "upsert");
    props.put("max.errors", "1");
    props.put("max.retries", "0");
    props.put("upsert.keys", "intkey, stringkey");
    props.put("value.converter.payload.field.name", "event");
    props.put("connection.user", "postgres");
    props.put("connection.password", "password123");
    props.put("zonemapattributes", "stringkey,intkey");
    props.put("distributionattributes", "stringkey");
    props.put("clusteredattributes", "stringkey");
    props.put("partitions", "5");
    props.put("transforms", "CastV");
    props.put("transforms.CastV.type", "org.apache.kafka.connect.transforms.Cast$Value");
    props.put("transforms.CastV.spec", "intkey:int64");
    props.put("value.converter.schema.names", "ComplexSchemaName, SimpleSchemaName");
    props.put("value.converter.json.ComplexSchemaName.subrecord1.subrecord2.int32", "true");
    props.put("value.converter.json.SimpleSchemaName.stringy", "true");
    props.put("value.converter.ComplexSchemaName.subrecord1.subrecord2.int32", "intkey");
    props.put("value.converter.ComplexSchemaName.string", "stringkey");
    props.put("value.converter.ComplexSchemaName.subrecord1.array", "arrayitem");
    props.put("value.converter.ComplexSchemaName.map", "mapvalue");
    props.put("value.converter.SimpleSchemaName.int", "intkey");
    props.put("value.converter.SimpleSchemaName.stringy", "stringkey");
    props.put("value.converter.input.format", "json");
    props.put("value.converter.allownonindexed", "true");
    props.put("value.converter.value.subject.name.strategy", "io.confluent.kafka.serializers.subject.RecordNameStrategy");

    connect.configureConnector("jdbc-sink-connector", props);
    waitForConnectorToStart("jdbc-sink-connector", 1);

    org.apache.avro.Schema subrecord2Schema = org.apache.avro.SchemaBuilder.record("subrecord2").fields()
            .optionalInt("int32")
            .endRecord();

    org.apache.avro.Schema subrecord1Schema = org.apache.avro.SchemaBuilder.record("subrecord1").fields()
            .name("subrecord2").type(subrecord2Schema)
            .noDefault()
            .name("array").type().array().items().unionOf()
            .nullType().and().stringType().endUnion()
            .noDefault()
            .endRecord();

    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.record("ComplexSchemaName").fields()
            .nullableInt("int8", 2)
            .requiredInt("int16")
            .requiredInt("int32")
            .requiredInt("int64")
            .requiredFloat("float32")
            .requiredBoolean("boolean")
            .optionalString("string")
            .requiredBytes("bytes")
            .name("array").type().array().items().type(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING)).noDefault()
            .name("map").type().map().values().type(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT)).noDefault()
            .name("subrecord1").type(subrecord1Schema).noDefault()
            .endRecord()
            ;

    GenericData.Record complexStruct = new GenericRecordBuilder(avroSchema)
            .set("int8", 12)
            .set("int16", 12)
            .set("int32", 12)
            .set("int64", 12L)
            .set("float32", 12.2f)
            .set("boolean", true)
            .set("string", "id45634")
            .set("bytes", ByteBuffer.wrap("foo".getBytes()))
            .set("array", Arrays.asList("a", "b", "c"))
            .set("map", Collections.singletonMap("field", 1))
            .set("subrecord1", new GenericRecordBuilder(subrecord1Schema)
                    .set("subrecord2", new GenericRecordBuilder(subrecord2Schema)
                            .set("int32", 199)
                            .build())
                    .set("array", Collections.singletonList("x"))
                    .build())
            .build();

    org.apache.avro.Schema simpleAvroSchema = org.apache.avro.SchemaBuilder.record("com.example.SimpleSchemaName")
            .fields().requiredString("int")
            .requiredString("stringy")
            .endRecord();

    GenericData.Record simpleStruct = new GenericRecordBuilder(simpleAvroSchema)
            .set("int", "199")
            .set("stringy", "stringvalue")
            .build();

    GenericData.Record complexStruct2 = new GenericRecordBuilder(avroSchema)
            .set("int8", 2)
            .set("int16", 2)
            .set("int32", 2)
            .set("int64", 2L)
            .set("float32", 2.2f)
            .set("boolean", true)
            .set("string", "id45634")
            .set("bytes", ByteBuffer.wrap("2".getBytes()))
            .set("array", Arrays.asList("c", "y", "z"))
            .set("map", Collections.singletonMap("field", 2))
            .set("subrecord1", new GenericRecordBuilder(subrecord1Schema)
                    .set("subrecord2", new GenericRecordBuilder(subrecord2Schema)
                            .set("int32", 199)
                            .build())
                    .set("array", Collections.singletonList("f"))
                    .build())
            .build();

    connect.kafka().produce(tableName, complexStruct.toString());
    connect.kafka().produce(tableName, complexStruct2.toString());
    connect.kafka().produce(tableName, simpleStruct.toString());

    waitForCommittedRecords("jdbc-sink-connector", Collections.singleton(tableName), 3, 1,
            TimeUnit.MINUTES.toMillis(2));

    assertEquals(
            1,
            postgresqlHelper.select(
                    "SELECT * FROM " + tableName + " WHERE intkey ='" +
                            ((GenericData.Record) ((GenericData.Record) complexStruct2.get("subrecord1")).get("subrecord2")).get("int32") + "'" +
                            " AND stringkey ='" +
                            complexStruct2.get("string") + "'"
                    ,
                    new PostgresqlHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals(199, rs.getInt("intkey"));
                        assertEquals("id45634", rs.getString("stringkey"));
                        assertEquals("f", rs.getString("arrayitem"));
                        assertEquals("2", rs.getString("mapvalue"));
                        JSONObject expectedJsonObject = new JSONObject(complexStruct2.toString());
                        JSONObject resultJsonObject = new JSONObject(rs.getString("event"));
                        expectedJsonObject.keys().forEachRemaining(k -> assertEquals(resultJsonObject.get(k).toString(), expectedJsonObject.get(k).toString()));
                      }
                    }
            )
    );

    assertEquals(
            1,
            postgresqlHelper.select(
                    "SELECT * FROM " + tableName + " WHERE intkey ='" +
                            simpleStruct.get("int") + "'" +
                            " AND stringkey ='" +
                            simpleStruct.get("stringy") + "'"
                    ,
                    new PostgresqlHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals(199, rs.getInt("intkey"));
                        assertEquals("stringvalue", rs.getString("stringkey"));
                        JSONObject expectedJsonObject = new JSONObject(simpleStruct.toString());
                        JSONObject resultJsonObject = new JSONObject(rs.getString("event"));
                        expectedJsonObject.keys().forEachRemaining(k -> assertEquals(resultJsonObject.get(k).toString(), expectedJsonObject.get(k).toString()));
                      }
                    }
            )
    );
    assertEquals(
            1,
            postgresqlHelper.select(
                    "SELECT count(*) FROM " + tableName ,
                    new PostgresqlHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals(2, rs.getInt(1));            }
                    }
            )
    );
    final JdbcSinkConfig config = new JdbcSinkConfig(props);
    PostgreSqlDatabaseDialect dialect = new PostgreSqlDatabaseDialect(config);
    TableId tableId = new TableId(null, null, tableName);
    TableDefinition refreshedMetadata = dialect.describeTable(postgresqlHelper.connection(), tableId);
    assertEquals(refreshedMetadata.definitionForColumn("intkey").type(), Types.BIGINT);

  }

  @Test
  public void testRecordSchemaMoreFieldsThanTableSendsToErrorReporter() throws Exception {
    props.put(ERRORS_TOLERANCE_CONFIG, ToleranceType.ALL.value());
    props.put(DLQ_TOPIC_NAME_CONFIG, DLQ_TOPIC_NAME);
    props.put(DLQ_TOPIC_REPLICATION_FACTOR_CONFIG, "1");
    props.put("value.converter.schema.names", "Person");
    props.put("value.converter.json.Person.firstname", "true");
    props.put("value.converter.Person.firstname", "firstname");
    props.put("value.converter.Person.lastname", "lastname");
    props.put("value.converter.Person.jsonid", "jsonid");
    props.put("value.converter.Person.userid", "userid");

    createTableWithLessFields();
    connect.configureConnector("jdbc-sink-connector", props);
    waitForConnectorToStart("jdbc-sink-connector", 1);

    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.record("com.example.Person")
            .fields()
            .requiredString("firstname")
            .requiredString("lastname")
            .requiredString("jsonid")
            .requiredString("userid")
            .endRecord();
    GenericRecord record = new GenericRecordBuilder(avroSchema)
            .set("firstname", "Christina")
            .set("lastname", "Brams")
            .set("jsonid", "5")
            .set("userid", UUID.randomUUID().toString())
            .build();

    connect.kafka().produce(tableName, record.toString());

    waitForCommittedRecords("jdbc-sink-connector", Collections.singleton(tableName), 1, 1,
        TimeUnit.MINUTES.toMillis(2));

    ConsumerRecords<byte[], byte[]> records = connect.kafka().consume(1, CONSUME_MAX_DURATION_MS,
        DLQ_TOPIC_NAME);

    assertEquals(1, records.count());
  }

  @Test
  public void testWriteToTableWithUuidColumn() throws Exception {
    createTableWithUuidColumns();
    props.put("value.converter.schema.names", "Person");
    props.put("value.converter.json.Person.firstname", "true");
    props.put("value.converter." + "Person.firstname", "firstname");
    props.put("value.converter." + "Person.lastname", "lastname");
    props.put("value.converter." + "Person.jsonid", "jsonid");
    props.put("value.converter." + "Person.userid", "userid");

    connect.configureConnector("jdbc-sink-connector", props);
    waitForConnectorToStart("jdbc-sink-connector", 1);


    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.record("com.example.Person")
            .fields()
            .requiredString("firstname")
            .requiredString("lastname")
            .requiredString("jsonid")
            .requiredString("userid")
            .endRecord();
    GenericRecord record = new GenericRecordBuilder(avroSchema)
            .set("firstname", "Christina")
            .set("lastname", "Brams")
            .set("jsonid", "5")
            .set("userid", UUID.randomUUID().toString())
            .build();

    connect.kafka().produce(tableName, record.toString());

    waitForCommittedRecords("jdbc-sink-connector", Collections.singleton(tableName), 1, 1,
        TimeUnit.MINUTES.toMillis(2));

    try (Connection c = postgresqlHelper.connection()) {
      try (Statement s = c.createStatement()) {
        try (ResultSet rs = s.executeQuery("SELECT * FROM " + tableName)) {
          assertTrue(rs.next());
          assertEquals(record.get("firstname"), rs.getString("firstname"));
          assertEquals(record.get("lastname"), rs.getString("lastname"));
          assertEquals(record.get("jsonid"), rs.getString("jsonid"));
          assertEquals(record.get("userid"), rs.getString("userid"));
        }
      }
    }
  }

  private void createTable(String columnsSql) throws SQLException {
    try (Connection c = postgresqlHelper.connection()) {
      c.setAutoCommit(false);
      try (Statement s = c.createStatement()) {
        String sql = String.format(
            columnsSql,
            tableName
        );
        LOG.info("Executing statement: {}", sql);
        s.execute(sql);
        c.commit();
      }
    }
  }

  private void createTableWithUuidColumns() throws SQLException {
    LOG.info("Creating table {} with UUID column", tableName);
    createTable("CREATE TABLE %s(firstname TEXT, lastname TEXT, jsonid json, userid UUID, event JSONB)");
    LOG.info("Created table {} with UUID column", tableName);
  }

  private void createTableWithLessFields() throws SQLException {
    LOG.info("Creating table {} with less fields", tableName);
    createTable("CREATE TABLE %s(firstname TEXT, jsonid json, userid UUID, event JSONB)");
    LOG.info("Created table {} with less fields", tableName);
  }

  private void createTableWithPrimaryKey() throws SQLException {
    LOG.info("Creating table {} with a primary key", tableName);
    createTable("CREATE TABLE %s(firstName TEXT PRIMARY KEY, event JSONB)");
    LOG.info("Created table {} with a primary key", tableName);
  }

}

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

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import no.norsktipping.kafka.connect.converter.JsonConverter;
import no.norsktipping.kafka.connect.converter.JsonConverterConfig;
import org.apache.avro.LogicalTypes;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.easymock.EasyMockSupport;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.confluent.connect.jdbc.sink.PostgresqlHelper.tablesUsed;
import static org.easymock.EasyMock.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class JdbcSinkTaskTest extends EasyMockSupport {
  private final PostgresqlHelper postgresqlHelper = new PostgresqlHelper(getClass().getSimpleName());
  private final JdbcDbWriter mockWriter = createMock(JdbcDbWriter.class);
  private final SinkTaskContext ctx = createMock(SinkTaskContext.class);
  private JsonConverter converter = null;

  private static final SinkRecord RECORD = new SinkRecord(
      "stub",
      0,
      null,
      null,
      null,
      null,
      0
  );

  private static final org.apache.avro.Schema timestampMilliType = LogicalTypes.timestampMillis().addToSchema(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG));

  private static final org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.record("com.example.Person").fields()
          .requiredString("firstName")
          .requiredString("lastName")
          .optionalInt("age")
          .optionalBoolean("bool")
          .optionalInt("short")
          .optionalInt("byte")
          .optionalLong("long")
          .optionalFloat("float")
          .optionalDouble("double")
          .name("modified").type(timestampMilliType).noDefault()
          .endRecord();
  private static final Schema expectedSchema = SchemaBuilder.struct()
          .field("event", Schema.STRING_SCHEMA).version(1).build();

  private static  MockSchemaRegistryClient schemaRegistry;
  private static  KafkaAvroSerializer serializer;
  private static String topic;

  @Before
  public void setUp() throws IOException, SQLException {
    postgresqlHelper.setUp();
    topic = "atopic";
    tablesUsed.add(topic);

    Map<String, String> map = Stream.of(
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://mock:8081"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.SCHEMA_NAMES, "Person"),
            new AbstractMap.SimpleImmutableEntry<>("Person.modified", "modified"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.PAYLOAD_FIELD_NAME, "event"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.INPUT_FORMAT, "avro")
    )
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    schemaRegistry = new MockSchemaRegistryClient();
    converter = new JsonConverter(schemaRegistry);
    converter.configure(map, false);
    serializer = new KafkaAvroSerializer(schemaRegistry);
    serializer.configure(Collections.singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://mock:8081"), false);
  }

  @After
  public void tearDown() throws IOException, SQLException {
    postgresqlHelper.tearDown();
  }

  @Test
  public void putPropagatesToDbWithAutoCreateAndKafkaCoordinates() throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put("connection.url", postgresqlHelper.postgreSQL());
    props.put("auto.create", "true");
    props.put("auto.evolve", "true");
    props.put("coordinates.enabled", "true");
    String timeZoneID = "America/Los_Angeles";
    TimeZone timeZone = TimeZone.getTimeZone(timeZoneID);
    props.put("db.timezone", timeZoneID);
    props.put("connection.user", "postgres");
    props.put("connection.password", "password123");
    props.put("uppercase", "false");

    JdbcSinkTask task = new JdbcSinkTask();
    SinkTaskContext sinkTaskContextMock = mock(SinkTaskContext.class);
    task.initialize(sinkTaskContextMock);

    task.start(props);

    GenericData.Record struct = new GenericRecordBuilder(avroSchema)
        .set("firstName", "Alex")
        .set("lastName", "Smith")
        .set("bool", true)
        .set("short", 1234)
        .set("byte", -32)
        .set("long", 12425436L)
        .set("float", (float) 2356.3)
        .set("double", -2436546.56457)
        .set("age", 21)
        .set("modified", 1474661402123L)
        .build();

    schemaRegistry.register(topic+ "-value", avroSchema);
    String expected = "{\"firstName\":\"Alex\",\"lastName\":\"Smith\",\"bool\":true,\"short\":1234,\"byte\":-32,\"long\":12425436,\"float\":2356.3,\"double\":-2436546.56457,\"age\":21,\"modified\":\"2016-09-23T20:10:02.123Z\"}";

    SchemaAndValue schemaAndValue = converter.toConnectData(topic, serializer.serialize(topic, struct));
    task.put(Collections.singleton(
            new SinkRecord(topic, 1, null, null, schemaAndValue.schema(), schemaAndValue.value(), 42)
    ));

   assertEquals(
        1,
        postgresqlHelper.select(
            "SELECT * FROM " + topic,
            new PostgresqlHelper.ResultSetReadCallback() {
              @Override
              public void read(ResultSet rs) throws SQLException {
                assertEquals(topic, rs.getString("connect_topic"));
                assertEquals(1, rs.getInt("connect_partition"));
                assertEquals(42, rs.getLong("connect_offset"));
                JSONObject expectedJsonObject = new JSONObject(expected);
                JSONObject resultJsonObject = new JSONObject(rs.getString("event"));
                expectedJsonObject.keys().forEachRemaining(k -> assertEquals(expectedJsonObject.get(k), resultJsonObject.get(k)));
              }
            }
        )
    );
  }

  @Test
  public void putPropagatesToDbWithAutoCreateAndKafkaCoordinatesTablePreExisting() throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put("connection.url", postgresqlHelper.postgreSQL());
    props.put("connection.user", "postgres");
    props.put("connection.password", "password123");
    props.put("uppercase", "false");

    Map<String, String> map = Stream.of(
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://mock:8081"),
            new AbstractMap.SimpleImmutableEntry<>("uppercase", "false"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.SCHEMA_NAMES, "Person"),
            new AbstractMap.SimpleImmutableEntry<>("Person.firstName", "firstname"),
            new AbstractMap.SimpleImmutableEntry<>("Person.lastName", "lastname"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.PAYLOAD_FIELD_NAME, "event"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.INPUT_FORMAT, "avro")
    )
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    converter.configure(map, false);
    JdbcSinkTask task = new JdbcSinkTask();
    task.initialize(mock(SinkTaskContext.class));

    postgresqlHelper.createTable(
        "CREATE TABLE " + topic.toLowerCase() + "(" +
        "    firstname  TEXT," +
        "    lastname  TEXT," +
        "    event  JSONB);"
    );

    task.start(props);

    GenericData.Record struct = new GenericRecordBuilder(avroSchema)
        .set("firstName", "Christina")
        .set("lastName", "Brams")
        .set("bool", false)
        .set("byte", -72)
        .set("long", 8594L)
        .set("double", 3256677.56457d)
        .set("age", 28)
        .set("modified", 1474661402123L)
        .build();

    schemaRegistry.register(topic+ "-value", avroSchema);
    String expected = "{\"firstName\":\"Christina\",\"lastName\":\"Brams\",\"age\":28,\"bool\":false,\"short\":null,\"byte\":-72,\"long\":8594,\"float\":null,\"double\":3256677.56457,\"modified\":\"2016-09-23T20:10:02.123Z\"}";

    SchemaAndValue schemaAndValue = converter.toConnectData(topic, serializer.serialize(topic, struct));
    System.out.println(schemaAndValue);
    task.put(Collections.singleton(
            new SinkRecord(topic, 1, null, null, schemaAndValue.schema(), schemaAndValue.value(), 43)
    ));
    assertEquals(
        1,
        postgresqlHelper.select(
            "SELECT * FROM " + topic.toLowerCase() + " WHERE firstname='" + struct.get("firstName") + "' and lastname='" + struct.get("lastName") + "'",
            new PostgresqlHelper.ResultSetReadCallback() {
              @Override
              public void read(ResultSet rs) throws SQLException {
                JSONObject expectedJsonObject = new JSONObject(expected);
                JSONObject resultJsonObject = new JSONObject(rs.getString("event"));
                expectedJsonObject.keys().forEachRemaining(k -> assertEquals(expectedJsonObject.get(k), resultJsonObject.get(k)));
              }
            }
        )
    );
  }

  @Test
  public void retries() throws SQLException {
    final int maxRetries = 2;
    final int retryBackoffMs = 1000;

    List<SinkRecord> records = createRecordsList(1);

    mockWriter.write(records);
    SQLException chainedException = new SQLException("cause 1");
    chainedException.setNextException(new SQLException("cause 2"));
    chainedException.setNextException(new SQLException("cause 3"));
    expectLastCall().andThrow(chainedException).times(1 + maxRetries);

    ctx.timeout(retryBackoffMs);
    expectLastCall().times(maxRetries);

    mockWriter.closeQuietly();
    expectLastCall().times(maxRetries);

    JdbcSinkTask task = new JdbcSinkTask() {
      @Override
      void initWriter() {
        this.writer = mockWriter;
      }
    };
    task.initialize(ctx);
    expect(ctx.errantRecordReporter()).andReturn(null);
    replayAll();

    Map<String, String> props = setupBasicProps(maxRetries, retryBackoffMs);
    task.start(props);

    try {
      task.put(records);
      fail();
    } catch (RetriableException expected) {
      assertEquals(SQLException.class, expected.getCause().getClass());
      int i = 0;
      for (Throwable t : (SQLException) expected.getCause()) {
        ++i;
        StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        System.out.println("Chained exception " + i + ": " + sw);
      }
    }

    try {
      task.put(records);
      fail();
    } catch (RetriableException expected) {
      assertEquals(SQLException.class, expected.getCause().getClass());
      int i = 0;
      for (Throwable t : (SQLException) expected.getCause()) {
        ++i;
        StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        System.out.println("Chained exception " + i + ": " + sw);
      }
    }

    try {
      task.put(records);
      fail();
    } catch (RetriableException e) {
      fail("Non-retriable exception expected");
    } catch (ConnectException expected) {
      assertEquals(SQLException.class, expected.getCause().getClass());
      int i = 0;
      for (Throwable t : (SQLException) expected.getCause()) {
        ++i;
        StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        System.out.println("Chained exception " + i + ": " + sw);
      }
    }

    verifyAll();
  }

  @Test
  public void errorReporting() throws SQLException {
    List<SinkRecord> records = createRecordsList(1);

    mockWriter.write(records);
    SQLException exception = new SQLException("cause 1");
    expectLastCall().andThrow(exception);
    mockWriter.closeQuietly();
    expectLastCall();
    mockWriter.write(anyObject());
    expectLastCall().andThrow(exception);

    JdbcSinkTask task = new JdbcSinkTask() {
      @Override
      void initWriter() {
        this.writer = mockWriter;
      }
    };
    task.initialize(ctx);
    ErrantRecordReporter reporter = createMock(ErrantRecordReporter.class);
    expect(ctx.errantRecordReporter()).andReturn(reporter);
    expect(reporter.report(anyObject(), anyObject())).andReturn(CompletableFuture.completedFuture(null));
    mockWriter.closeQuietly();
    expectLastCall();
    replayAll();

    Map<String, String> props = setupBasicProps(0, 0);
    task.start(props);
    task.put(records);
    verifyAll();
  }

  @Test
  public void errorReportingTableAlterOrCreateException() throws SQLException {
    List<SinkRecord> records = createRecordsList(1);

    mockWriter.write(records);
    TableAlterOrCreateException exception = new TableAlterOrCreateException("cause 1");
    expectLastCall().andThrow(exception);
    mockWriter.closeQuietly();
    expectLastCall();
    mockWriter.write(anyObject());
    expectLastCall().andThrow(exception);

    JdbcSinkTask task = new JdbcSinkTask() {
      @Override
      void initWriter() {
        this.writer = mockWriter;
      }
    };
    task.initialize(ctx);
    ErrantRecordReporter reporter = createMock(ErrantRecordReporter.class);
    expect(ctx.errantRecordReporter()).andReturn(reporter);
    expect(reporter.report(anyObject(), anyObject())).andReturn(CompletableFuture.completedFuture(null));
    mockWriter.closeQuietly();
    expectLastCall();
    replayAll();

    Map<String, String> props = setupBasicProps(0, 0);
    task.start(props);
    task.put(records);
    verifyAll();
  }

  @Test
  public void batchErrorReporting() throws SQLException {
    final int batchSize = 3;

    List<SinkRecord> records = createRecordsList(batchSize);

    mockWriter.write(records);
    SQLException exception = new SQLException("cause 1");
    expectLastCall().andThrow(exception);
    mockWriter.closeQuietly();
    expectLastCall();
    mockWriter.write(anyObject());
    expectLastCall().andThrow(exception).times(batchSize);

    JdbcSinkTask task = new JdbcSinkTask() {
      @Override
      void initWriter() {
        this.writer = mockWriter;
      }
    };
    task.initialize(ctx);
    ErrantRecordReporter reporter = createMock(ErrantRecordReporter.class);
    expect(ctx.errantRecordReporter()).andReturn(reporter);
    expect(reporter.report(anyObject(), anyObject())).andReturn(CompletableFuture.completedFuture(null)).times(batchSize);
    for (int i = 0; i < batchSize; i++) {
      mockWriter.closeQuietly();
      expectLastCall();
    }
    replayAll();

    Map<String, String> props = setupBasicProps(0, 0);
    task.start(props);
    task.put(records);
    verifyAll();
  }

  @Test
  public void oneInBatchErrorReporting() throws SQLException {
    final int batchSize = 3;

    List<SinkRecord> records = createRecordsList(batchSize);

    mockWriter.write(records);
    SQLException exception = new SQLException("cause 1");
    expectLastCall().andThrow(exception);
    mockWriter.closeQuietly();
    expectLastCall();
    mockWriter.write(anyObject());
    expectLastCall().times(2);
    expectLastCall().andThrow(exception);

    JdbcSinkTask task = new JdbcSinkTask() {
      @Override
      void initWriter() {
        this.writer = mockWriter;
      }
    };
    task.initialize(ctx);
    ErrantRecordReporter reporter = createMock(ErrantRecordReporter.class);
    expect(ctx.errantRecordReporter()).andReturn(reporter);
    expect(reporter.report(anyObject(), anyObject())).andReturn(CompletableFuture.completedFuture(null));
    mockWriter.closeQuietly();
    expectLastCall();
    replayAll();

    Map<String, String> props = setupBasicProps(0, 0);
    task.start(props);
    task.put(records);
    verifyAll();
  }

  @Test
  public void oneInMiddleBatchErrorReporting() throws SQLException {
    final int batchSize = 3;

    List<SinkRecord> records = createRecordsList(batchSize);

    mockWriter.write(records);
    SQLException exception = new SQLException("cause 1");
    expectLastCall().andThrow(exception);
    mockWriter.closeQuietly();
    expectLastCall();
    mockWriter.write(anyObject());
    expectLastCall();
    mockWriter.write(anyObject());
    expectLastCall().andThrow(exception);
    mockWriter.write(anyObject());
    expectLastCall();

    JdbcSinkTask task = new JdbcSinkTask() {
      @Override
      void initWriter() {
        this.writer = mockWriter;
      }
    };
    task.initialize(ctx);
    ErrantRecordReporter reporter = createMock(ErrantRecordReporter.class);
    expect(ctx.errantRecordReporter()).andReturn(reporter);
    expect(reporter.report(anyObject(), anyObject())).andReturn(CompletableFuture.completedFuture(null));
    mockWriter.closeQuietly();
    expectLastCall();
    replayAll();

    Map<String, String> props = setupBasicProps(0, 0);
    task.start(props);
    task.put(records);
    verifyAll();
  }

  private List<SinkRecord> createRecordsList(int batchSize) {
    List<SinkRecord> records = new ArrayList<>();
    for (int i = 0; i < batchSize; i++) {
      records.add(RECORD);
    }
    return records;
  }

  private Map<String, String> setupBasicProps(int maxRetries, long retryBackoffMs) {
    Map<String, String> props = new HashMap<>();
    props.put(JdbcSinkConfig.CONNECTION_URL, "stub");
    props.put(JdbcSinkConfig.MAX_RETRIES, String.valueOf(maxRetries));
    props.put(JdbcSinkConfig.RETRY_BACKOFF_MS, String.valueOf(retryBackoffMs));
    return props;
  }
}

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

import com.google.common.util.concurrent.UncheckedExecutionException;
import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.PostgreSqlDatabaseDialect;
import io.confluent.connect.jdbc.util.TableDefinition;
import io.confluent.connect.jdbc.util.TableId;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import no.norsktipping.kafka.connect.converter.JsonConverter;
import no.norsktipping.kafka.connect.converter.JsonConverterConfig;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.confluent.connect.jdbc.sink.PostgresqlHelper.tablesUsed;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.*;

public class JdbcDbWriterTest {

  private final PostgresqlHelper postgresqlHelper = new PostgresqlHelper(getClass().getSimpleName());

  private JdbcDbWriter writer = null;
  private DatabaseDialect dialect;
  private JsonConverter converter = null;
  private static final String TOPIC = "topic";
  private static MockSchemaRegistryClient schemaRegistry;
  private static KafkaAvroSerializer serializer;

  @Before
  public void setUp() throws IOException, SQLException {
    postgresqlHelper.setUp();
    tablesUsed.add(TOPIC);
    schemaRegistry = new MockSchemaRegistryClient();
    converter = new JsonConverter(schemaRegistry);
    serializer = new KafkaAvroSerializer(schemaRegistry);
    serializer.configure(Collections.singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://mock:8081"), false);
  }

  @After
  public void tearDown() throws IOException, SQLException {
    if (writer != null)
      writer.closeQuietly();
    postgresqlHelper.tearDown();
  }

  private JdbcDbWriter newWriter(Map<String, String> props) {
    final JdbcSinkConfig config = new JdbcSinkConfig(props);
    dialect = new PostgreSqlDatabaseDialect(config);
    final DbStructure dbStructure = new DbStructure(dialect);
    return new JdbcDbWriter(config, dialect, dbStructure);
  }

  @Test
  public void autoCreateWithAutoEvolveAvro() throws SQLException, RestClientException, IOException {
    Map<String, String> props = new HashMap<>();
    props.put("connection.url", postgresqlHelper.postgreSQL());
    props.put("auto.create", "true");
    props.put("auto.evolve", "true");
    props.put("value.converter.payload.field.name", "event");
    props.put("connection.user", "postgres");
    props.put("connection.password", "password123");
    props.put("zonemapattributes", "intkey");
    props.put("distributionattributes", "stringkey");
    props.put("clusteredattributes", "stringkey, intkey");
    props.put("partitions", "5");
    Map<String, String> map = Stream.of(
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://mock:8081"),

            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.SCHEMA_NAMES, "ComplexSchemaName"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.int32", "intkey"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.string", "stringkey"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.PAYLOAD_FIELD_NAME, "event"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.INPUT_FORMAT, "avro")
    )
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    converter.configure(map, false);
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
            .endRecord()
            ;

    GenericData.Record struct = new GenericRecordBuilder(avroSchema)
            .set("int8", 12)
            .set("int16", 12)
            .set("int32", 12)
            .set("int64", 12L)
            .set("float32", 12.2f)
            .set("boolean", true)
            .set("string", "id45634")
            .set("bytes", ByteBuffer.wrap("foo".getBytes()))
            .set("array", Arrays.asList("a", "b", "c"))
            .set("map", Collections.singletonMap("field", 1)).build();

    schemaRegistry.register(TOPIC+ "-value", avroSchema);
    String expected = struct.toString();
    SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, serializer.serialize(TOPIC, struct));

    final SinkRecord recordA = new SinkRecord(TOPIC, 0, null, null, schemaAndValue.schema(), schemaAndValue.value(), 0);
    writer = newWriter(props);
    writer.write(Collections.singletonList(recordA));

    assertEquals(
            1,
            postgresqlHelper.select(
                    "SELECT * FROM " + TOPIC,
                    new PostgresqlHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals("12", rs.getString("intkey"));
                        assertEquals("id45634", rs.getString("stringkey"));
                        JSONObject expectedJsonObject = new JSONObject(expected);
                        JSONObject resultJsonObject = new JSONObject(rs.getString("event"));
                        expectedJsonObject.keys().forEachRemaining(k -> assertEquals(resultJsonObject.get(k).toString(), expectedJsonObject.get(k).toString()));
                      }
                    }
            )
    );
    TableId tableId = new TableId(null, null, TOPIC);
    TableDefinition refreshedMetadata = dialect.describeTable(postgresqlHelper.connection, tableId);
    System.out.println(refreshedMetadata);
  }

  @Test
  public void autoCreateWithAutoEvolveJson() throws SQLException, RestClientException, IOException {
    Map<String, String> props = new HashMap<>();
    props.put("connection.url", postgresqlHelper.postgreSQL());
    props.put("auto.create", "true");
    props.put("auto.evolve", "true");
    props.put("value.converter.payload.field.name", "event");
    props.put("connection.user", "postgres");
    props.put("connection.password", "password123");
    props.put("zonemapattributes", "intkey");
    props.put("distributionattributes", "stringkey");
    props.put("clusteredattributes", "stringkey, intkey");
    props.put("partitions", "5");
    Map<String, String> map = Stream.of(
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://mock:8081"),

            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.SCHEMA_NAMES, "ComplexSchemaName"),
            new AbstractMap.SimpleImmutableEntry<>("json.ComplexSchemaName.string", "true"),

            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.int32", "intkey"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.string", "stringkey"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.PAYLOAD_FIELD_NAME, "event"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.INPUT_FORMAT, "json")
    )
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    converter.configure(map, false);

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
            .build();

    byte[] converted = record.toString().getBytes();
    String expected = record.toString();
    SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, converted);
    System.out.println(schemaAndValue.schema() + "\n" + schemaAndValue.value());

    final SinkRecord recordA = new SinkRecord(TOPIC, 0, null, null, schemaAndValue.schema(), schemaAndValue.value(), 0);
    writer = newWriter(props);
    writer.write(Collections.singletonList(recordA));

    assertEquals(
            1,
            postgresqlHelper.select(
                    "SELECT * FROM " + TOPIC,
                    new PostgresqlHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals("12", rs.getString("intkey"));
                        assertEquals("id45634", rs.getString("stringkey"));
                        JSONObject expectedJsonObject = new JSONObject(expected);
                        JSONObject resultJsonObject = new JSONObject(rs.getString("event"));
                        expectedJsonObject.keys().forEachRemaining(k -> assertEquals(resultJsonObject.get(k).toString(), expectedJsonObject.get(k).toString()));
                      }
                    }
            )
    );
    TableId tableId = new TableId(null, null, TOPIC);
    TableDefinition refreshedMetadata = dialect.describeTable(postgresqlHelper.connection, tableId);
    System.out.println(refreshedMetadata);
  }


  @Test
  public void autoCreateWithAutoEvolveAvroFirstValueFromArrayAndMapOneLevelAsColumn() throws SQLException, RestClientException, IOException {
    Map<String, String> props = new HashMap<>();
    props.put("connection.url", postgresqlHelper.postgreSQL());
    props.put("auto.create", "true");
    props.put("auto.evolve", "true");
    props.put("value.converter.payload.field.name", "event");
    props.put("connection.user", "postgres");
    props.put("connection.password", "password123");
    props.put("zonemapattributes", "intkey");
    props.put("distributionattributes", "stringkey");
    props.put("clusteredattributes", "stringkey, intkey");
    props.put("partitions", "5");
    Map<String, String> map = Stream.of(
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://mock:8081"),

            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.SCHEMA_NAMES, "ComplexSchemaName"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.int32", "intkey"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.string", "stringkey"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.array", "arrayitem"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.map", "mapvalue"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.PAYLOAD_FIELD_NAME, "event"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.INPUT_FORMAT, "avro")
    )
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    converter.configure(map, false);

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
            .endRecord()
            ;

    GenericData.Record struct = new GenericRecordBuilder(avroSchema)
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
            .build()
            ;

    schemaRegistry.register(TOPIC+ "-value", avroSchema);
    System.out.println(schemaRegistry.getAllSubjectsById(1));
    String expected = struct.toString();
    SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, serializer.serialize(TOPIC, struct));

    final SinkRecord recordA = new SinkRecord(TOPIC, 0, null, null, schemaAndValue.schema(), schemaAndValue.value(), 0);
    writer = newWriter(props);
    writer.write(Collections.singletonList(recordA));

    assertEquals(
            1,
            postgresqlHelper.select(
                    "SELECT * FROM " + TOPIC,
                    new PostgresqlHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals("12", rs.getString("intkey"));
                        assertEquals("id45634", rs.getString("stringkey"));
                        assertEquals("a", rs.getString("arrayitem"));
                        assertEquals("1", rs.getString("mapvalue"));
                        JSONObject expectedJsonObject = new JSONObject(expected);
                        JSONObject resultJsonObject = new JSONObject(rs.getString("event"));
                        expectedJsonObject.keys().forEachRemaining(k -> assertEquals(resultJsonObject.get(k).toString(), expectedJsonObject.get(k).toString()));
                      }
                    }
            )
    );
    TableId tableId = new TableId(null, null, TOPIC);
    TableDefinition refreshedMetadata = dialect.describeTable(postgresqlHelper.connection, tableId);
    System.out.println(refreshedMetadata);
  }

  @Test
  public void autoCreateWithAutoEvolveJsonFirstValueFromArrayAndMapOneLevelAsColumn() throws SQLException, RestClientException, IOException {
    Map<String, String> props = new HashMap<>();
    props.put("connection.url", postgresqlHelper.postgreSQL());
    props.put("auto.create", "true");
    props.put("auto.evolve", "true");
    props.put("value.converter.payload.field.name", "event");
    props.put("connection.user", "postgres");
    props.put("connection.password", "password123");
    props.put("zonemapattributes", "intkey");
    props.put("distributionattributes", "stringkey");
    props.put("clusteredattributes", "stringkey, intkey");
    props.put("partitions", "5");
    Map<String, String> map = Stream.of(
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://mock:8081"),

            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.SCHEMA_NAMES, "ComplexSchemaName"),
            new AbstractMap.SimpleImmutableEntry<>("json.ComplexSchemaName.string", "true"),

            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.int32", "intkey"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.string", "stringkey"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.array", "arrayitem"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.map", "mapvalue"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.PAYLOAD_FIELD_NAME, "event"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.INPUT_FORMAT, "json")
    )
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    converter.configure(map, false);

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
            .build();

    byte[] converted = record.toString().getBytes();
    String expected = record.toString();
    SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, converted);
    System.out.println(schemaAndValue.schema() + "\n" + schemaAndValue.value());

    final SinkRecord recordA = new SinkRecord(TOPIC, 0, null, null, schemaAndValue.schema(), schemaAndValue.value(), 0);
    writer = newWriter(props);
    writer.write(Collections.singletonList(recordA));

    assertEquals(
            1,
            postgresqlHelper.select(
                    "SELECT * FROM " + TOPIC,
                    new PostgresqlHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals("12", rs.getString("intkey"));
                        assertEquals("id45634", rs.getString("stringkey"));
                        assertEquals("a", rs.getString("arrayitem"));
                        assertEquals("1", rs.getString("mapvalue"));
                        JSONObject expectedJsonObject = new JSONObject(expected);
                        JSONObject resultJsonObject = new JSONObject(rs.getString("event"));
                        expectedJsonObject.keys().forEachRemaining(k -> assertEquals(resultJsonObject.get(k).toString(), expectedJsonObject.get(k).toString()));
                      }
                    }
            )
    );
    TableId tableId = new TableId(null, null, TOPIC);
    TableDefinition refreshedMetadata = dialect.describeTable(postgresqlHelper.connection, tableId);
    System.out.println(refreshedMetadata);
  }

  @Test
  public void autoCreateWithAutoEvolveAvroFirstValueFromArrayAndMapMultiLevelAsColumn() throws SQLException, RestClientException, IOException {
    Map<String, String> props = new HashMap<>();
    props.put("connection.url", postgresqlHelper.postgreSQL());
    props.put("auto.create", "true");
    props.put("auto.evolve", "true");
    props.put("value.converter.payload.field.name", "event");
    props.put("connection.user", "postgres");
    props.put("connection.password", "password123");
    props.put("zonemapattributes", "intkey");
    props.put("distributionattributes", "stringkey");
    props.put("clusteredattributes", "stringkey, intkey");
    props.put("partitions", "5");
    Map<String, String> map = Stream.of(
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://mock:8081"),

            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.SCHEMA_NAMES, "ComplexSchemaName"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.subrecord1.subrecord2.int32", "intkey"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.string", "stringkey"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.subrecord1.array", "arrayitem"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.map", "mapvalue"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.PAYLOAD_FIELD_NAME, "event"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.INPUT_FORMAT, "avro")
    )
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    converter.configure(map, false);

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
    System.out.println(avroSchema);
    GenericData.Record struct = new GenericRecordBuilder(avroSchema)
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

    schemaRegistry.register(TOPIC+ "-value", avroSchema);
    String expected = struct.toString();
    SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, serializer.serialize(TOPIC, struct));

    final SinkRecord recordA = new SinkRecord(TOPIC, 0, null, null, schemaAndValue.schema(), schemaAndValue.value(), 0);
    writer = newWriter(props);
    writer.write(Collections.singletonList(recordA));

    assertEquals(
            1,
            postgresqlHelper.select(
                    "SELECT * FROM " + TOPIC,
                    new PostgresqlHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals("199", rs.getString("intkey"));
                        assertEquals("id45634", rs.getString("stringkey"));
                        assertEquals("x", rs.getString("arrayitem"));
                        assertEquals("1", rs.getString("mapvalue"));
                        JSONObject expectedJsonObject = new JSONObject(expected);
                        JSONObject resultJsonObject = new JSONObject(rs.getString("event"));
                        expectedJsonObject.keys().forEachRemaining(k -> assertEquals(resultJsonObject.get(k).toString(), expectedJsonObject.get(k).toString()));
                      }
                    }
            )
    );
    TableId tableId = new TableId(null, null, TOPIC);
    TableDefinition refreshedMetadata = dialect.describeTable(postgresqlHelper.connection, tableId);
    System.out.println(refreshedMetadata);
  }

  @Test
  public void autoCreateWithAutoEvolveJsonFirstValueFromArrayAndMapMultipleLevelAsColumn() throws SQLException, RestClientException, IOException {
    Map<String, String> props = new HashMap<>();
    props.put("connection.url", postgresqlHelper.postgreSQL());
    props.put("auto.create", "true");
    props.put("auto.evolve", "true");
    props.put("value.converter.payload.field.name", "event");
    props.put("connection.user", "postgres");
    props.put("connection.password", "password123");
    props.put("zonemapattributes", "intkey");
    props.put("distributionattributes", "stringkey");
    props.put("clusteredattributes", "stringkey, intkey");
    props.put("partitions", "5");
    Map<String, String> map = Stream.of(
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://mock:8081"),

            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.SCHEMA_NAMES, "ComplexSchemaName"),
            new AbstractMap.SimpleImmutableEntry<>("json.ComplexSchemaName.subrecord1.subrecord2.int32", "true"),

            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.subrecord1.subrecord2.int32", "intkey"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.string", "stringkey"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.subrecord1.array", "arrayitem"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.map", "mapvalue"),

            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.PAYLOAD_FIELD_NAME, "event"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.INPUT_FORMAT, "json")
    )
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    converter.configure(map, false);

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



    byte[] converted = record.toString().getBytes();
    String expected = record.toString();
    SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, converted);
    System.out.println(schemaAndValue.schema() + "\n" + schemaAndValue.value());

    final SinkRecord recordA = new SinkRecord(TOPIC, 0, null, null, schemaAndValue.schema(), schemaAndValue.value(), 0);
    writer = newWriter(props);
    writer.write(Collections.singletonList(recordA));

    assertEquals(
            1,
            postgresqlHelper.select(
                    "SELECT * FROM " + TOPIC,
                    new PostgresqlHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals("199", rs.getString("intkey"));
                        assertEquals("id45634", rs.getString("stringkey"));
                        assertEquals("x", rs.getString("arrayitem"));
                        assertEquals("1", rs.getString("mapvalue"));
                        JSONObject expectedJsonObject = new JSONObject(expected);
                        JSONObject resultJsonObject = new JSONObject(rs.getString("event"));
                        expectedJsonObject.keys().forEachRemaining(k -> assertEquals(resultJsonObject.get(k).toString(), expectedJsonObject.get(k).toString()));
                      }
                    }
            )
    );
    TableId tableId = new TableId(null, null, TOPIC);
    TableDefinition refreshedMetadata = dialect.describeTable(postgresqlHelper.connection, tableId);
    System.out.println(refreshedMetadata);
  }

  @Test
  public void autoCreateWithAutoEvolveAvroFirstValueFromArrayAndMapMultiStructureInconsistentDereferecingColumns() throws SQLException, RestClientException, IOException {
    Map<String, String> props = new HashMap<>();
    props.put("connection.url", postgresqlHelper.postgreSQL());
    props.put("auto.create", "true");
    props.put("auto.evolve", "true");
    props.put("value.converter.payload.field.name", "event");
    props.put("connection.user", "postgres");
    props.put("connection.password", "password123");
    props.put("zonemapattributes", "intkey");
    props.put("distributionattributes", "stringkey");
    props.put("clusteredattributes", "stringkey, intkey");
    props.put("partitions", "5");
    Map<String, String> map = Stream.of(
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://mock:8081"),

            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.SCHEMA_NAMES, "ComplexSchemaName, SimpleSchemaName"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.subrecord1.subrecord2.int32", "intkey"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.string", "stringkey"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.subrecord1.array", "arrayitem"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.map", "mapvalue"),
            new AbstractMap.SimpleImmutableEntry<>("SimpleSchemaName.int", "intkey"),
            new AbstractMap.SimpleImmutableEntry<>("SimpleSchemaName.stringy", "stringkey"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.PAYLOAD_FIELD_NAME, "event"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.INPUT_FORMAT, "avro"),
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName())
    )
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    ConfigException thrown = assertThrows(
            ConfigException.class,
            () -> converter.configure(map, false)
    );

    assertTrue(thrown.getMessage().contains("Dereferencing arrayitem must be defined for each schema that is configured in schema.names"));
  }

  @Test
  public void autoCreateWithAutoEvolveAvroFirstValueFromArrayAndMapMultiStructureAllowNonIndex() throws SQLException, RestClientException, IOException {
    Map<String, String> props = new HashMap<>();
    props.put("connection.url", postgresqlHelper.postgreSQL());
    props.put("auto.create", "true");
    props.put("auto.evolve", "true");
    props.put("value.converter.payload.field.name", "event");
    props.put("connection.user", "postgres");
    props.put("connection.password", "password123");
    props.put("zonemapattributes", "intkey");
    props.put("distributionattributes", "stringkey");
    props.put("clusteredattributes", "stringkey, intkey");
    props.put("partitions", "5");
    Map<String, String> map = Stream.of(
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://mock:8081"),

            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.SCHEMA_NAMES, "ComplexSchemaName, SimpleSchemaName"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.subrecord1.subrecord2.int32", "intkey"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.string", "stringkey"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.subrecord1.array", "arrayitem"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.map", "mapvalue"),
            new AbstractMap.SimpleImmutableEntry<>("SimpleSchemaName.int", "intkey"),
            new AbstractMap.SimpleImmutableEntry<>("SimpleSchemaName.stringy", "stringkey"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.PAYLOAD_FIELD_NAME, "event"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.INPUT_FORMAT, "avro"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.ALLOWNONINDEXED, "true"),
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName())
    )
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    converter.configure(map, false);

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
            .fields().requiredInt("int")
            .requiredString("stringy")
            .endRecord();

    GenericData.Record simpleStruct = new GenericRecordBuilder(simpleAvroSchema)
            .set("int", 13)
            .set("stringy", "stringvalue")
            .build();

    schemaRegistry.register(TOPIC + avroSchema.getFullName() + "-value", avroSchema);
    schemaRegistry.register(TOPIC + simpleAvroSchema.getFullName() + "-value", simpleAvroSchema);

    String expectedComplex = complexStruct.toString();
    String expectedSimple = simpleStruct.toString();

    Map<String, String> smap = Stream.of(
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://mock:8081"),
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName())
    )
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    serializer.configure(smap, false);

    SchemaAndValue complexSchemaAndValue = converter.toConnectData(TOPIC, serializer.serialize(TOPIC, complexStruct));
    SchemaAndValue simpleSchemaAndValue = converter.toConnectData(TOPIC, serializer.serialize(TOPIC, simpleStruct));

    final SinkRecord recordA = new SinkRecord(TOPIC, 0, null, null, complexSchemaAndValue.schema(), complexSchemaAndValue.value(), 0);
    final SinkRecord recordB = new SinkRecord(TOPIC, 0, null, null, simpleSchemaAndValue.schema(), simpleSchemaAndValue.value(), 0);

    writer = newWriter(props);
    writer.write(Collections.singletonList(recordA));

    assertEquals(
            1,
            postgresqlHelper.select(
                    "SELECT * FROM " + TOPIC,
                    new PostgresqlHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals("199", rs.getString("intkey"));
                        assertEquals("id45634", rs.getString("stringkey"));
                        assertEquals("x", rs.getString("arrayitem"));
                        assertEquals("1", rs.getString("mapvalue"));
                        JSONObject expectedJsonObject = new JSONObject(expectedComplex);
                        JSONObject resultJsonObject = new JSONObject(rs.getString("event"));
                        expectedJsonObject.keys().forEachRemaining(k -> assertEquals(resultJsonObject.get(k).toString(), expectedJsonObject.get(k).toString()));
                      }
                    }
            )
    );
    writer.write(Collections.singletonList(recordB));
    assertEquals(
            1,
            postgresqlHelper.select(
                    "SELECT * FROM " + TOPIC + " WHERE intkey ='" + simpleStruct.get("int") +"'",
                    new PostgresqlHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals("13", rs.getString("intkey"));
                        assertEquals("stringvalue", rs.getString("stringkey"));
                        assertNull(rs.getString("arrayitem"));
                        assertNull(rs.getString("mapvalue"));
                        JSONObject expectedJsonObject = new JSONObject(expectedSimple);
                        JSONObject resultJsonObject = new JSONObject(rs.getString("event"));
                        expectedJsonObject.keys().forEachRemaining(k -> assertEquals(resultJsonObject.get(k).toString(), expectedJsonObject.get(k).toString()));
                      }
                    }
            )
    );

    TableId tableId = new TableId(null, null, TOPIC);
    TableDefinition refreshedMetadata = dialect.describeTable(postgresqlHelper.connection, tableId);
    System.out.println(refreshedMetadata);
  }

  @Test
  public void autoCreateWithAutoEvolveJsonFirstValueFromArrayAndMapMultiStructureAllowNonIndex() throws SQLException, RestClientException, IOException {
    Map<String, String> props = new HashMap<>();
    props.put("connection.url", postgresqlHelper.postgreSQL());
    props.put("auto.create", "true");
    props.put("auto.evolve", "true");
    props.put("connection.user", "postgres");
    props.put("connection.password", "password123");
    props.put("zonemapattributes", "intkey");
    props.put("distributionattributes", "stringkey");
    props.put("clusteredattributes", "stringkey, intkey");
    props.put("partitions", "5");
    props.put("value.converter." + JsonConverterConfig.SCHEMA_NAMES, "ComplexSchemaName, SimpleSchemaName");
            props.put("value.converter." +"json.ComplexSchemaName.subrecord1.subrecord2.int32", "true");
            props.put("value.converter." +"json.SimpleSchemaName.stringy", "true");
            props.put("value.converter." +"ComplexSchemaName.subrecord1.subrecord2.int32", "intkey");
            props.put("value.converter." +"ComplexSchemaName.string", "stringkey");
            props.put("value.converter." +"ComplexSchemaName.subrecord1.array", "arrayitem");
            props.put("value.converter." +"ComplexSchemaName.map", "mapvalue");
            props.put("value.converter." +"SimpleSchemaName.int", "intkey");
            props.put("value.converter." +"SimpleSchemaName.stringy", "stringkey");
            props.put("value.converter." + JsonConverterConfig.PAYLOAD_FIELD_NAME, "event");
            props.put("value.converter." +JsonConverterConfig.INPUT_FORMAT, "json");
            props.put("value.converter." +JsonConverterConfig.ALLOWNONINDEXED, "true");
            props.put("value.converter." +AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName());
    ;
    Map<String, String> map = Stream.of(
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://mock:8081"),

            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.SCHEMA_NAMES, "ComplexSchemaName, SimpleSchemaName"),
            new AbstractMap.SimpleImmutableEntry<>("json.ComplexSchemaName.subrecord1.subrecord2.int32", "true"),
            new AbstractMap.SimpleImmutableEntry<>("json.SimpleSchemaName.stringy", "true"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.subrecord1.subrecord2.int32", "intkey"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.string", "stringkey"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.subrecord1.array", "arrayitem"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.map", "mapvalue"),
            new AbstractMap.SimpleImmutableEntry<>("SimpleSchemaName.int", "intkey"),
            new AbstractMap.SimpleImmutableEntry<>("SimpleSchemaName.stringy", "stringkey"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.PAYLOAD_FIELD_NAME, "event"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.INPUT_FORMAT, "json"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.ALLOWNONINDEXED, "true"),
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName())
    )
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    converter.configure(map, false);

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

    org.apache.avro.Schema simpleAvroSchema = org.apache.avro.SchemaBuilder.record("com.example.SimpleSchemaName")
            .fields().requiredInt("int")
            .requiredString("stringy")
            .endRecord();

    GenericRecord simpleRecord = new GenericRecordBuilder(simpleAvroSchema)
            .set("int", 13)
            .set("stringy", "stringvalue")
            .build();

    byte[] converted = record.toString().getBytes();
    byte[] simpleConverted = simpleRecord.toString().getBytes();
    String expected = record.toString();
    String expectedSimple = simpleRecord.toString();
    SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, converted);
    SchemaAndValue simpleSchemaAndValue = converter.toConnectData(TOPIC, simpleConverted);
    System.out.println(schemaAndValue.schema() + "\n" + schemaAndValue.value());
    System.out.println(simpleSchemaAndValue.schema() + "\n" + simpleSchemaAndValue.value());

    final SinkRecord recordA = new SinkRecord(TOPIC, 0, null, null, schemaAndValue.schema(), schemaAndValue.value(), 0);
    final SinkRecord recordB = new SinkRecord(TOPIC, 0, null, null, simpleSchemaAndValue.schema(), simpleSchemaAndValue.value(), 0);

    writer = newWriter(props);
    writer.write(Collections.singletonList(recordA));

    assertEquals(
            1,
            postgresqlHelper.select(
                    "SELECT * FROM " + TOPIC,
                    new PostgresqlHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals("199", rs.getString("intkey"));
                        assertEquals("id45634", rs.getString("stringkey"));
                        assertEquals("x", rs.getString("arrayitem"));
                        assertEquals("1", rs.getString("mapvalue"));
                        JSONObject expectedJsonObject = new JSONObject(expected);
                        JSONObject resultJsonObject = new JSONObject(rs.getString("event"));
                        expectedJsonObject.keys().forEachRemaining(k -> assertEquals(resultJsonObject.get(k).toString(), expectedJsonObject.get(k).toString()));
                      }
                    }
            )
    );
    writer.write(Collections.singletonList(recordB));
    assertEquals(
            1,
            postgresqlHelper.select(
                    "SELECT * FROM " + TOPIC + " WHERE intkey ='" + simpleRecord.get("int") +"'",
                    new PostgresqlHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals("13", rs.getString("intkey"));
                        assertEquals("stringvalue", rs.getString("stringkey"));
                        assertNull(rs.getString("arrayitem"));
                        assertNull(rs.getString("mapvalue"));
                        JSONObject expectedJsonObject = new JSONObject(expectedSimple);
                        JSONObject resultJsonObject = new JSONObject(rs.getString("event"));
                        expectedJsonObject.keys().forEachRemaining(k -> assertEquals(resultJsonObject.get(k).toString(), expectedJsonObject.get(k).toString()));
                      }
                    }
            )
    );

    TableId tableId = new TableId(null, null, TOPIC);
    TableDefinition refreshedMetadata = dialect.describeTable(postgresqlHelper.connection, tableId);
    System.out.println(refreshedMetadata);
  }

  @Test
  public void autoCreateWithAutoEvolveAvroFirstValueFromArrayAndMapMultiStructureAllowNonIndexBatch() throws SQLException, RestClientException, IOException {
    Map<String, String> props = new HashMap<>();
    props.put("connection.url", postgresqlHelper.postgreSQL());
    props.put("auto.create", "true");
    props.put("auto.evolve", "true");
    props.put("value.converter.payload.field.name", "event");
    props.put("connection.user", "postgres");
    props.put("connection.password", "password123");
    props.put("zonemapattributes", "intkey");
    props.put("distributionattributes", "stringkey");
    props.put("clusteredattributes", "stringkey, intkey");
    props.put("partitions", "5");

    Map<String, String> map = Stream.of(
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://mock:8081"),

            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.SCHEMA_NAMES, "ComplexSchemaName, SimpleSchemaName"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.subrecord1.subrecord2.int32", "intkey"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.string", "stringkey"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.subrecord1.array", "arrayitem"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.map", "mapvalue"),
            new AbstractMap.SimpleImmutableEntry<>("SimpleSchemaName.int", "intkey"),
            new AbstractMap.SimpleImmutableEntry<>("SimpleSchemaName.stringy", "stringkey"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.PAYLOAD_FIELD_NAME, "event"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.INPUT_FORMAT, "avro"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.ALLOWNONINDEXED, "true"),
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName())
    )
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    converter.configure(map, false);
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
            .fields().requiredInt("int")
            .requiredString("stringy")
            .endRecord();

    GenericData.Record simpleStruct = new GenericRecordBuilder(simpleAvroSchema)
            .set("int", 13)
            .set("stringy", "stringvalue")
            .build();

    schemaRegistry.register(TOPIC + avroSchema.getFullName() + "-value", avroSchema);
    schemaRegistry.register(TOPIC + simpleAvroSchema.getFullName() + "-value", simpleAvroSchema);

    String expectedComplex = complexStruct.toString();
    String expectedSimple = simpleStruct.toString();

    Map<String, String> smap = Stream.of(
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://mock:8081"),
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName())
    )
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    serializer.configure(smap, false);

    SchemaAndValue complexSchemaAndValue = converter.toConnectData(TOPIC, serializer.serialize(TOPIC, complexStruct));
    SchemaAndValue simpleSchemaAndValue = converter.toConnectData(TOPIC, serializer.serialize(TOPIC, simpleStruct));

    final SinkRecord recordA = new SinkRecord(TOPIC, 0, null, null, complexSchemaAndValue.schema(), complexSchemaAndValue.value(), 0);
    final SinkRecord recordB = new SinkRecord(TOPIC, 0, null, null, simpleSchemaAndValue.schema(), simpleSchemaAndValue.value(), 0);

    writer = newWriter(props);
    writer.write(Arrays.asList(recordA, recordB, recordA, recordB));

    assertEquals(
            1,
            postgresqlHelper.select(
                    "SELECT * FROM " + TOPIC + " WHERE intkey ='" +
                            ((GenericData.Record) ((GenericData.Record) complexStruct.get("subrecord1")).get("subrecord2")).get("int32") +
                            "' LIMIT 1",
                    new PostgresqlHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals("199", rs.getString("intkey"));
                        assertEquals("id45634", rs.getString("stringkey"));
                        assertEquals("x", rs.getString("arrayitem"));
                        assertEquals("1", rs.getString("mapvalue"));
                        JSONObject expectedJsonObject = new JSONObject(expectedComplex);
                        JSONObject resultJsonObject = new JSONObject(rs.getString("event"));
                        expectedJsonObject.keys().forEachRemaining(k -> assertEquals(resultJsonObject.get(k).toString(), expectedJsonObject.get(k).toString()));
                      }
                    }
            )
    );
    assertEquals(
            1,
            postgresqlHelper.select(
                    "SELECT * FROM " + TOPIC + " WHERE intkey ='" + simpleStruct.get("int") +"' LIMIT 1",
                    new PostgresqlHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals("13", rs.getString("intkey"));
                        assertEquals("stringvalue", rs.getString("stringkey"));
                        assertNull(rs.getString("arrayitem"));
                        assertNull(rs.getString("mapvalue"));
                        JSONObject expectedJsonObject = new JSONObject(expectedSimple);
                        JSONObject resultJsonObject = new JSONObject(rs.getString("event"));
                        expectedJsonObject.keys().forEachRemaining(k -> assertEquals(resultJsonObject.get(k).toString(), expectedJsonObject.get(k).toString()));
                      }
                    }
            )
    );
    assertEquals(
            1,
            postgresqlHelper.select(
                    "SELECT count(*) FROM " + TOPIC ,
                    new PostgresqlHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                           assertEquals(4, rs.getInt(1));            }
                    }
            )
    );

    TableId tableId = new TableId(null, null, TOPIC);
    TableDefinition refreshedMetadata = dialect.describeTable(postgresqlHelper.connection, tableId);
    System.out.println(refreshedMetadata);
  }

  @Test
  public void autoCreateWithAutoEvolveAvroFirstValueFromArrayAndMapMultiStructureAllowNonIndexDeleteButDeleteDisabled() throws SQLException, RestClientException, IOException {
    Map<String, String> props = new HashMap<>();
    props.put("connection.url", postgresqlHelper.postgreSQL());
    props.put("auto.create", "true");
    props.put("auto.evolve", "true");
    props.put("delete.enabled", "false");
    props.put("delete.keys", "intkey");
    props.put("value.converter.payload.field.name", "event");
    props.put("connection.user", "postgres");
    props.put("connection.password", "password123");
    props.put("zonemapattributes", "intkey");
    props.put("distributionattributes", "stringkey");
    props.put("clusteredattributes", "stringkey, intkey");
    props.put("partitions", "5");


    Map<String, String> map = Stream.of(
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://mock:8081"),

            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.SCHEMA_NAMES, "ComplexSchemaName, SimpleSchemaName"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.subrecord1.subrecord2.int32", "intkey"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.string", "stringkey"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.subrecord1.array", "arrayitem"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.map", "mapvalue"),
            new AbstractMap.SimpleImmutableEntry<>("SimpleSchemaName.int", "intkey"),
            new AbstractMap.SimpleImmutableEntry<>("SimpleSchemaName.stringy", "stringkey"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.PAYLOAD_FIELD_NAME, "event"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.INPUT_FORMAT, "avro"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.ALLOWNONINDEXED, "true"),
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName())
    )
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    converter.configure(map, false);

    SchemaAndValue tombstone = new SchemaAndValue(SchemaBuilder.STRING_SCHEMA, "199");

    final SinkRecord recordC = new SinkRecord(TOPIC, 0, tombstone.schema(), tombstone.value(), null, null, 0);


    writer = newWriter(props);

    ConnectException e = assertThrows(ConnectException.class, () -> writer.write(Collections.singletonList(recordC)));
    assertTrue(
            e.getMessage(),
            e.getMessage().startsWith("Sink connector 'null' is configured with 'delete.enabled=false' and therefore requires records with a non-null Struct value and non-null Struct schema")
    );

    TableId tableId = new TableId(null, null, TOPIC);
    TableDefinition refreshedMetadata = dialect.describeTable(postgresqlHelper.connection, tableId);
    System.out.println(refreshedMetadata);
  }

  @Test
  public void autoCreateWithAutoEvolveAvroFirstValueFromArrayAndMapMultiStructureAllowNonIndexDeleteButDeleteEnabled() throws SQLException, RestClientException, IOException {
    Map<String, String> props = new HashMap<>();
    props.put("connection.url", postgresqlHelper.postgreSQL());
    props.put("auto.create", "true");
    props.put("auto.evolve", "true");
    props.put("delete.enabled", "true");
    props.put("delete.keys", "intkey");
    props.put("value.converter.payload.field.name", "event");
    props.put("connection.user", "postgres");
    props.put("connection.password", "password123");
    props.put("zonemapattributes", "intkey");
    props.put("distributionattributes", "stringkey");
    props.put("clusteredattributes", "stringkey, intkey");
    props.put("partitions", "5");


    Map<String, String> map = Stream.of(
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://mock:8081"),

            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.SCHEMA_NAMES, "ComplexSchemaName, SimpleSchemaName"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.subrecord1.subrecord2.int32", "intkey"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.string", "stringkey"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.subrecord1.array", "arrayitem"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.map", "mapvalue"),
            new AbstractMap.SimpleImmutableEntry<>("SimpleSchemaName.int", "intkey"),
            new AbstractMap.SimpleImmutableEntry<>("SimpleSchemaName.stringy", "stringkey"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.PAYLOAD_FIELD_NAME, "event"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.INPUT_FORMAT, "avro"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.ALLOWNONINDEXED, "true"),
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName())
    )
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    converter.configure(map, false);
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
            .fields().requiredInt("int")
            .requiredString("stringy")
            .endRecord();

    GenericData.Record simpleStruct = new GenericRecordBuilder(simpleAvroSchema)
            .set("int", 199)
            .set("stringy", "stringvalue")
            .build();

    schemaRegistry.register(TOPIC + avroSchema.getFullName() + "-value", avroSchema);
    schemaRegistry.register(TOPIC + simpleAvroSchema.getFullName() + "-value", simpleAvroSchema);

    String expectedComplex = complexStruct.toString();
    String expectedSimple = simpleStruct.toString();

    Map<String, String> smap = Stream.of(
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://mock:8081"),
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName())
    )
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    serializer.configure(smap, false);

    SchemaAndValue complexSchemaAndValue = converter.toConnectData(TOPIC, serializer.serialize(TOPIC, complexStruct));
    SchemaAndValue simpleSchemaAndValue = converter.toConnectData(TOPIC, serializer.serialize(TOPIC, simpleStruct));
    SchemaAndValue tombstone = new SchemaAndValue(SchemaBuilder.STRING_SCHEMA, "199");

    final SinkRecord recordA = new SinkRecord(TOPIC, 0, tombstone.schema(), tombstone.value(), complexSchemaAndValue.schema(), complexSchemaAndValue.value(), 0);
    final SinkRecord recordB = new SinkRecord(TOPIC, 0, tombstone.schema(), tombstone.value(), simpleSchemaAndValue.schema(), simpleSchemaAndValue.value(), 1);
    final SinkRecord recordC = new SinkRecord(TOPIC, 0, tombstone.schema(), tombstone.value(), null, null, 2);
    final SinkRecord recordD = new SinkRecord(TOPIC, 0, tombstone.schema(), tombstone.value(), complexSchemaAndValue.schema(), complexSchemaAndValue.value(), 0);


    writer = newWriter(props);
    writer.write(Arrays.asList(recordA, recordB, recordA, recordB, recordC, recordD));

    assertEquals(
            1,
            postgresqlHelper.select(
                    "SELECT * FROM " + TOPIC + " WHERE intkey ='" +
                            ((GenericData.Record) ((GenericData.Record) complexStruct.get("subrecord1")).get("subrecord2")).get("int32") +
                            "' LIMIT 1",
                    new PostgresqlHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals("199", rs.getString("intkey"));
                        assertEquals("id45634", rs.getString("stringkey"));
                        assertEquals("x", rs.getString("arrayitem"));
                        assertEquals("1", rs.getString("mapvalue"));
                        JSONObject expectedJsonObject = new JSONObject(expectedComplex);
                        JSONObject resultJsonObject = new JSONObject(rs.getString("event"));
                        expectedJsonObject.keys().forEachRemaining(k -> assertEquals(resultJsonObject.get(k).toString(), expectedJsonObject.get(k).toString()));
                      }
                    }
            )
    );
    assertEquals(
            1,
            postgresqlHelper.select(
                    "SELECT count(*) FROM " + TOPIC ,
                    new PostgresqlHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals(1, rs.getInt(1));            }
                    }
            )
    );

    TableId tableId = new TableId(null, null, TOPIC);
    TableDefinition refreshedMetadata = dialect.describeTable(postgresqlHelper.connection, tableId);
    System.out.println(refreshedMetadata);
  }

  @Test
  public void autoCreateWithAutoEvolveAvroFirstValueFromArrayAndMapMultiStructureAllowNonIndexDeleteButDeleteKeyMissing () throws SQLException, RestClientException, IOException {
    Map<String, String> props = new HashMap<>();
    props.put("connection.url", postgresqlHelper.postgreSQL());
    props.put("auto.create", "true");
    props.put("auto.evolve", "true");
    props.put("delete.enabled", "true");
    props.put("delete.keys", "intkey2");
    props.put("value.converter.payload.field.name", "event");
    props.put("connection.user", "postgres");
    props.put("connection.password", "password123");
    props.put("zonemapattributes", "intkey");
    props.put("distributionattributes", "stringkey");
    props.put("clusteredattributes", "stringkey, intkey");
    props.put("partitions", "5");


    Map<String, String> map = Stream.of(
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://mock:8081"),

            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.SCHEMA_NAMES, "ComplexSchemaName, SimpleSchemaName"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.subrecord1.subrecord2.int32", "intkey2"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.int32", "intkey"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.string", "stringkey"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.subrecord1.array", "arrayitem"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.map", "mapvalue"),
            new AbstractMap.SimpleImmutableEntry<>("SimpleSchemaName.int", "intkey2"),
            new AbstractMap.SimpleImmutableEntry<>("SimpleSchemaName.stringy", "stringkey"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.PAYLOAD_FIELD_NAME, "event"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.INPUT_FORMAT, "avro"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.ALLOWNONINDEXED, "true"),
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName())
    )
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    converter.configure(map, false);
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
                            //.set("int32", 199)
                            .build())
                    .set("array", Collections.singletonList("x"))
                    .build())
            .build();

    org.apache.avro.Schema simpleAvroSchema = org.apache.avro.SchemaBuilder.record("com.example.SimpleSchemaName")
            .fields().requiredInt("int")
            .requiredString("stringy")
            .endRecord();

    GenericData.Record simpleStruct = new GenericRecordBuilder(simpleAvroSchema)
            .set("int", 199)
            .set("stringy", "stringvalue")
            .build();

    schemaRegistry.register(TOPIC + avroSchema.getFullName() + "-value", avroSchema);
    schemaRegistry.register(TOPIC + simpleAvroSchema.getFullName() + "-value", simpleAvroSchema);

    String expectedComplex = complexStruct.toString();
    String expectedSimple = simpleStruct.toString();

    Map<String, String> smap = Stream.of(
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://mock:8081"),
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName())
    )
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    serializer.configure(smap, false);

    SchemaAndValue complexSchemaAndValue = converter.toConnectData(TOPIC, serializer.serialize(TOPIC, complexStruct));
    SchemaAndValue simpleSchemaAndValue = converter.toConnectData(TOPIC, serializer.serialize(TOPIC, simpleStruct));
    SchemaAndValue tombstone = new SchemaAndValue(SchemaBuilder.STRING_SCHEMA, "199");

    final SinkRecord recordA = new SinkRecord(TOPIC, 0, tombstone.schema(), tombstone.value(), complexSchemaAndValue.schema(), complexSchemaAndValue.value(), 0);
    final SinkRecord recordB = new SinkRecord(TOPIC, 0, tombstone.schema(), tombstone.value(), simpleSchemaAndValue.schema(), simpleSchemaAndValue.value(), 1);
    final SinkRecord recordC = new SinkRecord(TOPIC, 0, tombstone.schema(), tombstone.value(), null, null, 2);
    final SinkRecord recordD = new SinkRecord(TOPIC, 0, tombstone.schema(), tombstone.value(), complexSchemaAndValue.schema(), complexSchemaAndValue.value(), 0);


    writer = newWriter(props);
    ConnectException e = assertThrows(ConnectException.class, () -> writer.write(Arrays.asList(recordA, recordB, recordA, recordB, recordC, recordD)));
    assertTrue(
            e.getMessage(),
            e.getMessage().startsWith("Can't insert record with null for key value used")
    );

    TableId tableId = new TableId(null, null, TOPIC);
    TableDefinition refreshedMetadata = dialect.describeTable(postgresqlHelper.connection, tableId);
    System.out.println(refreshedMetadata);
  }

  @Test
  public void autoCreateWithAutoEvolveAvroFirstValueFromArrayAndMapMultiStructureAllowNonIndexDeleteFirstTombstone() throws SQLException, RestClientException, IOException {
    Map<String, String> props = new HashMap<>();
    props.put("connection.url", postgresqlHelper.postgreSQL());
    props.put("auto.create", "true");
    props.put("auto.evolve", "true");
    props.put("delete.enabled", "true");
    props.put("delete.keys", "intkey");
    props.put("value.converter.payload.field.name", "event");
    props.put("connection.user", "postgres");
    props.put("connection.password", "password123");
    props.put("zonemapattributes", "intkey");
    props.put("distributionattributes", "stringkey");
    props.put("clusteredattributes", "stringkey, intkey");
    props.put("partitions", "5");


    Map<String, String> map = Stream.of(
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://mock:8081"),

            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.SCHEMA_NAMES, "ComplexSchemaName, SimpleSchemaName"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.subrecord1.subrecord2.int32", "intkey"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.string", "stringkey"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.subrecord1.array", "arrayitem"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.map", "mapvalue"),
            new AbstractMap.SimpleImmutableEntry<>("SimpleSchemaName.int", "intkey"),
            new AbstractMap.SimpleImmutableEntry<>("SimpleSchemaName.stringy", "stringkey"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.PAYLOAD_FIELD_NAME, "event"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.INPUT_FORMAT, "avro"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.ALLOWNONINDEXED, "true"),
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName())
    )
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    converter.configure(map, false);

    SchemaAndValue tombstone = new SchemaAndValue(SchemaBuilder.STRING_SCHEMA, "199");

    final SinkRecord recordC = new SinkRecord(TOPIC, 0, tombstone.schema(), tombstone.value(), null, null, 0);


    writer = newWriter(props);
    writer.write(Collections.singletonList(recordC));
    /*ConnectException e = assertThrows(ConnectException.class, () -> writer.write(Collections.singletonList(recordC)));
    assertTrue(
            e.getMessage(),
            e.getMessage().startsWith("Sink connector 'null' is configured with 'delete.enabled=false' and therefore requires records with a non-null Struct value and non-null Struct schema")
    );*/

    TableId tableId = new TableId(null, null, TOPIC);
    TableDefinition refreshedMetadata = dialect.describeTable(postgresqlHelper.connection, tableId);
    System.out.println(refreshedMetadata);
  }

  @Test
  public void autoCreateWithAutoEvolveAvroFirstValueFromArrayAndMapMultiStructureAllowNonIndexUpsertAndDelete() throws SQLException, RestClientException, IOException {
    Map<String, String> props = new HashMap<>();
    props.put("connection.url", postgresqlHelper.postgreSQL());
    props.put("auto.create", "true");
    props.put("auto.evolve", "true");
    props.put("delete.enabled", "true");
    props.put("insert.mode", "upsert");
    props.put("delete.keys", "intkey");
    props.put("upsert.keys", "intkey, stringkey");
    props.put("value.converter.payload.field.name", "event");
    props.put("connection.user", "postgres");
    props.put("connection.password", "password123");
    props.put("zonemapattributes", "intkey");
    props.put("distributionattributes", "stringkey");
    props.put("clusteredattributes", "stringkey, intkey");
    props.put("partitions", "5");


    Map<String, String> map = Stream.of(
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://mock:8081"),

            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.SCHEMA_NAMES, "ComplexSchemaName, SimpleSchemaName"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.subrecord1.subrecord2.int32", "intkey"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.string", "stringkey"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.subrecord1.array", "arrayitem"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.map", "mapvalue"),
            new AbstractMap.SimpleImmutableEntry<>("SimpleSchemaName.int", "intkey"),
            new AbstractMap.SimpleImmutableEntry<>("SimpleSchemaName.stringy", "stringkey"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.PAYLOAD_FIELD_NAME, "event"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.INPUT_FORMAT, "avro"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.ALLOWNONINDEXED, "true"),
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName())
    )
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    converter.configure(map, false);
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
            .fields().requiredInt("int")
            .requiredString("stringy")
            .endRecord();

    GenericData.Record simpleStruct = new GenericRecordBuilder(simpleAvroSchema)
            .set("int", 199)
            .set("stringy", "stringvalue")
            .build();

    schemaRegistry.register(TOPIC + avroSchema.getFullName() + "-value", avroSchema);
    schemaRegistry.register(TOPIC + simpleAvroSchema.getFullName() + "-value", simpleAvroSchema);

    String expectedSimple = simpleStruct.toString();

    Map<String, String> smap = Stream.of(
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://mock:8081"),
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName())
    )
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    serializer.configure(smap, false);

    GenericData.Record complexStruct2 = new GenericRecordBuilder(avroSchema)
            .set("int8", 2)
            .set("int16", 2)
            .set("int32", 2)
            .set("int64", 2L)
            .set("float32", 2.2f)
            .set("boolean", true)
            .set("string", "id45634")
            .set("bytes", ByteBuffer.wrap("2".getBytes()))
            .set("array", Arrays.asList("x", "y", "z"))
            .set("map", Collections.singletonMap("field", 2))
            .set("subrecord1", new GenericRecordBuilder(subrecord1Schema)
                    .set("subrecord2", new GenericRecordBuilder(subrecord2Schema)
                            .set("int32", 199)
                            .build())
                    .set("array", Collections.singletonList("f"))
                    .build())
            .build();

    String expectedComplex = complexStruct2.toString();
    SchemaAndValue complexSchemaAndValue = converter.toConnectData(TOPIC, serializer.serialize(TOPIC, complexStruct));
    SchemaAndValue complexSchemaAndValue2 = converter.toConnectData(TOPIC, serializer.serialize(TOPIC, complexStruct2));

    SchemaAndValue simpleSchemaAndValue = converter.toConnectData(TOPIC, serializer.serialize(TOPIC, simpleStruct));
    SchemaAndValue tombstone = new SchemaAndValue(SchemaBuilder.STRING_SCHEMA, "199");

    final SinkRecord recordA = new SinkRecord(TOPIC, 0, tombstone.schema(), tombstone.value(), complexSchemaAndValue.schema(), complexSchemaAndValue.value(), 0);
    final SinkRecord recordB = new SinkRecord(TOPIC, 0, tombstone.schema(), tombstone.value(), simpleSchemaAndValue.schema(), simpleSchemaAndValue.value(), 1);
    final SinkRecord recordC = new SinkRecord(TOPIC, 0, tombstone.schema(), tombstone.value(), null, null, 2);
    final SinkRecord recordD = new SinkRecord(TOPIC, 0, tombstone.schema(), tombstone.value(), complexSchemaAndValue.schema(), complexSchemaAndValue2.value(), 3);


    writer = newWriter(props);
    writer.write(Arrays.asList(recordA, recordB, recordC, recordB, recordA, recordD));

    assertEquals(
            1,
            postgresqlHelper.select(
                    "SELECT * FROM " + TOPIC + " WHERE intkey ='" +
                            ((GenericData.Record) ((GenericData.Record) complexStruct2.get("subrecord1")).get("subrecord2")).get("int32") + "'" +
                            " AND stringkey ='" +
                            complexStruct2.get("string") + "'"
                            ,
                    new PostgresqlHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals("199", rs.getString("intkey"));
                        assertEquals("id45634", rs.getString("stringkey"));
                        assertEquals("f", rs.getString("arrayitem"));
                        assertEquals("2", rs.getString("mapvalue"));
                        JSONObject expectedJsonObject = new JSONObject(expectedComplex);
                        JSONObject resultJsonObject = new JSONObject(rs.getString("event"));
                        expectedJsonObject.keys().forEachRemaining(k -> assertEquals(resultJsonObject.get(k).toString(), expectedJsonObject.get(k).toString()));
                      }
                    }
            )
    );
    assertEquals(
            1,
            postgresqlHelper.select(
                    "SELECT * FROM " + TOPIC + " WHERE intkey ='" +
                            simpleStruct.get("int") + "'" +
                            " AND stringkey ='" +
                            simpleStruct.get("stringy") + "'"
                    ,
                    new PostgresqlHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals("199", rs.getString("intkey"));
                        assertEquals("stringvalue", rs.getString("stringkey"));
                        JSONObject expectedJsonObject = new JSONObject(expectedSimple);
                        JSONObject resultJsonObject = new JSONObject(rs.getString("event"));
                        expectedJsonObject.keys().forEachRemaining(k -> assertEquals(resultJsonObject.get(k).toString(), expectedJsonObject.get(k).toString()));
                      }
                    }
            )
    );
    assertEquals(
            1,
            postgresqlHelper.select(
                    "SELECT count(*) FROM " + TOPIC ,
                    new PostgresqlHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals(2, rs.getInt(1));            }
                    }
            )
    );

    TableId tableId = new TableId(null, null, TOPIC);
    TableDefinition refreshedMetadata = dialect.describeTable(postgresqlHelper.connection, tableId);
    System.out.println(refreshedMetadata);
  }

  @Test
  public void autoCreateWithAutoEvolveAvroFirstValueFromArrayAndMapMultiStructureAllowNonIndexUpsertAndDeleteNonBatch() throws SQLException, RestClientException, IOException {
    Map<String, String> props = new HashMap<>();
    props.put("connection.url", postgresqlHelper.postgreSQL());
    props.put("auto.create", "true");
    props.put("auto.evolve", "true");
    props.put("delete.enabled", "true");
    props.put("insert.mode", "upsert");
    props.put("delete.keys", "intkey");
    props.put("upsert.keys", "intkey, stringkey");
    props.put("value.converter.payload.field.name", "event");
    props.put("connection.user", "postgres");
    props.put("connection.password", "password123");
    props.put("zonemapattributes", "intkey");
    props.put("distributionattributes", "stringkey");
    props.put("clusteredattributes", "stringkey, intkey");
    props.put("partitions", "5");


    Map<String, String> map = Stream.of(
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://mock:8081"),

            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.SCHEMA_NAMES, "ComplexSchemaName, SimpleSchemaName"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.subrecord1.subrecord2.int32", "intkey"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.string", "stringkey"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.subrecord1.array", "arrayitem"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.map", "mapvalue"),
            new AbstractMap.SimpleImmutableEntry<>("SimpleSchemaName.int", "intkey"),
            new AbstractMap.SimpleImmutableEntry<>("SimpleSchemaName.stringy", "stringkey"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.PAYLOAD_FIELD_NAME, "event"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.INPUT_FORMAT, "avro"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.ALLOWNONINDEXED, "true"),
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName())
    )
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    converter.configure(map, false);
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
            .fields().requiredInt("int")
            .requiredString("stringy")
            .endRecord();

    GenericData.Record simpleStruct = new GenericRecordBuilder(simpleAvroSchema)
            .set("int", 199)
            .set("stringy", "stringvalue")
            .build();

    schemaRegistry.register(TOPIC + avroSchema.getFullName() + "-value", avroSchema);
    schemaRegistry.register(TOPIC + simpleAvroSchema.getFullName() + "-value", simpleAvroSchema);

    String expectedSimple = simpleStruct.toString();

    Map<String, String> smap = Stream.of(
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://mock:8081"),
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName())
    )
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    serializer.configure(smap, false);

    GenericData.Record complexStruct2 = new GenericRecordBuilder(avroSchema)
            .set("int8", 2)
            .set("int16", 2)
            .set("int32", 2)
            .set("int64", 2L)
            .set("float32", 2.2f)
            .set("boolean", true)
            .set("string", "id45634")
            .set("bytes", ByteBuffer.wrap("2".getBytes()))
            .set("array", Arrays.asList("x", "y", "z"))
            .set("map", Collections.singletonMap("field", 2))
            .set("subrecord1", new GenericRecordBuilder(subrecord1Schema)
                    .set("subrecord2", new GenericRecordBuilder(subrecord2Schema)
                            .set("int32", 199)
                            .build())
                    .set("array", Collections.singletonList("f"))
                    .build())
            .build();

    String expectedComplex = complexStruct2.toString();
    SchemaAndValue complexSchemaAndValue = converter.toConnectData(TOPIC, serializer.serialize(TOPIC, complexStruct));
    SchemaAndValue complexSchemaAndValue2 = converter.toConnectData(TOPIC, serializer.serialize(TOPIC, complexStruct2));

    SchemaAndValue simpleSchemaAndValue = converter.toConnectData(TOPIC, serializer.serialize(TOPIC, simpleStruct));
    SchemaAndValue tombstone = new SchemaAndValue(SchemaBuilder.STRING_SCHEMA, "199");

    final SinkRecord recordA = new SinkRecord(TOPIC, 0, tombstone.schema(), tombstone.value(), complexSchemaAndValue.schema(), complexSchemaAndValue.value(), 0);
    final SinkRecord recordB = new SinkRecord(TOPIC, 0, tombstone.schema(), tombstone.value(), simpleSchemaAndValue.schema(), simpleSchemaAndValue.value(), 1);
    final SinkRecord recordC = new SinkRecord(TOPIC, 0, tombstone.schema(), tombstone.value(), null, null, 2);
    final SinkRecord recordD = new SinkRecord(TOPIC, 0, tombstone.schema(), tombstone.value(), complexSchemaAndValue.schema(), complexSchemaAndValue2.value(), 3);


    writer = newWriter(props);
    writer.write(Collections.singletonList(recordA));
    writer.write(Collections.singletonList(recordB));
    writer.write(Collections.singletonList(recordC));
    writer.write(Collections.singletonList(recordB));
    writer.write(Collections.singletonList(recordA));
    writer.write(Collections.singletonList(recordD));

    assertEquals(
            1,
            postgresqlHelper.select(
                    "SELECT * FROM " + TOPIC + " WHERE intkey ='" +
                            ((GenericData.Record) ((GenericData.Record) complexStruct2.get("subrecord1")).get("subrecord2")).get("int32") + "'" +
                            " AND stringkey ='" +
                            complexStruct2.get("string") + "'"
                    ,
                    new PostgresqlHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals("199", rs.getString("intkey"));
                        assertEquals("id45634", rs.getString("stringkey"));
                        assertEquals("f", rs.getString("arrayitem"));
                        assertEquals("2", rs.getString("mapvalue"));
                        JSONObject expectedJsonObject = new JSONObject(expectedComplex);
                        JSONObject resultJsonObject = new JSONObject(rs.getString("event"));
                        expectedJsonObject.keys().forEachRemaining(k -> assertEquals(resultJsonObject.get(k).toString(), expectedJsonObject.get(k).toString()));
                      }
                    }
            )
    );
    assertEquals(
            1,
            postgresqlHelper.select(
                    "SELECT * FROM " + TOPIC + " WHERE intkey ='" +
                            simpleStruct.get("int") + "'" +
                            " AND stringkey ='" +
                            simpleStruct.get("stringy") + "'"
                    ,
                    new PostgresqlHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals("199", rs.getString("intkey"));
                        assertEquals("stringvalue", rs.getString("stringkey"));
                        JSONObject expectedJsonObject = new JSONObject(expectedSimple);
                        JSONObject resultJsonObject = new JSONObject(rs.getString("event"));
                        expectedJsonObject.keys().forEachRemaining(k -> assertEquals(resultJsonObject.get(k).toString(), expectedJsonObject.get(k).toString()));
                      }
                    }
            )
    );
    assertEquals(
            1,
            postgresqlHelper.select(
                    "SELECT count(*) FROM " + TOPIC ,
                    new PostgresqlHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals(2, rs.getInt(1));            }
                    }
            )
    );

    TableId tableId = new TableId(null, null, TOPIC);
    TableDefinition refreshedMetadata = dialect.describeTable(postgresqlHelper.connection, tableId);
    System.out.println(refreshedMetadata);
  }

  @Test
  public void autoCreateWithAutoEvolveAvroFirstValueFromArrayAndMapMultiStructureAllowNonIndexUpsertAndDeletePreExisting() throws SQLException, RestClientException, IOException {
    Map<String, String> props = new HashMap<>();
    props.put("connection.url", postgresqlHelper.postgreSQL());
    props.put("auto.create", "true");
    props.put("auto.evolve", "true");
    props.put("delete.enabled", "true");
    props.put("insert.mode", "upsert");
    props.put("delete.keys", "intkey");
    props.put("upsert.keys", "intkey, stringkey");
    props.put("value.converter.payload.field.name", "event");
    props.put("connection.user", "postgres");
    props.put("connection.password", "password123");
    props.put("zonemapattributes", "intkey");
    props.put("distributionattributes", "stringkey");
    props.put("clusteredattributes", "stringkey, intkey");
    props.put("partitions", "5");


    Map<String, String> map = Stream.of(
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://mock:8081"),

            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.SCHEMA_NAMES, "ComplexSchemaName, SimpleSchemaName"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.subrecord1.subrecord2.int32", "intkey"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.string", "stringkey"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.subrecord1.array", "arrayitem"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.map", "mapvalue"),
            new AbstractMap.SimpleImmutableEntry<>("SimpleSchemaName.int", "intkey"),
            new AbstractMap.SimpleImmutableEntry<>("SimpleSchemaName.stringy", "stringkey"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.PAYLOAD_FIELD_NAME, "event"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.INPUT_FORMAT, "avro"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.ALLOWNONINDEXED, "true"),
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName())
    )
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    converter.configure(map, false);
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
    postgresqlHelper.createTable(
            "CREATE TABLE " + TOPIC + "(" +
                    "    intkey  TEXT," +
                    "    mapvalue  TEXT," +
                    "    arrayitem  TEXT," +
                    "    stringkey  TEXT," +
                    "    event  JSONB);"
    );
    postgresqlHelper.execute("insert into " + TOPIC +" values ('199', null, null, 'stringvalue', '{\"int\": 199, \"stringy\": \"stringvalue\"}' );");
    postgresqlHelper.execute("insert into " + TOPIC +" values ('199', '2', 'f', 'id45634', '{\"map\": {\"field\": 2}, \"int8\": 2, \"array\": [\"x\", \"y\", \"z\"], \"bytes\": \"2\", \"int16\": 2, \"int32\": 2, \"int64\": 2, \"string\": \"id45634\", \"boolean\": true, \"float32\": 2.2, \"subrecord1\": {\"array\": [\"f\"], \"subrecord2\": {\"int32\": 199}}}' );");

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
            .fields().requiredInt("int")
            .requiredString("stringy")
            .endRecord();

    GenericData.Record simpleStruct = new GenericRecordBuilder(simpleAvroSchema)
            .set("int", 199)
            .set("stringy", "stringvalue")
            .build();

    schemaRegistry.register(TOPIC + avroSchema.getFullName() + "-value", avroSchema);
    schemaRegistry.register(TOPIC + simpleAvroSchema.getFullName() + "-value", simpleAvroSchema);

    String expectedSimple = simpleStruct.toString();

    Map<String, String> smap = Stream.of(
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://mock:8081"),
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName())
    )
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    serializer.configure(smap, false);

    GenericData.Record complexStruct2 = new GenericRecordBuilder(avroSchema)
            .set("int8", 2)
            .set("int16", 2)
            .set("int32", 2)
            .set("int64", 2L)
            .set("float32", 2.2f)
            .set("boolean", true)
            .set("string", "id45634")
            .set("bytes", ByteBuffer.wrap("2".getBytes()))
            .set("array", Arrays.asList("x", "y", "z"))
            .set("map", Collections.singletonMap("field", 2))
            .set("subrecord1", new GenericRecordBuilder(subrecord1Schema)
                    .set("subrecord2", new GenericRecordBuilder(subrecord2Schema)
                            .set("int32", 199)
                            .build())
                    .set("array", Collections.singletonList("f"))
                    .build())
            .build();

    String expectedComplex = complexStruct2.toString();
    SchemaAndValue complexSchemaAndValue = converter.toConnectData(TOPIC, serializer.serialize(TOPIC, complexStruct));
    SchemaAndValue complexSchemaAndValue2 = converter.toConnectData(TOPIC, serializer.serialize(TOPIC, complexStruct2));

    SchemaAndValue simpleSchemaAndValue = converter.toConnectData(TOPIC, serializer.serialize(TOPIC, simpleStruct));
    SchemaAndValue tombstone = new SchemaAndValue(SchemaBuilder.STRING_SCHEMA, "199");

    final SinkRecord recordA = new SinkRecord(TOPIC, 0, tombstone.schema(), tombstone.value(), complexSchemaAndValue.schema(), complexSchemaAndValue.value(), 0);
    final SinkRecord recordB = new SinkRecord(TOPIC, 0, tombstone.schema(), tombstone.value(), simpleSchemaAndValue.schema(), simpleSchemaAndValue.value(), 1);
    final SinkRecord recordC = new SinkRecord(TOPIC, 0, tombstone.schema(), tombstone.value(), null, null, 2);
    final SinkRecord recordD = new SinkRecord(TOPIC, 0, tombstone.schema(), tombstone.value(), complexSchemaAndValue.schema(), complexSchemaAndValue2.value(), 3);


    writer = newWriter(props);
    writer.write(Arrays.asList(recordA, recordD, recordC, recordA, recordD, recordB, recordB, recordB));

    assertEquals(
            1,
            postgresqlHelper.select(
                    "SELECT * FROM " + TOPIC + " WHERE intkey ='" +
                            ((GenericData.Record) ((GenericData.Record) complexStruct2.get("subrecord1")).get("subrecord2")).get("int32") + "'" +
                            " AND stringkey ='" +
                            complexStruct2.get("string") + "'"
                    ,
                    new PostgresqlHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals("199", rs.getString("intkey"));
                        assertEquals("id45634", rs.getString("stringkey"));
                        assertEquals("f", rs.getString("arrayitem"));
                        assertEquals("2", rs.getString("mapvalue"));
                        JSONObject expectedJsonObject = new JSONObject(expectedComplex);
                        JSONObject resultJsonObject = new JSONObject(rs.getString("event"));
                        expectedJsonObject.keys().forEachRemaining(k -> assertEquals(resultJsonObject.get(k).toString(), expectedJsonObject.get(k).toString()));
                      }
                    }
            )
    );
    assertEquals(
            1,
            postgresqlHelper.select(
                    "SELECT * FROM " + TOPIC + " WHERE intkey ='" +
                            simpleStruct.get("int") + "'" +
                            " AND stringkey ='" +
                            simpleStruct.get("stringy") + "'"
                    ,
                    new PostgresqlHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals("199", rs.getString("intkey"));
                        assertEquals("stringvalue", rs.getString("stringkey"));
                        JSONObject expectedJsonObject = new JSONObject(expectedSimple);
                        JSONObject resultJsonObject = new JSONObject(rs.getString("event"));
                        expectedJsonObject.keys().forEachRemaining(k -> assertEquals(resultJsonObject.get(k).toString(), expectedJsonObject.get(k).toString()));
                      }
                    }
            )
    );
    assertEquals(
            1,
            postgresqlHelper.select(
                    "SELECT count(*) FROM " + TOPIC ,
                    new PostgresqlHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals(2, rs.getInt(1));            }
                    }
            )
    );

    TableId tableId = new TableId(null, null, TOPIC);
    TableDefinition refreshedMetadata = dialect.describeTable(postgresqlHelper.connection, tableId);
    System.out.println(refreshedMetadata);
  }

  @Test
  public void autoCreateWithAutoEvolveAvroFirstValueFromArrayAndMapMultiStructureAllowNonIndexUpdateAndDeletePreExisting() throws SQLException, RestClientException, IOException {
    Map<String, String> props = new HashMap<>();
    props.put("connection.url", postgresqlHelper.postgreSQL());
    props.put("auto.create", "true");
    props.put("auto.evolve", "true");
    props.put("delete.enabled", "true");
    props.put("insert.mode", "update");
    props.put("delete.keys", "intkey");
    props.put("upsert.keys", "intkey, stringkey");
    props.put("value.converter.payload.field.name", "event");
    props.put("connection.user", "postgres");
    props.put("connection.password", "password123");
    props.put("zonemapattributes", "intkey");
    props.put("distributionattributes", "stringkey");
    props.put("clusteredattributes", "stringkey, intkey");
    props.put("partitions", "5");


    Map<String, String> map = Stream.of(
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://mock:8081"),

            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.SCHEMA_NAMES, "ComplexSchemaName, SimpleSchemaName"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.subrecord1.subrecord2.int32", "intkey"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.string", "stringkey"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.subrecord1.array", "arrayitem"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.map", "mapvalue"),
            new AbstractMap.SimpleImmutableEntry<>("SimpleSchemaName.int", "intkey"),
            new AbstractMap.SimpleImmutableEntry<>("SimpleSchemaName.stringy", "stringkey"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.PAYLOAD_FIELD_NAME, "event"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.INPUT_FORMAT, "avro"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.ALLOWNONINDEXED, "true"),
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName())
    )
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    converter.configure(map, false);
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
    postgresqlHelper.createTable(
            "CREATE TABLE " + TOPIC + "(" +
                    "    intkey  TEXT," +
                    "    mapvalue  TEXT," +
                    "    arrayitem  TEXT," +
                    "    stringkey  TEXT," +
                    "    event  JSONB);"
    );
    postgresqlHelper.execute("insert into " + TOPIC +" values ('199', null, null, 'stringvalue', '{\"int\": 199, \"stringy\": \"stringvalue\"}' );");
    postgresqlHelper.execute("insert into " + TOPIC +" values ('199', '2', 'f', 'id45634', '{\"map\": {\"field\": 2}, \"int8\": 2, \"array\": [\"x\", \"y\", \"z\"], \"bytes\": \"2\", \"int16\": 2, \"int32\": 2, \"int64\": 2, \"string\": \"id45634\", \"boolean\": true, \"float32\": 2.2, \"subrecord1\": {\"array\": [\"f\"], \"subrecord2\": {\"int32\": 199}}}' );");

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
            .fields().requiredInt("int")
            .requiredString("stringy")
            .endRecord();

    GenericData.Record simpleStruct = new GenericRecordBuilder(simpleAvroSchema)
            .set("int", 199)
            .set("stringy", "stringvalue")
            .build();

    schemaRegistry.register(TOPIC + avroSchema.getFullName() + "-value", avroSchema);
    schemaRegistry.register(TOPIC + simpleAvroSchema.getFullName() + "-value", simpleAvroSchema);

    String expectedSimple = simpleStruct.toString();

    Map<String, String> smap = Stream.of(
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://mock:8081"),
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName())
    )
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    serializer.configure(smap, false);

    GenericData.Record complexStruct2 = new GenericRecordBuilder(avroSchema)
            .set("int8", 2)
            .set("int16", 2)
            .set("int32", 2)
            .set("int64", 2L)
            .set("float32", 2.2f)
            .set("boolean", true)
            .set("string", "id45634")
            .set("bytes", ByteBuffer.wrap("2".getBytes()))
            .set("array", Arrays.asList("x", "y", "z"))
            .set("map", Collections.singletonMap("field", 2))
            .set("subrecord1", new GenericRecordBuilder(subrecord1Schema)
                    .set("subrecord2", new GenericRecordBuilder(subrecord2Schema)
                            .set("int32", 199)
                            .build())
                    .set("array", Collections.singletonList("f"))
                    .build())
            .build();

    String expectedComplex = complexStruct2.toString();
    SchemaAndValue complexSchemaAndValue = converter.toConnectData(TOPIC, serializer.serialize(TOPIC, complexStruct));
    SchemaAndValue complexSchemaAndValue2 = converter.toConnectData(TOPIC, serializer.serialize(TOPIC, complexStruct2));

    SchemaAndValue simpleSchemaAndValue = converter.toConnectData(TOPIC, serializer.serialize(TOPIC, simpleStruct));
    SchemaAndValue tombstone = new SchemaAndValue(SchemaBuilder.STRING_SCHEMA, "199");

    final SinkRecord recordA = new SinkRecord(TOPIC, 0, tombstone.schema(), tombstone.value(), complexSchemaAndValue.schema(), complexSchemaAndValue.value(), 0);
    final SinkRecord recordB = new SinkRecord(TOPIC, 0, tombstone.schema(), tombstone.value(), simpleSchemaAndValue.schema(), simpleSchemaAndValue.value(), 1);
    final SinkRecord recordC = new SinkRecord(TOPIC, 0, tombstone.schema(), tombstone.value(), null, null, 2);
    final SinkRecord recordD = new SinkRecord(TOPIC, 0, tombstone.schema(), tombstone.value(), complexSchemaAndValue.schema(), complexSchemaAndValue2.value(), 3);


    writer = newWriter(props);
    writer.write(Arrays.asList(recordA, recordD, recordA, recordD, recordB, recordB, recordB));

    assertEquals(
            1,
            postgresqlHelper.select(
                    "SELECT * FROM " + TOPIC + " WHERE intkey ='" +
                            ((GenericData.Record) ((GenericData.Record) complexStruct2.get("subrecord1")).get("subrecord2")).get("int32") + "'" +
                            " AND stringkey ='" +
                            complexStruct2.get("string") + "'"
                    ,
                    new PostgresqlHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals("199", rs.getString("intkey"));
                        assertEquals("id45634", rs.getString("stringkey"));
                        assertEquals("f", rs.getString("arrayitem"));
                        assertEquals("2", rs.getString("mapvalue"));
                        JSONObject expectedJsonObject = new JSONObject(expectedComplex);
                        JSONObject resultJsonObject = new JSONObject(rs.getString("event"));
                        expectedJsonObject.keys().forEachRemaining(k -> assertEquals(resultJsonObject.get(k).toString(), expectedJsonObject.get(k).toString()));
                      }
                    }
            )
    );
    assertEquals(
            1,
            postgresqlHelper.select(
                    "SELECT * FROM " + TOPIC + " WHERE intkey ='" +
                            simpleStruct.get("int") + "'" +
                            " AND stringkey ='" +
                            simpleStruct.get("stringy") + "'"
                    ,
                    new PostgresqlHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals("199", rs.getString("intkey"));
                        assertEquals("stringvalue", rs.getString("stringkey"));
                        JSONObject expectedJsonObject = new JSONObject(expectedSimple);
                        JSONObject resultJsonObject = new JSONObject(rs.getString("event"));
                        expectedJsonObject.keys().forEachRemaining(k -> assertEquals(resultJsonObject.get(k).toString(), expectedJsonObject.get(k).toString()));
                      }
                    }
            )
    );
    assertEquals(
            1,
            postgresqlHelper.select(
                    "SELECT count(*) FROM " + TOPIC ,
                    new PostgresqlHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals(2, rs.getInt(1));            }
                    }
            )
    );

    writer.write(Collections.singletonList(recordC));

    assertEquals(
            1,
            postgresqlHelper.select(
                    "SELECT count(*) FROM " + TOPIC ,
                    new PostgresqlHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals(0, rs.getInt(1));            }
                    }
            )
    );

    TableId tableId = new TableId(null, null, TOPIC);
    TableDefinition refreshedMetadata = dialect.describeTable(postgresqlHelper.connection, tableId);
    System.out.println(refreshedMetadata);
  }

  @Test
  public void autoCreateWithAutoEvolveAvroWrongSchema() throws SQLException, RestClientException, IOException {
    Map<String, String> props = new HashMap<>();
    props.put("connection.url", postgresqlHelper.postgreSQL());
    props.put("auto.create", "true");
    props.put("auto.evolve", "true");
    props.put("value.converter.payload.field.name", "event");
    props.put("connection.user", "postgres");
    props.put("connection.password", "password123");
    props.put("zonemapattributes", "intkey");
    props.put("distributionattributes", "stringkey");
    props.put("clusteredattributes", "stringkey, intkey");
    props.put("partitions", "5");
    Map<String, String> map = Stream.of(
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://mock:8081"),

            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.SCHEMA_NAMES, "ComplexSchemaNameWRONG"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.int32", "intkey"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.string", "stringkey"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.PAYLOAD_FIELD_NAME, "event"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.INPUT_FORMAT, "avro")
    )
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    converter.configure(map, false);
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
            .endRecord()
            ;

    GenericData.Record struct = new GenericRecordBuilder(avroSchema)
            .set("int8", 12)
            .set("int16", 12)
            .set("int32", 12)
            .set("int64", 12L)
            .set("float32", 12.2f)
            .set("boolean", true)
            .set("string", "id45634")
            .set("bytes", ByteBuffer.wrap("foo".getBytes()))
            .set("array", Arrays.asList("a", "b", "c"))
            .set("map", Collections.singletonMap("field", 1)).build();

    schemaRegistry.register(TOPIC+ "-value", avroSchema);
    String expected = struct.toString();



    writer = newWriter(props);


    DataException e = assertThrows(
            DataException.class,
            () -> converter.toConnectData(TOPIC, serializer.serialize(TOPIC, struct)));
    assertTrue(
            e.getMessage(),
            e.getMessage().startsWith("The schema name: ComplexSchemaName, is not configured in [ComplexSchemaNameWRONG]")
    );

    TableId tableId = new TableId(null, null, TOPIC);
    TableDefinition refreshedMetadata = dialect.describeTable(postgresqlHelper.connection, tableId);
    System.out.println(refreshedMetadata);
  }

  @Test
  public void autoCreateWithAutoEvolveAvroWrongInstruction() throws SQLException, RestClientException, IOException {
    Map<String, String> props = new HashMap<>();
    props.put("connection.url", postgresqlHelper.postgreSQL());
    props.put("auto.create", "true");
    props.put("auto.evolve", "true");
    props.put("value.converter.payload.field.name", "event");
    props.put("connection.user", "postgres");
    props.put("connection.password", "password123");
    props.put("zonemapattributes", "intkey");
    props.put("distributionattributes", "stringkey");
    props.put("clusteredattributes", "stringkey, intkey");
    props.put("partitions", "5");
    Map<String, String> map = Stream.of(
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://mock:8081"),

            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.SCHEMA_NAMES, "ComplexSchemaName"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.intWrong", "intkey"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.stringWrong", "stringkey"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.PAYLOAD_FIELD_NAME, "event"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.INPUT_FORMAT, "avro")
    )
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    converter.configure(map, false);
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
            .endRecord()
            ;

    GenericData.Record struct = new GenericRecordBuilder(avroSchema)
            .set("int8", 12)
            .set("int16", 12)
            .set("int32", 12)
            .set("int64", 12L)
            .set("float32", 12.2f)
            .set("boolean", true)
            .set("string", "id45634")
            .set("bytes", ByteBuffer.wrap("foo".getBytes()))
            .set("array", Arrays.asList("a", "b", "c"))
            .set("map", Collections.singletonMap("field", 1)).build();

    schemaRegistry.register(TOPIC+ "-value", avroSchema);
    String expected = struct.toString();

    writer = newWriter(props);

    UncheckedExecutionException e = assertThrows(
            UncheckedExecutionException.class,
            () -> converter.toConnectData(TOPIC, serializer.serialize(TOPIC, struct)));
    assertTrue(
            e.getCause() instanceof DataException
    );
    assertTrue(
            e.getCause().getMessage().startsWith("The following key is not found in the Avro schema: intWrong")
    );
  }

  @Test
  public void autoCreateWithAutoEvolveAvroFirstValueFromArrayAndMapMultiStructureDisAllowNonIndexBatch() throws SQLException, RestClientException, IOException {
    Map<String, String> props = new HashMap<>();
    props.put("connection.url", postgresqlHelper.postgreSQL());
    props.put("auto.create", "true");
    props.put("auto.evolve", "true");
    props.put("value.converter.payload.field.name", "event");
    props.put("connection.user", "postgres");
    props.put("connection.password", "password123");
    props.put("zonemapattributes", "intkey");
    props.put("distributionattributes", "stringkey");
    props.put("clusteredattributes", "stringkey, intkey");
    props.put("partitions", "5");

    Map<String, String> map = Stream.of(
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://mock:8081"),

            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.SCHEMA_NAMES, "ComplexSchemaName, SimpleSchemaName"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.subrecord1.subrecord2.int32", "intkey"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.string", "stringkey"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.subrecord1.array", "arrayitem"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.map", "mapvalue"),
            new AbstractMap.SimpleImmutableEntry<>("SimpleSchemaName.int", "intkey"),
            new AbstractMap.SimpleImmutableEntry<>("SimpleSchemaName.stringy", "stringkey"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.PAYLOAD_FIELD_NAME, "event"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.INPUT_FORMAT, "avro"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.ALLOWNONINDEXED, "false"),
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName())
    )
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    ConfigException e = assertThrows(
            ConfigException.class,
            () -> converter.configure(map, false));
    assertTrue(
            e.getMessage().startsWith("Dereferencing arrayitem must be defined for each schema that is configured in schema.names")
    );
  }

  @Test
  public void autoCreateWithAutoEvolveAvroFirstValueFromArrayAndMapMultiStructureCacheSize() throws SQLException, RestClientException, IOException {
    Map<String, String> props = new HashMap<>();
    props.put("connection.url", postgresqlHelper.postgreSQL());
    props.put("auto.create", "true");
    props.put("auto.evolve", "true");
    props.put("delete.enabled", "true");
    props.put("insert.mode", "update");
    props.put("delete.keys", "intkey");
    props.put("upsert.keys", "intkey, stringkey");
    props.put("value.converter.payload.field.name", "event");
    props.put("connection.user", "postgres");
    props.put("connection.password", "password123");
    props.put("zonemapattributes", "intkey");
    props.put("distributionattributes", "stringkey");
    props.put("clusteredattributes", "stringkey, intkey");
    props.put("partitions", "5");


    Map<String, String> map = Stream.of(
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://mock:8081"),

            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.SCHEMA_NAMES, "ComplexSchemaName, SimpleSchemaName"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.subrecord1.subrecord2.int32", "intkey"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.string", "stringkey"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.subrecord1.array", "arrayitem"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.map", "mapvalue"),
            new AbstractMap.SimpleImmutableEntry<>("SimpleSchemaName.int", "intkey"),
            new AbstractMap.SimpleImmutableEntry<>("SimpleSchemaName.stringy", "stringkey"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.PAYLOAD_FIELD_NAME, "event"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.INPUT_FORMAT, "avro"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.ALLOWNONINDEXED, "true"),
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName())
    )
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    converter.configure(map, false);
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
    postgresqlHelper.createTable(
            "CREATE TABLE " + TOPIC + "(" +
                    "    intkey  TEXT," +
                    "    mapvalue  TEXT," +
                    "    arrayitem  TEXT," +
                    "    stringkey  TEXT," +
                    "    event  JSONB);"
    );
    postgresqlHelper.execute("insert into " + TOPIC +" values ('199', null, null, 'stringvalue', '{\"int\": 199, \"stringy\": \"stringvalue\"}' );");
    postgresqlHelper.execute("insert into " + TOPIC +" values ('199', '2', 'f', 'id45634', '{\"map\": {\"field\": 2}, \"int8\": 2, \"array\": [\"x\", \"y\", \"z\"], \"bytes\": \"2\", \"int16\": 2, \"int32\": 2, \"int64\": 2, \"string\": \"id45634\", \"boolean\": true, \"float32\": 2.2, \"subrecord1\": {\"array\": [\"f\"], \"subrecord2\": {\"int32\": 199}}}' );");

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
            .fields().requiredInt("int")
            .requiredString("stringy")
            .endRecord();

    GenericData.Record simpleStruct = new GenericRecordBuilder(simpleAvroSchema)
            .set("int", 199)
            .set("stringy", "stringvalue")
            .build();

    schemaRegistry.register(TOPIC + avroSchema.getFullName() + "-value", avroSchema);
    schemaRegistry.register(TOPIC + simpleAvroSchema.getFullName() + "-value", simpleAvroSchema);

    String expectedSimple = simpleStruct.toString();

    Map<String, String> smap = Stream.of(
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://mock:8081"),
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName())
    )
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    serializer.configure(smap, false);

    GenericData.Record complexStruct2 = new GenericRecordBuilder(avroSchema)
            .set("int8", 2)
            .set("int16", 2)
            .set("int32", 2)
            .set("int64", 2L)
            .set("float32", 2.2f)
            .set("boolean", true)
            .set("string", "id45634")
            .set("bytes", ByteBuffer.wrap("2".getBytes()))
            .set("array", Arrays.asList("x", "y", "z"))
            .set("map", Collections.singletonMap("field", 2))
            .set("subrecord1", new GenericRecordBuilder(subrecord1Schema)
                    .set("subrecord2", new GenericRecordBuilder(subrecord2Schema)
                            .set("int32", 199)
                            .build())
                    .set("array", Collections.singletonList("f"))
                    .build())
            .build();

    String expectedComplex = complexStruct2.toString();
    SchemaAndValue complexSchemaAndValue = converter.toConnectData(TOPIC, serializer.serialize(TOPIC, complexStruct));
    SchemaAndValue complexSchemaAndValue2 = converter.toConnectData(TOPIC, serializer.serialize(TOPIC, complexStruct2));

    SchemaAndValue simpleSchemaAndValue = converter.toConnectData(TOPIC, serializer.serialize(TOPIC, simpleStruct));
    SchemaAndValue tombstone = new SchemaAndValue(SchemaBuilder.STRING_SCHEMA, "199");

    final SinkRecord recordA = new SinkRecord(TOPIC, 0, tombstone.schema(), tombstone.value(), complexSchemaAndValue.schema(), complexSchemaAndValue.value(), 0);
    final SinkRecord recordB = new SinkRecord(TOPIC, 0, tombstone.schema(), tombstone.value(), simpleSchemaAndValue.schema(), simpleSchemaAndValue.value(), 1);
    final SinkRecord recordC = new SinkRecord(TOPIC, 0, tombstone.schema(), tombstone.value(), null, null, 2);
    final SinkRecord recordD = new SinkRecord(TOPIC, 0, tombstone.schema(), tombstone.value(), complexSchemaAndValue.schema(), complexSchemaAndValue2.value(), 3);


    writer = newWriter(props);
    writer.write(Arrays.asList(recordA, recordD, recordA, recordD, recordB, recordC, recordB, recordB));
    assertEquals(2,converter.getCacheSize());
    writer.write(Arrays.asList(recordA, recordD, recordC, recordA, recordD, recordB, recordB, recordB));
    assertEquals(2,converter.getCacheSize());
    writer.write(Arrays.asList(recordA, recordD, recordA, recordD, recordB, recordB, recordB));
    assertEquals(2,converter.getCacheSize());
  }

  @Test
  public void autoCreateWithAutoEvolveAvroUTF8() throws SQLException, RestClientException, IOException {
    Map<String, String> props = new HashMap<>();
    props.put("connection.url", postgresqlHelper.postgreSQL());
    props.put("auto.create", "true");
    props.put("auto.evolve", "true");
    props.put("value.converter.payload.field.name", "event");
    props.put("connection.user", "postgres");
    props.put("connection.password", "password123");
    props.put("zonemapattributes", "intkey");
    props.put("distributionattributes", "stringkey");
    props.put("clusteredattributes", "stringkey, intkey");
    props.put("partitions", "5");
    Map<String, String> map = Stream.of(
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://mock:8081"),

            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.SCHEMA_NAMES, "ComplexSchemaName"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.int32", "intkey"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.string", "stringkey"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.PAYLOAD_FIELD_NAME, "event"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.INPUT_FORMAT, "avro")
    )
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    converter.configure(map, false);
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
            .endRecord()
            ;

    GenericData.Record struct = new GenericRecordBuilder(avroSchema)
            .set("int8", 12)
            .set("int16", 12)
            .set("int32", 12)
            .set("int64", 12L)
            .set("float32", 12.2f)
            .set("boolean", true)
            .set("string", "id45634#¤§`´åæøØÆÅ")
            .set("bytes", ByteBuffer.wrap("foo".getBytes()))
            .set("array", Arrays.asList("a", "b", "c"))
            .set("map", Collections.singletonMap("field", 1)).build();

    schemaRegistry.register(TOPIC+ "-value", avroSchema);
    String expected = struct.toString();
    SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, serializer.serialize(TOPIC, struct));

    final SinkRecord recordA = new SinkRecord(TOPIC, 0, null, null, schemaAndValue.schema(), schemaAndValue.value(), 0);
    System.out.println(recordA);
    writer = newWriter(props);
    writer.write(Collections.singletonList(recordA));

    assertEquals(
            1,
            postgresqlHelper.select(
                    "SELECT * FROM " + TOPIC,
                    new PostgresqlHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals("12", rs.getString("intkey"));
                        assertEquals("id45634#¤§`´åæøØÆÅ", rs.getString("stringkey"));
                        JSONObject expectedJsonObject = new JSONObject(expected);
                        JSONObject resultJsonObject = new JSONObject(rs.getString("event"));
                        expectedJsonObject.keys().forEachRemaining(k -> assertEquals(resultJsonObject.get(k).toString(), expectedJsonObject.get(k).toString()));
                      }
                    }
            )
    );
    TableId tableId = new TableId(null, null, TOPIC);
    TableDefinition refreshedMetadata = dialect.describeTable(postgresqlHelper.connection, tableId);
    System.out.println(refreshedMetadata);
  }

}
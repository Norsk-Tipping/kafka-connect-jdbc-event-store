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

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.PostgreSqlDatabaseDialect;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import no.norsktipping.kafka.connect.converter.JsonConverter;
import no.norsktipping.kafka.connect.converter.JsonConverterConfig;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.confluent.connect.jdbc.sink.PostgresqlHelper.tablesUsed;

public class JdbcDbReadmeTest {

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
  public void ReadmeTest() throws SQLException, RestClientException, IOException {
    Map<String, String> props = new HashMap<>();
    props.put("connection.url", postgresqlHelper.postgreSQL());
    props.put("auto.create", "true");
    props.put("auto.evolve", "true");
    props.put("connection.user", "postgres");
    props.put("connection.password", "password123");
    props.put("coordinates.enabled", "true");
    /*props.put("value.converter.payload.field.name", "event");
    props.put("zonemapattributes", "intkey");
    props.put("distributionattributes", "stringkey");
    props.put("clusteredattributes", "stringkey, intkey");
    props.put("partitions", "5");*/
    Map<String, String> map = Stream.of(
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://mock:8081"),

            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.SCHEMA_NAMES, "ComplexSchemaName, SimpleSchemaName"),
    /*        new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.subrecord1.subrecord2.int32", "intkey"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.string", "stringkey"),*/
      /*      new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.subrecord1.array", "arrayitem"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.map", "mapvalue"),*/
           /* new AbstractMap.SimpleImmutableEntry<>("SimpleSchemaName.int", "intkey"),
            new AbstractMap.SimpleImmutableEntry<>("SimpleSchemaName.stringy", "stringkey"),*/
            //new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.PAYLOAD_FIELD_NAME, "event"),
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

    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.record("com.example.ComplexSchemaName").fields()
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

    Map<String, String> smap = Stream.of(
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://mock:8081"),
            new AbstractMap.SimpleImmutableEntry<>(AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName())
    )
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    serializer.configure(smap, false);

    SchemaAndValue complexSchemaAndValue = converter.toConnectData(TOPIC, serializer.serialize(TOPIC, complexStruct));
    SchemaAndValue simpleSchemaAndValue = converter.toConnectData(TOPIC, serializer.serialize(TOPIC, simpleStruct));

    final SinkRecord recordA = new SinkRecord(TOPIC, 1, null, null, complexSchemaAndValue.schema(), complexSchemaAndValue.value(), 1332);
    final SinkRecord recordB = new SinkRecord(TOPIC, 2, null, null, simpleSchemaAndValue.schema(), simpleSchemaAndValue.value(), 4443);

    writer = newWriter(props);
    writer.write(Collections.singletonList(recordA));
    writer.write(Collections.singletonList(recordB));

  }

  @Test
  public void ReadmeTestJson() throws SQLException, RestClientException, IOException {
    Map<String, String> props = new HashMap<>();
    props.put("connection.url", postgresqlHelper.postgreSQL());
    props.put("auto.create", "true");
    props.put("auto.evolve", "true");
    props.put("connection.user", "postgres");
    props.put("connection.password", "password123");
    /*props.put("value.converter.payload.field.name", "event");
    props.put("zonemapattributes", "intkey");
    props.put("distributionattributes", "stringkey");
    props.put("clusteredattributes", "stringkey, intkey");
    props.put("partitions", "5");*/
    Map<String, String> map = Stream.of(
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.SCHEMA_NAMES, "ComplexSchemaName, SimpleSchemaName"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.subrecord1.subrecord2.int32", "intkey"),
            new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.string", "stringkey"),
            //new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.subrecord1.array", "arrayitem"),
            //new AbstractMap.SimpleImmutableEntry<>("ComplexSchemaName.map", "mapvalue"),
            new AbstractMap.SimpleImmutableEntry<>("SimpleSchemaName.int", "intkey"),
            new AbstractMap.SimpleImmutableEntry<>("SimpleSchemaName.stringy", "stringkey"),
            //new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.PAYLOAD_FIELD_NAME, "event"),
            new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.INPUT_FORMAT, "json"),
            new AbstractMap.SimpleImmutableEntry<>("json.ComplexSchemaName.subrecord1.subrecord2.int32", "199"),
            new AbstractMap.SimpleImmutableEntry<>("json.SimpleSchemaName.stringy", "true"),
            //new AbstractMap.SimpleImmutableEntry<>(JsonConverterConfig.ALLOWNONINDEXED, "true"),
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

    org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.record("com.example.ComplexSchemaName").fields()
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


    SchemaAndValue complexSchemaAndValue = converter.toConnectData(TOPIC, complexStruct.toString().getBytes());
    SchemaAndValue simpleSchemaAndValue = converter.toConnectData(TOPIC, simpleStruct.toString().getBytes());

    final SinkRecord recordA = new SinkRecord(TOPIC, 0, null, null, complexSchemaAndValue.schema(), complexSchemaAndValue.value(), 0);
    final SinkRecord recordB = new SinkRecord(TOPIC, 0, null, null, simpleSchemaAndValue.schema(), simpleSchemaAndValue.value(), 0);

    writer = newWriter(props);
    writer.write(Collections.singletonList(recordA));
    writer.write(Collections.singletonList(recordB));

  }

}
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

package io.confluent.connect.jdbc.sink.metadata;

import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.*;

public class FieldsMetadata {

  public final Set<String> upsertKeyFieldNames;
  public final Set<String> deleteKeyFieldNames;
  public final Set<String> nonKeyFieldNames;
  public final Map<String, SinkRecordField> allFields;

  // visible for testing
  public FieldsMetadata(
      Set<String> upsertKeyFieldNames,
      Set<String> deleteKeyFieldNames,
      Set<String> nonKeyFieldNames,
      Map<String, SinkRecordField> allFields,
      boolean deleteRecord
  ) {
    HashSet<String> result = new HashSet<>(upsertKeyFieldNames);
    result.addAll(deleteKeyFieldNames);
    boolean fieldCountsMatch = (nonKeyFieldNames.size()  == allFields.size());
    boolean allFieldsContained = (allFields.keySet().containsAll(result)
                   && allFields.keySet().containsAll(nonKeyFieldNames));
    if (!deleteRecord && (!fieldCountsMatch || !allFieldsContained)) {
      throw new IllegalArgumentException(String.format(
          "Validation fail -- keyFieldNames:%s nonKeyFieldNames:%s allFields:%s",
          result, nonKeyFieldNames, allFields
      ));
    }
    this.deleteKeyFieldNames = deleteKeyFieldNames;
    this.upsertKeyFieldNames = upsertKeyFieldNames;
    this.nonKeyFieldNames = nonKeyFieldNames;
    this.allFields = allFields;
  }

  public static FieldsMetadata extract(
      final String tableName,
      final JdbcSinkConfig.InsertMode insertMode,
      final Set<String> upsertKeys,
      final Set<String> deleteKeys,
      final Boolean coordinates,
      final Boolean deleteEnabled,
      final SchemaPair schemaPair
  ) {
    return extract(
        tableName,
        insertMode,
        upsertKeys,
        deleteKeys,
        coordinates,
        deleteEnabled,
        schemaPair.keySchema,
        schemaPair.valueSchema
    );
  }

  public static FieldsMetadata extract(
      final String tableName,
      final JdbcSinkConfig.InsertMode insertMode,
      final Set<String> upsertKeys,
      final Set<String> deleteKeys,
      final Boolean coordinates,
      final Boolean deleteEnabled,
      final Schema keySchema,
      final Schema valueSchema
  ) {
    if (valueSchema != null && valueSchema.type() != Schema.Type.STRUCT) {
      throw new ConnectException("Value schema must be of type Struct");
    }

    final Map<String, SinkRecordField> allFields = new HashMap<>();

    final Set<String> upsertKeyFieldNames = new LinkedHashSet<>();
    final Set<String> deleteKeyFieldNames = new LinkedHashSet<>();
    final Set<String> nonKeyFieldNames = new LinkedHashSet<>();

      if (coordinates && valueSchema != null) {
        extractKafkaPk(tableName, nonKeyFieldNames, allFields);
      }
      if (valueSchema != null && ( insertMode == JdbcSinkConfig.InsertMode.UPSERT || insertMode == JdbcSinkConfig.InsertMode.UPDATE) ) {
        extractRecordValuePk(tableName, upsertKeys, valueSchema, allFields, upsertKeyFieldNames);
      }

    if (valueSchema != null) {
      for (Field field : valueSchema.fields()) {
        /*if (deleteEnabled) {
          if (deleteKeyFieldNames.contains(field.name())) {
            continue;
          }
        }
        if (insertMode == JdbcSinkConfig.InsertMode.UPSERT || insertMode == JdbcSinkConfig.InsertMode.UPDATE) {
          if (upsertKeyFieldNames.contains(field.name())) {
            continue;
          }
        }*/
        nonKeyFieldNames.add(field.name());

        final Schema fieldSchema = field.schema();
        allFields.put(field.name(), new SinkRecordField(fieldSchema, field.name(), false));
      }
    }

    if (deleteEnabled) {
      extractRecordKeyPk(tableName, deleteKeys, keySchema, allFields, deleteKeyFieldNames);
    }

    if (allFields.isEmpty()) {
      throw new ConnectException(
              "No fields found using key and value schemas for table: " + tableName
      );
    }

    final Map<String, SinkRecordField> allFieldsOrdered = new LinkedHashMap<>();
    for (String fieldName : JdbcSinkConfig.DEFAULT_KAFKA_PK_NAMES) {
      if (allFields.containsKey(fieldName)) {
        allFieldsOrdered.put(fieldName, allFields.get(fieldName));
      }
    }

    if (valueSchema != null) {
      for (Field field : valueSchema.fields()) {
        String fieldName = field.name();
        if (allFields.containsKey(fieldName)) {
          allFieldsOrdered.put(fieldName, allFields.get(fieldName));
        }
      }
    }

    if (allFieldsOrdered.size() < allFields.size()) {
      ArrayList<String> fieldKeys = new ArrayList<>(allFields.keySet());
      Collections.sort(fieldKeys);
      for (String fieldName : fieldKeys) {
        if (!allFieldsOrdered.containsKey(fieldName)) {
          allFieldsOrdered.put(fieldName, allFields.get(fieldName));
        }
      }
    }

    return new FieldsMetadata(upsertKeyFieldNames, deleteKeyFieldNames, nonKeyFieldNames, allFieldsOrdered, valueSchema == null);
  }

  private static void extractKafkaPk(
      final String tableName,
      final Set<String> nonKeyFieldNames,
      final Map<String, SinkRecordField> allFields
  ) {
    /*if (configuredPkFields.isEmpty()) {
      keyFieldNames.addAll(JdbcSinkConfig.DEFAULT_KAFKA_PK_NAMES);
    } else if (configuredPkFields.size() == 3) {
      keyFieldNames.addAll(configuredPkFields);
    } else {
      throw new ConnectException(String.format(
          "PK mode for table '%s' is %s so there should either be no field names defined for "
          + "defaults %s to be applicable, or exactly 3, defined fields are: %s",
          tableName,
          JdbcSinkConfig.PrimaryKeyMode.KAFKA,
          JdbcSinkConfig.DEFAULT_KAFKA_PK_NAMES,
          configuredPkFields
      ));
    }*/

    final Iterator<String> it = JdbcSinkConfig.DEFAULT_KAFKA_PK_NAMES.iterator();
    final String topicFieldName = it.next();
    allFields.put(
        topicFieldName,
        new SinkRecordField(Schema.STRING_SCHEMA, topicFieldName, false)
    );
    nonKeyFieldNames.add(topicFieldName);

    final String partitionFieldName = it.next();
    allFields.put(
        partitionFieldName,
        new SinkRecordField(Schema.INT32_SCHEMA, partitionFieldName, false)
    );
    nonKeyFieldNames.add(partitionFieldName);

    final String offsetFieldName = it.next();
    allFields.put(
        offsetFieldName,
        new SinkRecordField(Schema.INT64_SCHEMA, offsetFieldName, false)
    );
    nonKeyFieldNames.add(offsetFieldName);

    final String timestampFieldName = it.next();
    allFields.put(
            timestampFieldName,
            new SinkRecordField(Timestamp.builder().optional().build(), timestampFieldName, false)
    );
    nonKeyFieldNames.add(timestampFieldName);

    final String timestampTypeFieldName = it.next();
    allFields.put(
            timestampTypeFieldName,
            new SinkRecordField(Schema.STRING_SCHEMA, timestampTypeFieldName, false)
    );
    nonKeyFieldNames.add(timestampTypeFieldName);

  }

  private static void extractRecordKeyPk(
      final String tableName,
      final Set<String> deleteKeys,
      final Schema keySchema,
      final Map<String, SinkRecordField> allFields,
      final Set<String> deleteKeyFieldNames
  ) {
    {
      if (keySchema == null) {
        throw new ConnectException(String.format(
            "Delete is enabled for table '%s' in %s, but record key schema is missing",
            tableName,
            JdbcSinkConfig.DELETE_ENABLED
        ));
      }
      final Schema.Type keySchemaType = keySchema.type();
      if (keySchemaType.isPrimitive()) {
        if (deleteKeys.size() != 1) {
          throw new ConnectException(String.format(
              "Need exactly one PK column defined since the key schema for records is a "
              + "primitive type, defined columns are: %s",
              deleteKeys
          ));
        }
        final String fieldName = deleteKeys.stream().findFirst().orElseThrow(() ->
                new ConnectException(String.format(
                        "Need exactly one PK column defined since the key schema for records is a "
                                + "primitive type, defined columns are: %s",
                        deleteKeys
                )));

        deleteKeyFieldNames.add(fieldName);
        if (!allFields.isEmpty()) {
          SinkRecordField field = Optional.ofNullable(allFields)
                  .map(fields -> fields.get(fieldName))
                  .orElseThrow(() ->
                          new ConnectException(String.format(
                                  "Delete is enabled but the configured key %s is not found in the record value schema",
                                  fieldName
                          ))
                  );
          if (!Objects.equals(keySchema.type(), field.schema().type())) {
            throw new ConnectException(String.format(
                    "Delete is enabled but the configured Kafka record key %s is of type %S while previous record value " +
                            "fields for that field are stored as type %s. \n" +
                            "Use SMT to assure Kafka record key that is used to delete is of same type as respective field in the Kafka record value field that gets inserted",
                    fieldName,
                    keySchema.type().toString(),
                    field.schema().type().toString()
            ));
          }
          allFields.put(fieldName, new SinkRecordField(field.schema(), field.name(), true));
        } else {
          allFields.put(fieldName, new SinkRecordField(keySchema, fieldName, true));
        }
      } else if (keySchemaType == Schema.Type.STRUCT) {
        if (deleteKeys.isEmpty()) {
            throw new ConnectException(String.format(
                    "Delete is enabled but no delete keys are found in configuration %s",
                    JdbcSinkConfig.DELETE_KEYS
            ));
        } else {
          for (String fieldName : deleteKeys) {
            final Field keyField = keySchema.field(fieldName);
            if (keyField == null) {
              throw new ConnectException(String.format(
                  "%s is 'true' for table '%s' and %s is configured with field(s) %s, but record key "
                  + "schema does not contain field: %s",
                      JdbcSinkConfig.DELETE_ENABLED, tableName, JdbcSinkConfig.DELETE_KEYS, deleteKeys, fieldName
              ));
            }
            deleteKeyFieldNames.add(keyField.name());
            if (!allFields.isEmpty()) {
              SinkRecordField field = Optional.of(allFields)
                      .map(fields -> fields.get(keyField.name()))
                      .orElseThrow(() ->
                              new ConnectException(String.format(
                                      "Delete is enabled but the configured key %s is not found in the record value schema",
                                      keyField.name()
                              ))
                      );
              Schema.Type keyFieldType = Optional.of(keySchema)
                      .map(s -> s.field(keyField.name()))
                      .map(f -> f.schema().type()).orElseThrow(
                              () -> new ConnectException(String.format(
                      "Could not determine schema type for key field %s",
                      fieldName))
                      );
              if (!Objects.equals(keyFieldType, field.schema().type())) {
                throw new ConnectException(String.format(
                        "Delete is enabled but the configured Kafka record key %s is of type %S while previous record value " +
                                "fields for that field are stored as type %s. \n" +
                                "Use SMT to assure Kafka record key that is used to delete is of same type as respective field in the Kafka record value field that gets inserted",
                        fieldName,
                        keySchema.field(keyField.name()).schema().type(),
                        field.schema().type().toString()
                ));
              }
              allFields.put(keyField.name(), new SinkRecordField(field.schema(), fieldName, true));
            } else {
              allFields.put(fieldName, new SinkRecordField(keySchema, fieldName, true));
            }
          }
        }
      } else {
        throw new ConnectException(
            "Key schema must be primitive type or Struct, but is of type: " + keySchemaType
        );
      }
    }
  }

  private static void extractRecordValuePk(
      final String tableName,
      final Set<String> upsertKeys,
      final Schema valueSchema,
      final Map<String, SinkRecordField> allFields,
      final Set<String> upsertKeyFieldNames
  ) {
    if (valueSchema == null) {
      throw new ConnectException(String.format(
              "Upsert or Update is enabled for table '%s' in %s, but record value schema is missing",
              tableName,
              JdbcSinkConfig.INSERT_MODE)
      );
    }
    if (upsertKeys.isEmpty()) {
      throw new ConnectException(
              String.format("Upsert or Update is enabled for table '%s' in %s, but %s is empty",
                      tableName,
                      JdbcSinkConfig.INSERT_MODE,
                      JdbcSinkConfig.UPSERT_KEYS));
    } else {
      for (String fieldName : upsertKeys) {
        final Field keyField = valueSchema.field(fieldName);
        if (keyField == null) {
          throw new ConnectException(String.format(
                  "Upsert or Update is configured for table '%s' and %s is configured with field(s) %s, but record value "
                          + "schema does not contain field: %s",
                  tableName, JdbcSinkConfig.UPSERT_KEYS, upsertKeys, fieldName
          ));
        }
        upsertKeyFieldNames.add(keyField.name());

        allFields.put(keyField.name(), new SinkRecordField(keyField.schema(), fieldName, true));
      }
    }
  }

  @Override
  public String toString() {
    return "FieldsMetadata{"
           + "upsertKeyFieldNames=" + upsertKeyFieldNames
           + "deleteKeyFieldNames=" + deleteKeyFieldNames
           + ", nonKeyFieldNames=" + nonKeyFieldNames
           + ", allFields=" + allFields
           + '}';
  }
}

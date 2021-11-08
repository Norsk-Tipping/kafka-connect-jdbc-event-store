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

import io.confluent.connect.jdbc.util.*;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;

public class JdbcSinkConfig extends AbstractConfig {

  public enum InsertMode {
    INSERT,
    UPSERT,
    UPDATE;

  }

  public enum DBEncoding {
    UTF8,
    UTF16;

  }

  /*public enum PrimaryKeyMode {
    NONE,
    KAFKA,
    RECORD_KEY,
    RECORD_VALUE;
  }*/

  public static final List<String> DEFAULT_KAFKA_PK_NAMES = Collections.unmodifiableList(
      Arrays.asList(
          ucase("connect_topic"),
              ucase("connect_partition"),
                      ucase("connect_offset"),
                              ucase("connect_timestamp"),
                                      ucase("connect_timestamp_type")
      )
  );

  public static final String COORDINATES_ENABLED = "coordinates.enabled";
  private static final String COORDINATES_ENABLED_DEFAULT = "false";
  private static final String COORDINATES_ENABLED_DOC =
          "Whether to store records with they kafka coordinates: topic, partition, offset.";
  private static final String COORDINATES_ENABLED_DISPLAY = "Enable kafka coordinates";

  public static final String CONNECTION_URL = JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG;
  private static final String CONNECTION_URL_DOC =
      "JDBC connection URL.\n"
          + "For example: ``jdbc:oracle:thin:@localhost:1521:orclpdb1``, "
          + "``jdbc:mysql://localhost/db_name``, "
          + "``jdbc:sqlserver://localhost;instance=SQLEXPRESS;"
          + "databaseName=db_name``";
  private static final String CONNECTION_URL_DISPLAY = "JDBC URL";

  public static final String CONNECTION_USER = JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG;
  private static final String CONNECTION_USER_DOC = "JDBC connection user.";
  private static final String CONNECTION_USER_DISPLAY = "JDBC User";

  public static final String CONNECTION_PASSWORD =
      JdbcSourceConnectorConfig.CONNECTION_PASSWORD_CONFIG;
  private static final String CONNECTION_PASSWORD_DOC = "JDBC connection password.";
  private static final String CONNECTION_PASSWORD_DISPLAY = "JDBC Password";

  public static final String CONNECTION_ATTEMPTS =
      JdbcSourceConnectorConfig.CONNECTION_ATTEMPTS_CONFIG;
  private static final String CONNECTION_ATTEMPTS_DOC =
      JdbcSourceConnectorConfig.CONNECTION_ATTEMPTS_DOC;
  private static final String CONNECTION_ATTEMPTS_DISPLAY =
      JdbcSourceConnectorConfig.CONNECTION_ATTEMPTS_DISPLAY;
  public static final int CONNECTION_ATTEMPTS_DEFAULT =
      JdbcSourceConnectorConfig.CONNECTION_ATTEMPTS_DEFAULT;

  public static final String CONNECTION_BACKOFF =
      JdbcSourceConnectorConfig.CONNECTION_BACKOFF_CONFIG;
  private static final String CONNECTION_BACKOFF_DOC =
      JdbcSourceConnectorConfig.CONNECTION_BACKOFF_DOC;
  private static final String CONNECTION_BACKOFF_DISPLAY =
      JdbcSourceConnectorConfig.CONNECTION_BACKOFF_DISPLAY;
  public static final long CONNECTION_BACKOFF_DEFAULT =
      JdbcSourceConnectorConfig.CONNECTION_BACKOFF_DEFAULT;

  public static final String TABLE_NAME_FORMAT = "table.name.format";
  private static final String TABLE_NAME_FORMAT_DEFAULT = "${topic}";
  private static final String TABLE_NAME_FORMAT_DOC =
      "A format string for the destination table name, which may contain '${topic}' as a "
      + "placeholder for the originating topic name.\n"
      + "For example, ``kafka_${topic}`` for the topic 'orders' will map to the table name "
      + "'kafka_orders'.";
  private static final String TABLE_NAME_FORMAT_DISPLAY = "Table Name Format";

  public static final String MAX_RETRIES = "max.retries";
  private static final int MAX_RETRIES_DEFAULT = 10;
  private static final String MAX_RETRIES_DOC =
      "The maximum number of times to retry on errors before failing the task.";
  private static final String MAX_RETRIES_DISPLAY = "Maximum Retries";

  public static final String RETRY_BACKOFF_MS = "retry.backoff.ms";
  private static final int RETRY_BACKOFF_MS_DEFAULT = 3000;
  private static final String RETRY_BACKOFF_MS_DOC =
      "The time in milliseconds to wait following an error before a retry attempt is made.";
  private static final String RETRY_BACKOFF_MS_DISPLAY = "Retry Backoff (millis)";

  public static final String BATCH_SIZE = "batch.size";
  private static final int BATCH_SIZE_DEFAULT = 3000;
  private static final String BATCH_SIZE_DOC =
      "Specifies how many records to attempt to batch together for insertion into the destination"
      + " table, when possible.";
  private static final String BATCH_SIZE_DISPLAY = "Batch Size";

  public static final String DELETE_ENABLED = "delete.enabled";
  private static final String DELETE_ENABLED_DEFAULT = "false";
  private static final String DELETE_ENABLED_DOC =
      "Whether to treat ``null`` record values as deletes. Requires ``pk.mode`` "
      + "to be ``record_key``.";
  private static final String DELETE_ENABLED_DISPLAY = "Enable deletes";

  public static final String AUTO_CREATE = "auto.create";
  private static final String AUTO_CREATE_DEFAULT = "false";
  private static final String AUTO_CREATE_DOC =
      "Whether to automatically create the destination table based on record schema if it is "
      + "found to be missing by issuing ``CREATE``.";
  private static final String AUTO_CREATE_DISPLAY = "Auto-Create";

  public static final String AUTO_EVOLVE = "auto.evolve";
  private static final String AUTO_EVOLVE_DEFAULT = "false";
  private static final String AUTO_EVOLVE_DOC =
      "Whether to automatically add columns in the table schema when found to be missing relative "
      + "to the record schema by issuing ``ALTER``.";
  private static final String AUTO_EVOLVE_DISPLAY = "Auto-Evolve";

  public static final String INSERT_MODE = "insert.mode";
  private static final String INSERT_MODE_DEFAULT = "insert";
  private static final String INSERT_MODE_DOC =
      "The insertion mode to use. Supported modes are:\n"
      + "``insert``\n"
      + "    Use standard SQL ``INSERT`` statements.\n"
      + "``upsert``\n"
      + "    Use the appropriate upsert semantics for the target database if it is supported by "
      + "the connector, e.g. ``INSERT OR IGNORE``.\n"
      + "``update``\n"
      + "    Use the appropriate update semantics for the target database if it is supported by "
      + "the connector, e.g. ``UPDATE``.";
  private static final String INSERT_MODE_DISPLAY = "Insert Mode";

  public static final String UPSERT_KEYS = "upsert.keys";
  private static final String UPSERT_KEYS_DEFAULT = "";
  private static final String UPSERT_KEYS_DOC =
      "List of comma-separated key field names that are used to upsert or update matching rows.\n"
      + "Upsert will be used to write events idempotent to the target to prevent duplicates. \n"
      + "Upsert is executed as a DELETE followed by INSERT filtered on these key(s). \n"
      + "Assure these keys guarantee an individual record with for example an event id." ;
  private static final String UPSERT_KEYS_DISPLAY = "Keys used for upsert";

  public static final String DELETE_KEYS = "delete.keys";
  private static final String DELETE_KEYS_DEFAULT = "";
  private static final String DELETE_KEYS_DOC =
          "Key field names from Kafka Record Key that is used to delete matching rows.\n"
                  + "Delete can be used to delete a series of events that match the value(s) of the Kafka Record Key \n"
                  + "received in a tombstone record.";
  private static final String DELETE_KEYS_DISPLAY = "Keys used for delete";

 /* public static final String PK_MODE = "pk.mode";
  private static final String PK_MODE_DEFAULT = "none";
  private static final String PK_MODE_DOC =
      "The primary key mode, also refer to ``" + PK_FIELDS + "`` documentation for interplay. "
      + "Supported modes are:\n"
      + "``none``\n"
      + "    No keys utilized.\n"
      + "``kafka``\n"
      + "    Kafka coordinates are used as the PK.\n"
      + "``record_key``\n"
      + "    Field(s) from the record key are used, which may be a primitive or a struct.\n"
      + "``record_value``\n"
      + "    Field(s) from the record value are used, which must be a struct.";
  private static final String PK_MODE_DISPLAY = "Primary Key Mode";*/

/*  public static final String FIELDS_WHITELIST = "fields.whitelist";
  private static final String FIELDS_WHITELIST_DEFAULT = "";
  private static final String FIELDS_WHITELIST_DOC =
      "List of comma-separated record value field names. If empty, all fields from the record "
      + "value are utilized, otherwise used to filter to the desired fields.\n"
      + "Note that ``" + PK_FIELDS + "`` is applied independently in the context of which field"
      + "(s) form the primary key columns in the destination database,"
      + " while this configuration is applicable for the other columns.";
  private static final String FIELDS_WHITELIST_DISPLAY = "Fields Whitelist";*/

  private static final ConfigDef.Range NON_NEGATIVE_INT_VALIDATOR = ConfigDef.Range.atLeast(0);

  private static final String CONNECTION_GROUP = "Connection";
  private static final String WRITES_GROUP = "Writes";
  private static final String DATAMAPPING_GROUP = "Data Mapping";
  private static final String DDL_GROUP = "DDL Support";
  private static final String RETRIES_GROUP = "Retries";

  public static final String DIALECT_NAME_CONFIG = "dialect.name";
  private static final String DIALECT_NAME_DISPLAY = "Database Dialect";
  public static final String DIALECT_NAME_DEFAULT = "";
  private static final String DIALECT_NAME_DOC =
      "The name of the database dialect that should be used for this connector. By default this "
      + "is empty, and the connector automatically determines the dialect based upon the "
      + "JDBC connection URL. Use this if you want to override that behavior and use a "
      + "specific dialect. All properly-packaged dialects in the JDBC connector plugin "
      + "can be used.";

  public static final String DB_ENCODING = "db.encoding";
  private static final String DB_ENCODING_DISPLAY = "Database Encoding";
  public static final String DB_ENCODING_DEFAULT = "UTF8";
  private static final String DB_ENCODING_DOC =
          "The name of the encoding the target db uses. By default UTF8 is used. " +
                  "Set this to UTF16 in case this encoding is used.";

  public static final String DB_TIMEZONE_CONFIG = "db.timezone";
  public static final String DB_TIMEZONE_DEFAULT = "UTC";
  private static final String DB_TIMEZONE_CONFIG_DOC =
      "Name of the JDBC timezone that should be used in the connector when "
      + "inserting time-based values. Defaults to UTC.";
  private static final String DB_TIMEZONE_CONFIG_DISPLAY = "DB Time Zone";

  public static final String QUOTE_SQL_IDENTIFIERS_CONFIG =
      JdbcSourceConnectorConfig.QUOTE_SQL_IDENTIFIERS_CONFIG;
  public static final String QUOTE_SQL_IDENTIFIERS_DEFAULT =
      JdbcSourceConnectorConfig.QUOTE_SQL_IDENTIFIERS_DEFAULT;
  public static final String QUOTE_SQL_IDENTIFIERS_DOC =
      JdbcSourceConnectorConfig.QUOTE_SQL_IDENTIFIERS_DOC;
  private static final String QUOTE_SQL_IDENTIFIERS_DISPLAY =
      JdbcSourceConnectorConfig.QUOTE_SQL_IDENTIFIERS_DISPLAY;

  public static final String TABLE_TYPES_CONFIG = "table.types";
  private static final String TABLE_TYPES_DISPLAY = "Table Types";
  public static final String TABLE_TYPES_DEFAULT = TableType.TABLE.toString();
  private static final String TABLE_TYPES_DOC =
      "The comma-separated types of database tables to which the sink connector can write. "
      + "By default this is ``" + TableType.TABLE + "``, but any combination of ``"
      + TableType.TABLE + "`` and ``" + TableType.VIEW + "`` is allowed. Not all databases "
      + "support writing to views, and when they do the the sink connector will fail if the "
      + "view definition does not match the records' schemas (regardless of ``"
      + AUTO_EVOLVE + "``).";

  public static final String PAYLOAD_FIELD_NAME = "value.converter.payload.field.name";
  private static final String PAYLOAD_FIELD_NAME_DEFAULT = "event";
  private static final String PAYLOAD_FIELD_NAME_DOC =
          "Specify the field name that will be used to contain the record value in JSON ";

  public static final String SCHEMA_NAMES = "value.converter.schema.names";
  private static final String SCHEMA_NAMES_DOC =
          "Specify the field name that will be used to contain the record value in JSON ";

  //Added flag to indicate uppercase or lowercase
  public static final String UPPERCASE = "uppercase";
  private static final String UPPERCASE_DEFAULT = "false";
  private static final String UPPERCASE_DOC =
          "Whether to automatically apply uppercase to columns is enabled."
                  + " When set to false lowercase will be applied.";
  private static final String UPPERCASE_DISPLAY = "Uppercase fields";

  public static final String ZONEMAPATTRIBUTES = "zonemapattributes";
  private static final String ZONEMAPATTRIBUTES_DEFAULT = "";
  private static final String ZONEMAPATTRIBUTES_DOC =
          "List of comma-separated column names that need to be included in a zonemap for create table";
  private static final String ZONEMAPATTRIBUTES_DISPLAY = "Zonemap column names";

  public static final String CLUSTEREDATTRIBUTES = "clusteredattributes";
  private static final String CLUSTEREDATTRIBUTES_DEFAULT = "";
  private static final String CLUSTEREDATTRIBUTES_DOC =
          "List of comma-separated column names that need to be stored clustered for create table";
  private static final String CLUSTEREDATTRIBUTES_DISPLAY = "Cluster column names";

  public static final String DISTRIBUTIONATTRIBUTES = "distributionattributes";
  private static final String DISTRIBUTIONATTRIBUTES_DEFAULT = "";
  private static final String DISTRIBUTIONATTRIBUTES_DOC =
          "List of comma-separated column names that need to be used as distribution columns for create table";
  private static final String DISTRIBUTIONATTRIBUTES_DISPLAY = "Distribution column names";

  public static final String PARTITIONS = "partitions";
  private static final int PARTITIONS_DEFAULT = 10;
  private static final String PARTITIONS_DOC =
          "The number of partitions to distribute the table data on.";
  private static final String PARTITIONS_DISPLAY = "Number of partitions";

  public static final String VALUE_CONVERTER_NAME = "value.converter";
  private static final String VALUE_CONVERTER_DOC =
          "Specify the class name of the value converter ";
  private static final String VALUE_CONVERTER_DEFAULT = "no.norsktipping.kafka.connect.converter.JsonConverter";
  private static final String VALUE_CONVERTER_DISPLAY = "Value converter class";

  private static final EnumRecommender QUOTE_METHOD_RECOMMENDER =
      EnumRecommender.in(QuoteMethod.values());

  private static final EnumRecommender TABLE_TYPES_RECOMMENDER =
      EnumRecommender.in(TableType.values());

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
        // Connection
        .define(
            CONNECTION_URL,
            ConfigDef.Type.STRING,
            ConfigDef.NO_DEFAULT_VALUE,
            ConfigDef.Importance.HIGH,
            CONNECTION_URL_DOC,
            CONNECTION_GROUP,
            1,
            ConfigDef.Width.LONG,
            CONNECTION_URL_DISPLAY
        )
        .define(
            CONNECTION_USER,
            ConfigDef.Type.STRING,
            null,
            ConfigDef.Importance.HIGH,
            CONNECTION_USER_DOC,
            CONNECTION_GROUP,
            2,
            ConfigDef.Width.MEDIUM,
            CONNECTION_USER_DISPLAY
        )
        .define(
            CONNECTION_PASSWORD,
            ConfigDef.Type.PASSWORD,
            null,
            ConfigDef.Importance.HIGH,
            CONNECTION_PASSWORD_DOC,
            CONNECTION_GROUP,
            3,
            ConfigDef.Width.MEDIUM,
            CONNECTION_PASSWORD_DISPLAY
        )
        .define(
            DIALECT_NAME_CONFIG,
            ConfigDef.Type.STRING,
            DIALECT_NAME_DEFAULT,
            DatabaseDialectRecommender.INSTANCE,
            ConfigDef.Importance.LOW,
            DIALECT_NAME_DOC,
            CONNECTION_GROUP,
            4,
            ConfigDef.Width.LONG,
            DIALECT_NAME_DISPLAY,
            DatabaseDialectRecommender.INSTANCE
        )
        .define(
            DB_ENCODING,
            ConfigDef.Type.STRING,
            DB_ENCODING_DEFAULT,
            EnumValidator.in(DBEncoding.values()),
            ConfigDef.Importance.MEDIUM,
            DB_ENCODING_DOC,
            CONNECTION_GROUP,
            5,
            ConfigDef.Width.MEDIUM,
            DB_ENCODING_DISPLAY
        )
        .define(
            CONNECTION_ATTEMPTS,
            ConfigDef.Type.INT,
            CONNECTION_ATTEMPTS_DEFAULT,
            ConfigDef.Range.atLeast(1),
            ConfigDef.Importance.LOW,
            CONNECTION_ATTEMPTS_DOC,
            CONNECTION_GROUP,
            6,
            ConfigDef.Width.SHORT,
            CONNECTION_ATTEMPTS_DISPLAY
        ).define(
            CONNECTION_BACKOFF,
            ConfigDef.Type.LONG,
            CONNECTION_BACKOFF_DEFAULT,
            ConfigDef.Importance.LOW,
            CONNECTION_BACKOFF_DOC,
            CONNECTION_GROUP,
            7,
            ConfigDef.Width.SHORT,
            CONNECTION_BACKOFF_DISPLAY
        )
        // Writes
        .define(
            INSERT_MODE,
            ConfigDef.Type.STRING,
            INSERT_MODE_DEFAULT,
            EnumValidator.in(InsertMode.values()),
            ConfigDef.Importance.HIGH,
            INSERT_MODE_DOC,
            WRITES_GROUP,
            1,
            ConfigDef.Width.MEDIUM,
            INSERT_MODE_DISPLAY
        )
        .define(
            BATCH_SIZE,
            ConfigDef.Type.INT,
            BATCH_SIZE_DEFAULT,
            NON_NEGATIVE_INT_VALIDATOR,
            ConfigDef.Importance.MEDIUM,
            BATCH_SIZE_DOC, WRITES_GROUP,
            2,
            ConfigDef.Width.SHORT,
            BATCH_SIZE_DISPLAY
        )
        .define(
            DELETE_ENABLED,
            ConfigDef.Type.BOOLEAN,
            DELETE_ENABLED_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            DELETE_ENABLED_DOC, WRITES_GROUP,
            3,
            ConfigDef.Width.SHORT,
            DELETE_ENABLED_DISPLAY
        )
        .define(
            TABLE_TYPES_CONFIG,
            ConfigDef.Type.LIST,
            TABLE_TYPES_DEFAULT,
            TABLE_TYPES_RECOMMENDER,
            ConfigDef.Importance.LOW,
            TABLE_TYPES_DOC,
            WRITES_GROUP,
            4,
            ConfigDef.Width.MEDIUM,
            TABLE_TYPES_DISPLAY
        )
        // Data Mapping
        .define(
            TABLE_NAME_FORMAT,
            ConfigDef.Type.STRING,
            TABLE_NAME_FORMAT_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            TABLE_NAME_FORMAT_DOC,
            DATAMAPPING_GROUP,
            1,
            ConfigDef.Width.LONG,
            TABLE_NAME_FORMAT_DISPLAY
        )
        /*.define(
            PK_MODE,
            ConfigDef.Type.STRING,
            PK_MODE_DEFAULT,
            EnumValidator.in(PrimaryKeyMode.values()),
            ConfigDef.Importance.HIGH,
            PK_MODE_DOC,
            DATAMAPPING_GROUP,
            2,
            ConfigDef.Width.MEDIUM,
            PK_MODE_DISPLAY,
            PrimaryKeyModeRecommender.INSTANCE
        )*/
        .define(
            COORDINATES_ENABLED,
            ConfigDef.Type.BOOLEAN,
            COORDINATES_ENABLED_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            COORDINATES_ENABLED_DOC, DATAMAPPING_GROUP,
            2,
            ConfigDef.Width.SHORT,
            COORDINATES_ENABLED_DISPLAY
        )
        .define(
            UPSERT_KEYS,
            ConfigDef.Type.LIST,
            UPSERT_KEYS_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            UPSERT_KEYS_DOC,
            DATAMAPPING_GROUP,
            3,
            ConfigDef.Width.LONG, UPSERT_KEYS_DISPLAY
        )
          .define(
            DELETE_KEYS,
            ConfigDef.Type.LIST,
            DELETE_KEYS_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            DELETE_KEYS_DOC,
            DATAMAPPING_GROUP,
            4,
            ConfigDef.Width.MEDIUM,
            DELETE_KEYS_DISPLAY
          )
        /*.define(
            FIELDS_WHITELIST,
            ConfigDef.Type.LIST,
            FIELDS_WHITELIST_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            FIELDS_WHITELIST_DOC,
            DATAMAPPING_GROUP,
            4,
            ConfigDef.Width.LONG,
            FIELDS_WHITELIST_DISPLAY
        )*/.define(
          DB_TIMEZONE_CONFIG,
          ConfigDef.Type.STRING,
          DB_TIMEZONE_DEFAULT,
          TimeZoneValidator.INSTANCE,
          ConfigDef.Importance.MEDIUM,
          DB_TIMEZONE_CONFIG_DOC,
          DATAMAPPING_GROUP,
          5,
          ConfigDef.Width.MEDIUM,
          DB_TIMEZONE_CONFIG_DISPLAY
        )
        // DDL
        .define(
            AUTO_CREATE,
            ConfigDef.Type.BOOLEAN,
            AUTO_CREATE_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            AUTO_CREATE_DOC, DDL_GROUP,
            1,
            ConfigDef.Width.SHORT,
            AUTO_CREATE_DISPLAY
        )
        .define(
            AUTO_EVOLVE,
            ConfigDef.Type.BOOLEAN,
            AUTO_EVOLVE_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            AUTO_EVOLVE_DOC, DDL_GROUP,
            2,
            ConfigDef.Width.SHORT,
            AUTO_EVOLVE_DISPLAY
        ).define(
            QUOTE_SQL_IDENTIFIERS_CONFIG,
            ConfigDef.Type.STRING,
            QUOTE_SQL_IDENTIFIERS_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            QUOTE_SQL_IDENTIFIERS_DOC,
            DDL_GROUP,
            3,
            ConfigDef.Width.MEDIUM,
            QUOTE_SQL_IDENTIFIERS_DISPLAY,
            QUOTE_METHOD_RECOMMENDER
        )
        // Retries
        .define(
            MAX_RETRIES,
            ConfigDef.Type.INT,
            MAX_RETRIES_DEFAULT,
            NON_NEGATIVE_INT_VALIDATOR,
            ConfigDef.Importance.MEDIUM,
            MAX_RETRIES_DOC,
            RETRIES_GROUP,
            1,
            ConfigDef.Width.SHORT,
            MAX_RETRIES_DISPLAY
        )
        .define(
            RETRY_BACKOFF_MS,
            ConfigDef.Type.INT,
            RETRY_BACKOFF_MS_DEFAULT,
            NON_NEGATIVE_INT_VALIDATOR,
            ConfigDef.Importance.MEDIUM,
            RETRY_BACKOFF_MS_DOC,
            RETRIES_GROUP,
            2,
            ConfigDef.Width.SHORT,
            RETRY_BACKOFF_MS_DISPLAY
        ).define(
          PAYLOAD_FIELD_NAME,
          ConfigDef.Type.STRING,
          PAYLOAD_FIELD_NAME_DEFAULT,
          ConfigDef.Importance.HIGH,
          PAYLOAD_FIELD_NAME_DOC
          ).define(
          ZONEMAPATTRIBUTES,
          ConfigDef.Type.LIST,
          ZONEMAPATTRIBUTES_DEFAULT,
          ConfigDef.Importance.MEDIUM,
          ZONEMAPATTRIBUTES_DOC,
          WRITES_GROUP,
            6,
          ConfigDef.Width.LONG,
          ZONEMAPATTRIBUTES_DISPLAY
          ).define(
          CLUSTEREDATTRIBUTES,
          ConfigDef.Type.LIST,
          CLUSTEREDATTRIBUTES_DEFAULT,
          ConfigDef.Importance.MEDIUM,
          CLUSTEREDATTRIBUTES_DOC,
          WRITES_GROUP,
          7,
          ConfigDef.Width.LONG,
          CLUSTEREDATTRIBUTES_DISPLAY
          ).define(
          DISTRIBUTIONATTRIBUTES,
          ConfigDef.Type.LIST,
          DISTRIBUTIONATTRIBUTES_DEFAULT,
          ConfigDef.Importance.MEDIUM,
          DISTRIBUTIONATTRIBUTES_DOC,
          WRITES_GROUP,
          8,
          ConfigDef.Width.LONG,
          DISTRIBUTIONATTRIBUTES_DISPLAY
          ).define(
          PARTITIONS,
          ConfigDef.Type.INT,
          PARTITIONS_DEFAULT,
          NON_NEGATIVE_INT_VALIDATOR,
          ConfigDef.Importance.LOW,
          PARTITIONS_DOC,
          WRITES_GROUP,
            9,
          ConfigDef.Width.SHORT,
          PARTITIONS_DISPLAY
          ).define(
            VALUE_CONVERTER_NAME,
            ConfigDef.Type.STRING,
            "no.norsktipping.kafka.connect.converter.JsonConverter",
            new ConfigDef.Validator() {
              @Override
              public void ensureValid(String key, Object value) {
                if (!value.equals("no.norsktipping.kafka.connect.converter.JsonConverter")) {
                  throw new ConfigException(key, value, "Invalid value converter");
                }
              }
            },
            ConfigDef.Importance.HIGH,
            VALUE_CONVERTER_DOC,
            WRITES_GROUP,
            10,
            ConfigDef.Width.SHORT,
            VALUE_CONVERTER_DISPLAY
          ).define(
            SCHEMA_NAMES,
            ConfigDef.Type.LIST,
            new ArrayList<>(),
            ConfigDef.Importance.HIGH,
            SCHEMA_NAMES_DOC
          ).define(
            UPPERCASE,
            ConfigDef.Type.BOOLEAN,
            UPPERCASE_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            UPPERCASE_DOC, WRITES_GROUP,
            11,
            ConfigDef.Width.SHORT,
            UPPERCASE_DISPLAY
          )
          ;

  public final String connectorName;
  public final String connectionUrl;
  public final String connectionUser;
  public final String connectionPassword;
  public final int connectionAttempts;
  public final long connectionBackoffMs;
  public final String tableNameFormat;
  public final int batchSize;
  public final boolean deleteEnabled;
  public final boolean coordinatesEnabled;
  public final int maxRetries;
  public final int retryBackoffMs;
  public final boolean autoCreate;
  public final boolean autoEvolve;
  public final InsertMode insertMode;
//  public final PrimaryKeyMode pkMode;
  public final Set<String> upsertKeys;
  public final Set<String> deleteKeys;

  //public final Set<String> fieldsWhitelist;
  public final String dialectName;
  public final Charset dbEncoding;
  public final TimeZone timeZone;
  public final EnumSet<TableType> tableTypes;
  public final String payloadFieldName;
  public final List<String> schemaNames;
  public final List<String> clusteredattributes;
  public final List<String> distributionattributes;
  public final List<String> zonemapattributes;
  public final int partitions;
  //UCASE: uppercase boolean
  public static boolean uppercase;

  public JdbcSinkConfig(Map<?, ?> props) {
    super(CONFIG_DEF, props);
    uppercase = getBoolean(UPPERCASE);
    connectorName = ConfigUtils.connectorName(props);
    connectionUrl = getString(CONNECTION_URL);
    connectionUser = getString(CONNECTION_USER);
    connectionPassword = getPasswordValue(CONNECTION_PASSWORD);
    connectionAttempts = getInt(CONNECTION_ATTEMPTS);
    connectionBackoffMs = getLong(CONNECTION_BACKOFF);
    tableNameFormat = getString(TABLE_NAME_FORMAT).trim();
    batchSize = getInt(BATCH_SIZE);
    deleteEnabled = getBoolean(DELETE_ENABLED);
    coordinatesEnabled = getBoolean(COORDINATES_ENABLED);
    maxRetries = getInt(MAX_RETRIES);
    retryBackoffMs = getInt(RETRY_BACKOFF_MS);
    autoCreate = getBoolean(AUTO_CREATE);
    autoEvolve = getBoolean(AUTO_EVOLVE);
    insertMode = InsertMode.valueOf(getString(INSERT_MODE).toUpperCase());
    //pkMode = PrimaryKeyMode.valueOf(getString(PK_MODE).toUpperCase());
    upsertKeys = new HashSet<>(getList(UPSERT_KEYS));
    deleteKeys = new HashSet<>(getList(DELETE_KEYS));
    dialectName = getString(DIALECT_NAME_CONFIG);
    dbEncoding = getCharset(DBEncoding.valueOf(getString(DB_ENCODING).toUpperCase()));
//    fieldsWhitelist = new HashSet<>(getList(FIELDS_WHITELIST));
    String dbTimeZone = getString(DB_TIMEZONE_CONFIG);
    timeZone = TimeZone.getTimeZone(ZoneId.of(dbTimeZone));
    payloadFieldName = getString(PAYLOAD_FIELD_NAME).trim();
    schemaNames = getList(SCHEMA_NAMES);
    clusteredattributes = getList(CLUSTEREDATTRIBUTES).stream().map(ca -> uppercase ? ca.toUpperCase() : ca.toLowerCase()).collect(Collectors.toList());
    distributionattributes = getList(DISTRIBUTIONATTRIBUTES).stream().map(da -> uppercase ? da.toUpperCase() : da.toLowerCase()).collect(Collectors.toList());
    zonemapattributes = getList(ZONEMAPATTRIBUTES).stream().map(zm -> uppercase ? zm.toUpperCase() : zm.toLowerCase()).collect(Collectors.toList());
    partitions = getInt(PARTITIONS);
    //uppercase or lowercase for columns and table names boolean


    Map<String, ArrayList<Object>> schemaValues = schemaNames.stream().map(schemaName ->
            new AbstractMap.SimpleEntry<>(
                    schemaName,
                    new ArrayList<>(this.originalsWithPrefix("value.converter." + schemaName + ".")
                            .values())))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    if (insertMode == InsertMode.UPSERT || insertMode == InsertMode.UPDATE) {
      if (upsertKeys.isEmpty() || !schemaValues.values().stream().allMatch(values -> values.containsAll(upsertKeys))) {
        throw new ConfigException(
        String.format("All %s configured in %s must be configured with a subset of keys configured as <newname> in value converter configuration i.e. %s when %s is %s or %s",
                upsertKeys, UPSERT_KEYS, "value.converer.<schemaname>.<oldname>,<newname>", INSERT_MODE, InsertMode.UPSERT, InsertMode.UPDATE));
      }
    }
    if (deleteEnabled) {
      if (deleteKeys.isEmpty() || !schemaValues.values().stream().allMatch(values -> values.containsAll(deleteKeys))) {
        throw new ConfigException(
                String.format("Delete keys configured in %s must be configured with a matching key configured as <newname> in value converter configuration i.e. %s when %s is %s",
                        DELETE_KEYS, "value.converer.<schemaname>.<oldname>,<newname>", DELETE_ENABLED, "true"));
      }
    }
    tableTypes = TableType.parse(getList(TABLE_TYPES_CONFIG));

  }

  public static String ucase(String string) {
    return uppercase ? string.toUpperCase() : string.toLowerCase();
  }


  private Charset getCharset(DBEncoding encoding) {
    switch (encoding) {
      case UTF8:
        return StandardCharsets.UTF_8;
      case UTF16:
        return StandardCharsets.UTF_16;
    }
    throw new ConfigException(
            String.format("DB encoding %s is not recognized. Legal values are %s", encoding, Arrays.toString(DBEncoding.values())));
  }

  private String getPasswordValue(String key) {
    Password password = getPassword(key);
    if (password != null) {
      return password.value();
    }
    return null;
  }

  public String connectorName() {
    return connectorName;
  }

  public EnumSet<TableType> tableTypes() {
    return tableTypes;
  }

  public Set<String> tableTypeNames() {
    return tableTypes().stream().map(TableType::toString).collect(Collectors.toSet());
  }

  private static class EnumValidator implements ConfigDef.Validator {
    private final List<String> canonicalValues;
    private final Set<String> validValues;

    private EnumValidator(List<String> canonicalValues, Set<String> validValues) {
      this.canonicalValues = canonicalValues;
      this.validValues = validValues;
    }

    public static <E> EnumValidator in(E[] enumerators) {
      final List<String> canonicalValues = new ArrayList<>(enumerators.length);
      final Set<String> validValues = new HashSet<>(enumerators.length * 2);
      for (E e : enumerators) {
        canonicalValues.add(e.toString().toLowerCase());
        validValues.add(e.toString().toUpperCase());
        validValues.add(e.toString().toLowerCase());
      }
      return new EnumValidator(canonicalValues, validValues);
    }

    @Override
    public void ensureValid(String key, Object value) {
      if (!validValues.contains(value)) {
        throw new ConfigException(key, value, "Invalid enumerator");
      }
    }

    @Override
    public String toString() {
      return canonicalValues.toString();
    }
  }

  public static void main(String... args) {
    System.out.println(CONFIG_DEF.toEnrichedRst());
  }

}

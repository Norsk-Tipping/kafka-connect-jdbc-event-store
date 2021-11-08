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

import io.confluent.connect.jdbc.dialect.DatabaseDialectProvider.SubprotocolBasedProvider;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig.InsertMode;
import io.confluent.connect.jdbc.sink.PreparedStatementBinder;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.sink.metadata.SchemaPair;
import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import io.confluent.connect.jdbc.util.*;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.errors.ConnectException;

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.sql.*;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * A {@link DatabaseDialect} for Oracle.
 */
public class OracleDatabaseDialect extends GenericDatabaseDialect {

  /**
   * The provider for {@link OracleDatabaseDialect}.
   */
  public static class Provider extends SubprotocolBasedProvider {
    public Provider() {
      super(OracleDatabaseDialect.class.getSimpleName(), "oracle");
    }

    @Override
    public DatabaseDialect create(AbstractConfig config) {
      return new OracleDatabaseDialect(config);
    }
  }

  /**
   * Create a new dialect instance with the given connector configuration.
   *
   * @param config the connector configuration; may not be null
   */
  public OracleDatabaseDialect(AbstractConfig config) {
    super(config, new IdentifierRules(".", "\"", "\""));
  }

  @Override
  protected String currentTimestampDatabaseQuery() {
    return "select CURRENT_TIMESTAMP from dual";
  }

  @Override
  protected String checkConnectionQuery() {
    return "SELECT 1 FROM DUAL";
  }

  @Override
  public StatementBinder statementBinder(
      PreparedStatement statement,
      SchemaPair schemaPair,
      FieldsMetadata fieldsMetadata,
      TableDefinition tableDefinition,
      InsertMode insertMode,
      Boolean coordinates
  ) {
    return new PreparedStatementBinder(
        this,
        statement,
        schemaPair,
        fieldsMetadata,
        tableDefinition,
        insertMode,
        coordinates
    );
  }

  @Override
  public void bindField(
      PreparedStatement statement,
      int index,
      Schema schema,
      Object value,
      ColumnDefinition colDef
  ) throws SQLException {
    if (value == null) {
      statement.setObject(index, null);
    } else {
      boolean bound = maybeBindLogical(statement, index, schema, value);
      if (!bound) {
        bound = maybeBindPrimitive(statement, index, schema, value, colDef);
      }
      if (!bound) {
        throw new ConnectException("Unsupported source data type: " + schema.type());
      }
    }
  }

  protected boolean maybeBindPrimitive(
      PreparedStatement statement,
      int index,
      Schema schema,
      Object value,
      ColumnDefinition colDef
  ) throws SQLException {
    if (colDef == null) {
      return super.maybeBindPrimitive(statement, index, schema, value);
    }

    if (schema.type() == Type.STRING) {
      if (colDef.type() == Types.BLOB) {
        statement.setBytes(index, ((String)value).getBytes(super.dbEncoding));
        return true;
      } else if (colDef.type() == Types.CLOB) {
        statement.setString(index, ((String) value));
        return true;
      } else if (colDef.type() == Types.NCLOB) {
        statement.setNCharacterStream(index, new StringReader((String) value));
        return true;
      } else if (colDef.type() == Types.NVARCHAR || colDef.type() == Types.NCHAR) {
        statement.setNString(index, (String) value);
        return true;
      } else {
        return super.maybeBindPrimitive(statement, index, schema, value);
      }
    }

    if (schema.type() == Type.BYTES && colDef.type() == Types.BLOB) {
      if (value instanceof ByteBuffer) {
        statement.setBlob(index, new ByteArrayInputStream(((ByteBuffer) value).array()));
      } else if (value instanceof byte[]) {
        statement.setBlob(index, new ByteArrayInputStream((byte[]) value));
      } else {
        return super.maybeBindPrimitive(statement, index, schema, value);
      }
      return true;
    }
    return super.maybeBindPrimitive(statement, index, schema, value);
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  @Override
  protected String getSqlType(SinkRecordField field) {
    if (field.schemaName() != null) {
      switch (field.schemaName()) {
        case Decimal.LOGICAL_NAME:
          return "NUMBER(*," + field.schemaParameters().get(Decimal.SCALE_FIELD) + ")";
        case Date.LOGICAL_NAME:
          return "DATE";
        case Time.LOGICAL_NAME:
          return "DATE";
        case Timestamp.LOGICAL_NAME:
          return "TIMESTAMP";
        default:
          // fall through to normal types
      }
    }
    switch (field.schemaType()) {
      case INT8:
        return "NUMBER(3,0)";
      case INT16:
        return "NUMBER(5,0)";
      case INT32:
        return "NUMBER(10,0)";
      case INT64:
        return "NUMBER(19,0)";
      case FLOAT32:
        return "BINARY_FLOAT";
      case FLOAT64:
        return "BINARY_DOUBLE";
      case BOOLEAN:
        return "NUMBER(1,0)";
      case STRING:
        if (field.name() != null && field.name().equals(converterPayloadFieldName())) {
          return "BLOB";
        }
        if (field.name() != null && clusteredAttributes().contains(field.name())) {
          return "VARCHAR2(100)";
        }
        return "CLOB";
      case BYTES:
        return "BLOB";
      default:
        return super.getSqlType(field);
    }
  }

  @Override
  public void applyDdlStatements(
          Connection connection,
          List<String> statements
  ) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      for (String ddlStatement : statements) {
        Pattern pattern = Pattern.compile(";");
        List<String> individuals = pattern.splitAsStream(ddlStatement).collect(Collectors.toList());
        for (String individual : individuals) {
          statement.executeUpdate(individual);
        }
      }
    }
  }

  @Override
  public String buildCreateTableStatement(
          TableId table,
          Collection<SinkRecordField> fields
  ) {
    ExpressionBuilder builder = expressionBuilder();
    //final List<String> pkFieldNames = extractPrimaryKeyFieldNames(fields);
    builder.append("CREATE TABLE ");
    builder.append(table);
    builder.append(" (");
    writeColumnsSpec(builder, fields);
/*    builder.append(",");
    builder.append(System.lineSeparator());
    builder.append("CONSTRAINT ");
    builder.append(table.tableName());
    builder.append("_ensure_json ");
    builder.append("CHECK (");
    builder.appendColumnName(converterPayloadFieldName());
    builder.append(" IS JSON)");*/
    builder.append(")");
    builder.append(System.lineSeparator());
    if (!distributionAttributes().isEmpty()) {
      builder.append(System.lineSeparator());
      builder.append("PARTITION BY HASH (");
      builder.appendList()
              .delimitedBy(",")
              .transformedBy(ExpressionBuilder.quote())
              .of(distributionAttributes());
      builder.append(")");
      builder.append("(");
      for (int i=0; i<partitions(); i++) {
        builder.append(System.lineSeparator());
        builder.append("PARTITION ");
        builder.appendColumnName(table.tableName() + "_h" + i, QuoteMethod.ALWAYS);
        if (i < partitions()-1) {builder.append(",");}
      }
      builder.append(")");
    }
    if (!clusteredAttributes().isEmpty()) {
      builder.append(System.lineSeparator());
      builder.append(" CLUSTERING BY LINEAR ORDER (");
      builder.appendList()
              .delimitedBy(",")
              .transformedBy(ExpressionBuilder.quote())
              .of(clusteredAttributes());
      builder.append(")");
    }
    if (!zonemapAttributes().isEmpty()) {
      builder.append(";");
      builder.append(System.lineSeparator());
      builder.append("CREATE MATERIALIZED ZONEMAP ");
      builder.appendColumnName(table.tableName() +"_zmap", QuoteMethod.ALWAYS);
      builder.append(" REFRESH FAST ON COMMIT ");
      builder.append("ON ");
      builder.append(table);
      builder.append(" (");
      builder.appendList()
              .delimitedBy(",")
              .transformedBy(ExpressionBuilder.quote())
              .of(zonemapAttributes());
      builder.append(")");
    }
    builder.append(";");
    return builder.toString();
  }

  @Override
  public String buildDropTableStatement(
      TableId table,
      DropOptions options
  ) {
    // https://stackoverflow.com/questions/1799128/oracle-if-table-exists
    ExpressionBuilder builder = expressionBuilder();

    builder.append("DROP TABLE ");
    builder.append(table);
    if (options.cascade()) {
      builder.append(" CASCADE CONSTRAINTS");
    }
    String dropStatement = builder.toString();

    if (!options.ifExists()) {
      return dropStatement;
    }
    builder = expressionBuilder();
    builder.append("BEGIN ");
    // The drop statement includes double quotes for identifiers, so that's compatible with the
    // single quote used to delimit the string literal
    // https://docs.oracle.com/cd/B28359_01/appdev.111/b28370/literal.htm#LNPLS01326
    builder.append("EXECUTE IMMEDIATE '" + dropStatement + "' ");
    builder.append("EXCEPTION ");
    builder.append("WHEN OTHERS THEN ");
    builder.append("IF SQLCODE != -942 THEN ");
    builder.append("    RAISE;");
    builder.append("END IF;");
    builder.append("END;");
    return builder.toString();
  }

  @Override
  public List<String> buildAlterTable(
      TableId table,
      Collection<SinkRecordField> fields
  ) {
    ExpressionBuilder builder = expressionBuilder();
    builder.append("ALTER TABLE ");
    builder.append(table);
    builder.append(" ADD(");
    writeColumnsSpec(builder, fields);
    builder.append(")");
    return Collections.singletonList(builder.toString());
  }


  @Override
  protected String sanitizedUrl(String url) {
    // Oracle can also have ":username/password@" after the driver type
    return super.sanitizedUrl(url)
                .replaceAll("(:thin:[^/]*)/([^@]*)@", "$1/****@")
                .replaceAll("(:oci[^:]*:[^/]*)/([^@]*)@", "$1/****@");
  }
}

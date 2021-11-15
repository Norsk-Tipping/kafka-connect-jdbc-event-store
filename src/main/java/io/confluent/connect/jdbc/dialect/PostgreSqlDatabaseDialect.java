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
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import io.confluent.connect.jdbc.util.*;
import io.confluent.connect.jdbc.util.ExpressionBuilder.Transform;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.data.Schema.Type;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * A {@link DatabaseDialect} for PostgreSQL.
 */
public class PostgreSqlDatabaseDialect extends GenericDatabaseDialect {

  private final Logger log = LoggerFactory.getLogger(PostgreSqlDatabaseDialect.class);

  /**
   * The provider for {@link PostgreSqlDatabaseDialect}.
   */
  public static class Provider extends SubprotocolBasedProvider {
    public Provider() {
      super(PostgreSqlDatabaseDialect.class.getSimpleName(), "postgresql");
    }

    @Override
    public DatabaseDialect create(AbstractConfig config) {
      return new PostgreSqlDatabaseDialect(config);
    }
  }

  static final String JSON_TYPE_NAME = "json";
  static final String JSONB_TYPE_NAME = "jsonb";
  static final String UUID_TYPE_NAME = "uuid";
  private String tablename;

  /**
   * Define the PG datatypes that require casting upon insert/update statements.
   */
  private static final Set<String> CAST_TYPES = Collections.unmodifiableSet(
      Utils.mkSet(
          JSON_TYPE_NAME,
          JSONB_TYPE_NAME,
          UUID_TYPE_NAME
      )
  );

  /**
   * Create a new dialect instance with the given connector configuration.
   *
   * @param config the connector configuration; may not be null
   */
  public PostgreSqlDatabaseDialect(AbstractConfig config) {
    super(config, new IdentifierRules(".", "\"", "\""));
  }

  /**
   * Perform any operations on a {@link PreparedStatement} before it is used. This is called from
   * the {@link #createPreparedStatement(Connection, String)} method after the statement is
   * created but before it is returned/used.
   *
   * <p>This method sets the {@link PreparedStatement#setFetchDirection(int) fetch direction}
   * to {@link ResultSet#FETCH_FORWARD forward} as an optimization for the driver to allow it to
   * scroll more efficiently through the result set and prevent out of memory errors.
   *
   * @param stmt the prepared statement; never null
   * @throws SQLException the error that might result from initialization
   */
  @Override
  protected void initializePreparedStatement(PreparedStatement stmt) throws SQLException {
    super.initializePreparedStatement(stmt);

    log.trace("Initializing PreparedStatement fetch direction to FETCH_FORWARD for '{}'", stmt);
    stmt.setFetchDirection(ResultSet.FETCH_FORWARD);
  }

  @Override
  public PreparedStatement createPreparedStatement(
          Connection db,
          String query
  ) throws SQLException {
    if (!query.toLowerCase().startsWith("insert into")) {
      log.debug("Creating a PreparedStatement '{}'", query);
      PreparedStatement stmt = db.prepareStatement(query);
      initializePreparedStatement(stmt);
      return stmt;
    } else {
      log.debug("Creating a BulkloadStatement");
      BaseConnection bc = db.unwrap(BaseConnection.class);
      CopyManager cm = bc.getCopyAPI();

      PreparedStatement stmt = null;
      try {
        stmt = new BulkLoadPreparedStatement(cm, rowlength, tablename);
      } catch (IOException e) {
        throw new SQLException("Could not create prepared statement for bulk load", e);
      }
      return stmt;
    }
  }

  @Override
  public void bindField(
          PreparedStatement statement,
          int index,
          Schema schema,
          Object value,
          ColumnDefinition colDef
  ) throws SQLException {
    if (statement instanceof BulkLoadPreparedStatement) {
      if (value == null) {
        statement.setString(index, null);
      } else {
        statement.setString(index, value.toString());
      }
    } else {
      super.bindField(statement, index, schema, value, colDef);
    }
  }

  @Override
  protected String getSqlType(SinkRecordField field) {
    if (field.schemaName() != null) {
      switch (field.schemaName()) {
        case Decimal.LOGICAL_NAME:
          return "DECIMAL";
        case Date.LOGICAL_NAME:
          return "DATE";
        case Time.LOGICAL_NAME:
          return "TIME";
        case Timestamp.LOGICAL_NAME:
          return "TIMESTAMP";
        default:
          // fall through to normal types
      }
    }
    switch (field.schemaType()) {
      case INT8:
        return "SMALLINT";
      case INT16:
        return "SMALLINT";
      case INT32:
        return "INT";
      case INT64:
        return "BIGINT";
      case FLOAT32:
        return "REAL";
      case FLOAT64:
        return "DOUBLE PRECISION";
      case BOOLEAN:
        return "BOOLEAN";
      case STRING:
        if (field.name() != null && JdbcSinkConfig.ucase(field.name()).equals(converterPayloadFieldName())) {
          return "JSONB";
        }
        return "TEXT";
      case BYTES:
        return "BYTEA";
      default:
        return super.getSqlType(field);
    }
  }

  @Override
  public String buildCreateTableStatement(
          TableId table,
          Collection<SinkRecordField> fields
  ) {
    ExpressionBuilder builder = expressionBuilder();

    builder.append("CREATE TABLE ");
    builder.append(table);
    builder.append(" (");
    writeColumnsSpec(builder, fields);
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
    }
    if (!distributionAttributes().isEmpty()) {
      builder.append(";");
      for (int i=0; i<partitions(); i++) {
        builder.append(System.lineSeparator());
        builder.append("CREATE TABLE ");
        builder.appendColumnName(table.tableName() + "_h" + i, QuoteMethod.ALWAYS);
        builder.append(" PARTITION OF ");
        builder.append(table);
        builder.append(" FOR VALUES WITH (modulus ");
        builder.append(partitions());
        builder.append(", remainder ");
        builder.append(i);
        builder.append(")");
        if (i<partitions()-1) {builder.append(";");}
      }
    }
    if (!zonemapAttributes().isEmpty()) {
      builder.append(";");
      if (!distributionAttributes().isEmpty()) {
        for (int i=0; i<partitions(); i++) {
          int finalI = i;
          zonemapAttributes().forEach(ca -> {
            builder.append(System.lineSeparator());
            builder.append("CREATE INDEX ");
            builder.appendColumnName("brin_" + table.tableName() + "_h" + finalI + "_" + ca, QuoteMethod.ALWAYS);
            builder.append(" ON ");
            builder.appendColumnName(table.tableName() + "_h" + finalI, QuoteMethod.ALWAYS);
            builder.append(" USING brin(");
            builder.appendColumnName(ca, QuoteMethod.ALWAYS);
            builder.append(")");
            if (finalI < partitions()) {builder.append(";");}
          });
        }
      } else {
        zonemapAttributes().forEach(ca -> {
          builder.append(System.lineSeparator());
          builder.append("CREATE INDEX ");
          builder.appendColumnName("brin_" + table.tableName() + "_" + ca, QuoteMethod.ALWAYS);
          builder.append(" ON ");
          builder.append(table);
          builder.append(" USING brin(");
          builder.appendColumnName(ca, QuoteMethod.ALWAYS);
          builder.append(")");
        });
      }
    }
    if (!clusteredAttributes().isEmpty()) {
      builder.append(";");
      builder.append(System.lineSeparator());
      builder.append("CREATE EXTENSION IF NOT EXISTS bloom;");
      if (!distributionAttributes().isEmpty()) {
        for (int i=0; i<partitions(); i++) {
            builder.append(System.lineSeparator());
            builder.append("CREATE INDEX ");
            builder.appendColumnName("bloom_" + table.tableName() + "_h" + i, QuoteMethod.ALWAYS);
            builder.append(" ON ");
            builder.appendColumnName(table.tableName() + "_h" + i, QuoteMethod.ALWAYS);
            builder.append(" USING bloom(");
            builder.appendList()
                    .delimitedBy(",")
                    .transformedBy(ExpressionBuilder.quote())
                    .of(clusteredAttributes());
            builder.append(")");
            if (i<partitions()-1) {builder.append(";");}
        }
      } else {
        builder.append(System.lineSeparator());
        builder.appendColumnName("CREATE INDEX bloom_" + table.tableName(), QuoteMethod.ALWAYS);
        builder.append(" ON ");
        builder.append(table);
        builder.append(" USING bloom(");
        builder.appendList()
                .delimitedBy(",")
                .transformedBy(ExpressionBuilder.quote())
                .of(clusteredAttributes());
        builder.append(")");
      }
    }
    builder.append(";");
    return builder.toString();
  }

  @Override
  public String buildInsertStatement(
      TableId table,
      Collection<ColumnId> nonKeyColumns,
      TableDefinition definition
  ) {
    ExpressionBuilder builder = expressionBuilder();
    builder.append("INSERT INTO ");
    builder.append(table);
    builder.append(" (");
    builder.appendList()
           .delimitedBy(",")
           .transformedBy(ExpressionBuilder.columnNames())
           .of(nonKeyColumns);
    builder.append(") VALUES (");
    builder.appendList()
           .delimitedBy(",")
           .transformedBy(this.columnValueVariables(definition))
           .of(nonKeyColumns);
    builder.append(")");
    tablename = table.tableName();
    return builder.toString();
  }

  @Override
  public String buildUpdateStatement(
      TableId table,
      Collection<ColumnId> keyColumns,
      Collection<ColumnId> nonKeyColumns,
      TableDefinition definition
  ) {
    ExpressionBuilder builder = expressionBuilder();
    builder.append("UPDATE ");
    builder.append(table);
    builder.append(" SET ");
    builder.appendList()
           .delimitedBy(", ")
           .transformedBy(this.columnNamesWithValueVariables(definition))
           .of(nonKeyColumns);
    if (!keyColumns.isEmpty()) {
      builder.append(" WHERE ");
      builder.appendList()
             .delimitedBy(" AND ")
             .transformedBy(ExpressionBuilder.columnNamesWith(" = ?"))
             .of(keyColumns);
    }
    return builder.toString();
  }

  @Override
  protected void formatColumnValue(
      ExpressionBuilder builder,
      String schemaName,
      Map<String, String> schemaParameters,
      Schema.Type type,
      Object value
  ) {
    if (schemaName == null && Type.BOOLEAN.equals(type)) {
      builder.append((Boolean) value ? "TRUE" : "FALSE");
    } else {
      super.formatColumnValue(builder, schemaName, schemaParameters, type, value);
    }
  }

  /**
   * Return the transform that produces an assignment expression each with the name of one of the
   * columns and the prepared statement variable. PostgreSQL may require the variable to have a
   * type suffix, such as {@code ?::uuid}.
   *
   * @param defn the table definition; may be null if unknown
   * @return the transform that produces the assignment expression for use within a prepared
   *         statement; never null
   */
  protected Transform<ColumnId> columnNamesWithValueVariables(TableDefinition defn) {
    return (builder, columnId) -> {
      builder.appendColumnName(columnId.name());
      builder.append(" = ?");
      builder.append(valueTypeCast(defn, columnId));
    };
  }

  /**
   * Return the transform that produces a prepared statement variable for each of the columns.
   * PostgreSQL may require the variable to have a type suffix, such as {@code ?::uuid}.
   *
   * @param defn the table definition; may be null if unknown
   * @return the transform that produces the variable expression for each column; never null
   */
  protected Transform<ColumnId> columnValueVariables(TableDefinition defn) {
    return (builder, columnId) -> {
      builder.append("?");
      builder.append(valueTypeCast(defn, columnId));
    };
  }

  /**
   * Return the typecast expression that can be used as a suffix for a value variable of the
   * given column in the defined table.
   *
   * <p>This method returns a blank string except for those column types that require casting
   * when set with literal values. For example, a column of type {@code uuid} must be cast when
   * being bound with with a {@code varchar} literal, since a UUID value cannot be bound directly.
   *
   * @param tableDefn the table definition; may be null if unknown
   * @param columnId  the column within the table; may not be null
   * @return the cast expression, or an empty string; never null
   */
  protected String valueTypeCast(TableDefinition tableDefn, ColumnId columnId) {
    if (tableDefn != null) {
      ColumnDefinition defn = tableDefn.definitionForColumn(columnId.name());
      if (defn != null) {
        String typeName = defn.typeName(); // database-specific
        if (typeName != null) {
          typeName = typeName.toLowerCase();
          if (CAST_TYPES.contains(typeName)) {
            return "::" + typeName;
          }
        }
      }
    }
    return "";
  }
}


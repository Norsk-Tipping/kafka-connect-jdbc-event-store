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
import io.confluent.connect.jdbc.dialect.DatabaseDialect.StatementBinder;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.sink.metadata.SchemaPair;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;

import static io.confluent.connect.jdbc.sink.JdbcSinkConfig.InsertMode.INSERT;
import static io.confluent.connect.jdbc.sink.JdbcSinkConfig.InsertMode.UPSERT;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

public class BufferedRecords {
  private static final Logger log = LoggerFactory.getLogger(BufferedRecords.class);

  private final TableId tableId;
  private final JdbcSinkConfig config;
  private final DatabaseDialect dbDialect;
  private final DbStructure dbStructure;
  private final Connection connection;

  private List<SinkRecord> records = new ArrayList<>();
  private Schema keySchema;
  private Schema valueSchema;
  private RecordValidator recordValidator;
  private FieldsMetadata fieldsMetadata;
  private PreparedStatement updatePreparedStatement;
  private PreparedStatement deletePreparedStatement;
  private PreparedStatement upsertDeletePreparedStatement;
  private StatementBinder updateStatementBinder;
  private StatementBinder deleteStatementBinder;
  private StatementBinder upsertDeleteStatementBinder;
  private boolean deletesInBatch = false;
  private boolean upsertDeletesInBatch = false;
  private boolean updatesInBatch = false;
  private final HashSet<Object> keys;

  public BufferedRecords(
      JdbcSinkConfig config,
      TableId tableId,
      DatabaseDialect dbDialect,
      DbStructure dbStructure,
      Connection connection
  ) {
    this.tableId = tableId;
    this.config = config;
    this.dbDialect = dbDialect;
    this.dbStructure = dbStructure;
    this.connection = connection;
    this.recordValidator = RecordValidator.create(config);
    this.keys = new HashSet<>();
  }

  public List<SinkRecord> add(SinkRecord record) throws SQLException {
    recordValidator.validate(record);
    final List<SinkRecord> flushed = new ArrayList<>();

    boolean schemaChanged = false;
    if (!Objects.equals(keySchema, record.keySchema())) {
      keySchema = record.keySchema();
      schemaChanged = true;
    }
    // tombstone
    if (isNull(record.valueSchema())) {
      // For deletes, value and optionally value schema come in as null.
      // We don't want to treat this as a schema change if key schemas is the same
      // otherwise we flush unnecessarily.
      if (config.deleteEnabled) {
        deletesInBatch = true;
      }
    } //insert or upsert (upsert related insert or delete) or update without schema change
    else if (Objects.equals(valueSchema, record.valueSchema())) {
      //flush any tombstones in buffer before adding new inserts/upsert/update that came in now
      //...this does allow many sequential tombstones to buffer up..until a new insert/upsert/update arrives
      if (config.deleteEnabled && deletesInBatch) {
        // flush so an insert after a delete of same record isn't lost
        flushed.addAll(flush());
      }
      if (!record.headers().isEmpty() && record.headers().allWithName("UPSERTDELETE").hasNext()) {
        // in case there have been buffered up upserts with the same key, flush
        if (keys.contains(config.upsertKeys.stream().map(uk -> ((Struct) record.value()).get(uk)).collect(Collectors.toList()))) {
          // flush so a tombstone after an upsert-related-delete-insert cycle of same record isn't lost
          // i.e. let the tombstone survive
          flushed.addAll(flush());
          record.headers().addBoolean("UPSERTDELETE", true);
        }
      }
    } else {
      // value schema is not null and has changed. This is a real schema change.
      valueSchema = record.valueSchema();
      schemaChanged = true;
    }
    if (schemaChanged /*|| updateStatementBinder == null*/) {
      // Each batch needs to have the same schemas, so get the buffered records out
      flushed.addAll(flush());

      // re-initialize everything that depends on the record schema
      final SchemaPair schemaPair = new SchemaPair(
          record.keySchema(),
          record.valueSchema()
      );

      fieldsMetadata = FieldsMetadata.extract(
          tableId.tableName(),
          config.insertMode,
          config.upsertKeys,
          config.deleteKeys,
          config.coordinatesEnabled,
          config.deleteEnabled,
          schemaPair
      );
      boolean cora = dbStructure.createOrAmendIfNecessary(
          config,
          connection,
          tableId,
          fieldsMetadata
      );
      if (isNull(record.value())  && isNull(deletePreparedStatement)) {
        if (!cora) {
          return flushed;
        }
      }

      final String insertSql = record.valueSchema() != null ? JdbcSinkConfig.ucase(getInsertSql()) : null;
      final String deleteSql = config.deleteEnabled ? JdbcSinkConfig.ucase(getDeleteSql(fieldsMetadata.deleteKeyFieldNames)) : null;
      final String upsertDeleteSql = record.valueSchema() != null && config.insertMode == UPSERT ? JdbcSinkConfig.ucase(getDeleteSql(fieldsMetadata.upsertKeyFieldNames)) : null;

      log.debug(
          "{} sql: {} deleteSql: {} upsertDeleteSql: {} meta: {}",
          config.insertMode,
          insertSql,
          deleteSql,
          upsertDeleteSql,
          fieldsMetadata
      );
      close();

      if (/*config.deleteEnabled && */nonNull(insertSql)) {
        updatePreparedStatement = dbDialect.createPreparedStatement(connection, insertSql);
        if (config.insertMode == UPSERT) {
          updateStatementBinder = dbDialect.statementBinder(
                  updatePreparedStatement,
                  schemaPair,
                  fieldsMetadata,
                  dbStructure.tableDefinition(connection, tableId),
                  INSERT,
                  config.coordinatesEnabled
          );
        } else {
          updateStatementBinder = dbDialect.statementBinder(
                  updatePreparedStatement,
                  schemaPair,
                  fieldsMetadata,
                  dbStructure.tableDefinition(connection, tableId),
                  config.insertMode,
                  config.coordinatesEnabled
          );
        }
      }

      if (/*config.deleteEnabled && */nonNull(deleteSql)) {
        deletePreparedStatement = dbDialect.createPreparedStatement(connection, deleteSql);
        deleteStatementBinder = dbDialect.statementBinder(
                deletePreparedStatement,
                schemaPair,
                fieldsMetadata,
                dbStructure.tableDefinition(connection, tableId),
                config.insertMode,
                config.coordinatesEnabled
        );
      }
      if (/*config.deleteEnabled && */nonNull(upsertDeleteSql)) {
        upsertDeletePreparedStatement = dbDialect.createPreparedStatement(connection, upsertDeleteSql);
        upsertDeleteStatementBinder = dbDialect.statementBinder(
                upsertDeletePreparedStatement,
                schemaPair,
                fieldsMetadata,
                dbStructure.tableDefinition(connection, tableId),
                config.insertMode,
                config.coordinatesEnabled
        );
      }
    }
    
    // set deletesInBatch if schema value is not null
    if (isNull(record.value()) && config.deleteEnabled) {
      deletesInBatch = true;
    } else if (!record.headers().isEmpty() && record.headers().allWithName("UPSERTDELETE").hasNext()) {
      upsertDeletesInBatch = true;
      updatesInBatch = true;
      keys.add(config.upsertKeys.stream().map(uk -> ((Struct) record.value()).get(uk)).collect(Collectors.toList()));
    } else if (nonNull(record.value())) {
      updatesInBatch = true;
    }

    records.add(record);

    if (records.size() >= config.batchSize) {
      flushed.addAll(flush());
    }
    return flushed;
  }

  public List<SinkRecord> flush() throws SQLException {
    if (records.isEmpty()) {
      log.debug("Records is empty");
      return new ArrayList<>();
    }
    log.debug("Flushing {} buffered records", records.size());
    for (SinkRecord r : records) {
      if (isNull(r.value())) {
        if (nonNull(deleteStatementBinder)) {
          deleteStatementBinder.bindRecord(r);
        }
      } else {
        if (r.headers().allWithName("UPSERTDELETE").hasNext()) {
          if (nonNull(upsertDeleteStatementBinder)) {
            upsertDeleteStatementBinder.bindRecord(r);
            r.headers().clear();
            updateStatementBinder.bindRecord(r);
          }
        } else {
          updateStatementBinder.bindRecord(r);
        }
      }
    }
      Optional<Long> totalUpdateCount = executeUpdates();
      long totalDeleteCount = executeDeletes();

      final long expectedCount = updateRecordCount();
      log.debug("{} records:{} resulting in totalUpdateCount:{} totalDeleteCount:{}",
              config.insertMode, records.size(), totalUpdateCount, totalDeleteCount
      );
      if (totalUpdateCount.filter(total -> total != expectedCount).isPresent()
              && config.insertMode == INSERT) {
        throw new ConnectException(String.format(
                "Update count (%d) did not sum up to total number of records inserted (%d)",
                totalUpdateCount.get(),
                expectedCount
        ));
      }
      if (!totalUpdateCount.isPresent()) {
        log.debug(
                "{} records:{} , but no count of the number of rows it affected is available",
                config.insertMode,
                records.size()
        );
      }

    final List<SinkRecord> flushedRecords = records;
    records = new ArrayList<>();
    deletesInBatch = false;
    updatesInBatch = false;
    upsertDeletesInBatch = false;
    keys.clear();
    return flushedRecords;
  }

  /**
   * @return an optional count of all updated rows or an empty optional if no info is available
   */
  private Optional<Long> executeUpdates() throws SQLException {
    Optional<Long> count = Optional.empty();

    if (config.insertMode == UPSERT) {
      if (upsertDeletesInBatch) {
        executeUpsertDeletes();
      }
    }
    if (updatesInBatch) {
      for (int updateCount : updatePreparedStatement.executeBatch()) {
        if (updateCount != Statement.SUCCESS_NO_INFO) {
          count = count.isPresent()
                  ? count.map(total -> total + updateCount)
                  : Optional.of((long) updateCount);
        }
      }
    }
    return count;
  }

  private long executeUpsertDeletes() throws SQLException {
    long totalDeleteCount = 0;
    if (nonNull(upsertDeletePreparedStatement) && upsertDeletesInBatch) {
      for (int updateCount : upsertDeletePreparedStatement.executeBatch()) {
        if (updateCount != Statement.SUCCESS_NO_INFO) {
          totalDeleteCount += updateCount;
        }
      }
    }
    return totalDeleteCount;
  }

  private long executeDeletes() throws SQLException {
    long totalDeleteCount = 0;
    if (nonNull(deletePreparedStatement) && deletesInBatch) {
      for (int updateCount : deletePreparedStatement.executeBatch()) {
        if (updateCount != Statement.SUCCESS_NO_INFO) {
          totalDeleteCount += updateCount;
        }
      }
    }
    return totalDeleteCount;
  }

  private long updateRecordCount() {
    return records
        .stream()
        // ignore deletes
        .filter(record -> nonNull(record.value()) || !config.deleteEnabled)
        .count();
  }

  public void close() throws SQLException {
    log.debug(
        "Closing BufferedRecords with updatePreparedStatement: {} deletePreparedStatement: {}",
        updatePreparedStatement,
        deletePreparedStatement
    );
    if (nonNull(updatePreparedStatement)) {
      updatePreparedStatement.close();
      updatePreparedStatement = null;
    }
    if (nonNull(deletePreparedStatement)) {
      deletePreparedStatement.close();
      deletePreparedStatement = null;
    }
  }

  private String getInsertSql() throws SQLException {

    switch (config.insertMode) {
      case INSERT:
      case UPSERT:
        return dbDialect.buildInsertStatement(
            tableId,
            asColumns(fieldsMetadata.nonKeyFieldNames),
            dbStructure.tableDefinition(connection, tableId)
        );
      case UPDATE:
        if (fieldsMetadata.upsertKeyFieldNames.isEmpty()) {
          throw new ConnectException(String.format(
                  "Write to table '%s' in UPDATE mode requires key field names to be known, check the"
                          + " configuration %s",
                  tableId,
                  JdbcSinkConfig.UPSERT_KEYS
          ));
        }
        return dbDialect.buildUpdateStatement(
            tableId,
            asColumns(fieldsMetadata.upsertKeyFieldNames),
            asColumns(fieldsMetadata.nonKeyFieldNames),
            dbStructure.tableDefinition(connection, tableId)
        );
      default:
        throw new ConnectException("Invalid insert mode");
    }
  }

  private String getDeleteSql(Set<String> keyFieldNames) {
    String sql = null;
    if (config.deleteEnabled || config.insertMode == UPSERT) {
        if (keyFieldNames.isEmpty()) {
          throw new ConnectException("Require keys to support delete");
        }
        try {
          sql = dbDialect.buildDeleteStatement(
              tableId,
              asColumns(keyFieldNames)
          );
        } catch (UnsupportedOperationException e) {
          throw new ConnectException(String.format(
              "Deletes to table '%s' are not supported with the %s dialect.",
              tableId,
              dbDialect.name()
          ));
        }
    }
    return sql;
  }

  private Collection<ColumnId> asColumns(Collection<String> names) {
    return names.stream()
        .map(name -> new ColumnId(tableId, name))
        .collect(Collectors.toList());
  }
}

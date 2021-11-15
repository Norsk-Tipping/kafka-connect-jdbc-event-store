package io.confluent.connect.jdbc.dialect;

import au.com.bytecode.opencsv.CSVWriter;
import org.postgresql.copy.CopyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import static au.com.bytecode.opencsv.CSVWriter.DEFAULT_LINE_END;

public class BulkLoadPreparedStatement implements PreparedStatement {

    private final Logger log = LoggerFactory.getLogger(BulkLoadPreparedStatement.class);
    private final List<String[]> rows;
    private String[] row;
    private final String tablename;
    private final CopyManager cm;

    public BulkLoadPreparedStatement(CopyManager cm, int rowlength, String tablename) throws IOException {
        this.cm = cm;
        this.rows = new ArrayList<>();
        this.row = new String[rowlength];
        this.tablename = tablename;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public int executeUpdate() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        row[parameterIndex - 1] = x;
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setDate(int parameterIndex, java.sql.Date x) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setTime(int parameterIndex, java.sql.Time x) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setTimestamp(int parameterIndex, java.sql.Timestamp x) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void clearParameters() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean execute() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void addBatch() throws SQLException {
        rows.add(row);
        row = new String[row.length];
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void close() throws SQLException {
        rows.clear();
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public int getMaxRows() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void cancel() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void clearWarnings() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public int getUpdateCount() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public int getFetchDirection() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public int getFetchSize() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public int getResultSetType() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void addBatch(String sql) throws SQLException {

        throw new SQLException("Not implemented");
    }

    @Override
    public void clearBatch() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public int[] executeBatch() throws SQLException {
        if (rows.size() > 0) {
            FileWriter filewriter = null;
            CSVWriter writer = null;
            InputStream is = null;
            try {
                filewriter = new FileWriter("bulkload.csv", false);
                writer = new CSVWriter(filewriter, '\t', '"', '\\', DEFAULT_LINE_END);

                try {
                    writer.writeAll(rows);
                    writer.flush();
                } catch (IOException e) {
                    throw new SQLException(String.format("Could not flush csv of rows to load into Postgres for path %s", "bulkload.csv"));
                }
                try {
                    is = new FileInputStream("bulkload.csv");
                } catch (IOException e) {
                    throw new SQLException(String.format("Could not read csv of rows to load into Postgres for path %s", "bulkload.csv"));
                }
                try {
                    long res = cm.copyIn("COPY \"" + tablename + "\" FROM STDIN WITH (FORMAT CSV, DELIMITER  E'\\t', QUOTE '\"', ESCAPE '\\', ENCODING 'UTF8') ",
                            is);
                    return new int[]{(int) res};
                } catch (IOException e) {
                    throw new SQLException(String.format("Could not read csv of rows to load into Postgres for path %s", "bulkload.csv"));
                }
            } catch (IOException e) {
                throw new SQLException(String.format("Could not create csv writer to load into Postgres for path %s", "bulkload.csv"));
            } finally {
                try {
                    rows.clear();
                    if (filewriter != null) {
                        filewriter.close();
                    }
                } catch (IOException e) {
                    log.error(String.format("Could not close filewriter for path %s", "bulkload.csv"), e);
                }
                try {
                    if (writer != null) {
                        writer.close();
                    }
                } catch (IOException e) {
                    log.error(String.format("Could not close filewriter for path %s", "bulkload.csv"), e);
                }
                try {
                    if (is != null) {
                        is.close();
                    }
                } catch (IOException e) {
                    log.error(String.format("Could not close filewriter for path %s", "bulkload.csv"), e);
                }
                try {
                    Files.deleteIfExists(Paths.get("bulkload.csv"));
                } catch (IOException e) {
                    log.error(String.format("Could not close filewriter for path %s", "bulkload.csv"), e);
                }
            }
        } else {
            return new int[]{};
        }
    }

    @Override
    public Connection getConnection() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean isClosed() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean isPoolable() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLException("Not implemented");
    }
}

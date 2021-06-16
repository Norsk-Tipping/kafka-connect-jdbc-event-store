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

import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.sink.JdbcSourceConnectorConfig;
import io.confluent.connect.jdbc.util.ColumnDefinition;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Before;
import org.junit.runners.Parameterized;
import org.mockito.Mock;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public abstract class BaseDialectTypeTest<T extends GenericDatabaseDialect> {

  public static final boolean NULLABLE = true;
  public static final boolean NOT_NULLABLE = false;

  public static final TableId TABLE_ID = new TableId(null, null, "MyTable");
  public static final ColumnId COLUMN_ID = new ColumnId(TABLE_ID, "columnA", "aliasA");

  public static final BigDecimal BIG_DECIMAL = new BigDecimal(9.9);
  public static final long LONG = Long.MAX_VALUE;
  public static final int INT = Integer.MAX_VALUE;
  public static final short SHORT = Short.MAX_VALUE;
  public static final byte BYTE = Byte.MAX_VALUE;
  public static final double DOUBLE = Double.MAX_VALUE;

  @Parameterized.Parameter(0)
  public Schema.Type expectedType;

  @Parameterized.Parameter(1)
  public Object expectedValue;

  @Parameterized.Parameter(2)
  public JdbcSourceConnectorConfig.NumericMapping numMapping;

  @Parameterized.Parameter(3)
  public boolean optional;

  @Parameterized.Parameter(4)
  public int columnType;

  @Parameterized.Parameter(5)
  public int precision;

  @Parameterized.Parameter(6)
  public int scale;

  @Mock
  ResultSet resultSet = mock(ResultSet.class);

  @Mock
  ColumnDefinition columnDefn = mock(ColumnDefinition.class);

  protected boolean signed = true;
  protected T dialect;
  protected SchemaBuilder schemaBuilder;
  protected DatabaseDialect.ColumnConverter converter;

  @Before
  public void setup() throws Exception {
    dialect = createDialect();
  }


  /**
   * Create an instance of the dialect to be tested.
   *
   * @return the dialect; may not be null
   */
  protected abstract T createDialect();

  /**
   * Create a {@link JdbcSourceConnectorConfig} with the specified URL and optional config props.
   *
   * @param url           the database URL; may not be null
   * @param propertyPairs optional set of config name-value pairs; must be an even number
   * @return the config; never null
   */
  protected JdbcSinkConfig sourceConfigWithUrl(
      String url,
      String... propertyPairs
  ) {
    Map<String, String> connProps = new HashMap<>();
    connProps.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
    connProps.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, "test-");
    connProps.putAll(propertiesFromPairs(propertyPairs));
    connProps.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, url);
    connProps.put(JdbcSourceConnectorConfig.NUMERIC_MAPPING_CONFIG, numMapping.toString());
    return new JdbcSinkConfig(connProps);
  }

  protected Map<String, String> propertiesFromPairs(String... pairs) {
    Map<String, String> props = new HashMap<>();
    assertEquals("Expecting even number of properties but found " + pairs.length, 0,
                 pairs.length % 2);
    for (int i = 0; i != pairs.length; ++i) {
      String key = pairs[i];
      String value = pairs[++i];
      props.put(key, value);
    }
    return props;
  }
}

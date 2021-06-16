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
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

public class DatabaseDialectsTest {

  @Test
  public void shouldLoadAllBuiltInDialects() {
    Collection<? extends DatabaseDialectProvider> providers = DatabaseDialects
        .registeredDialectProviders();
    assertContainsInstanceOf(providers, GenericDatabaseDialect.Provider.class);
    assertContainsInstanceOf(providers, OracleDatabaseDialect.Provider.class);
    assertContainsInstanceOf(providers, PostgreSqlDatabaseDialect.Provider.class);
    assertContainsInstanceOf(providers, NetezzaSqlDatabaseDialect.Provider.class);
    assertContainsInstanceOf(providers, MockDatabaseDialect.Provider.class);
  }

  @Test
  public void shouldFindGenericDialect() {
    assertDialect(GenericDatabaseDialect.class, "jdbc:someting:");
  }


  @Test
  public void shouldFindOracleDialect() {
    assertDialect(OracleDatabaseDialect.class, "jdbc:oracle:thin:@something");
    assertDialect(OracleDatabaseDialect.class, "jdbc:oracle:doesn'tmatter");
  }

  @Test
  public void shouldFindNetezzaSqlDialect() {
    assertDialect(NetezzaSqlDatabaseDialect.class, "jdbc:netezza://localhost/test");
  }

  @Test
  public void shouldFindPostgreSqlDialect() {
    assertDialect(PostgreSqlDatabaseDialect.class, "jdbc:postgresql://localhost/test");
  }


  @Test
  public void shouldFindMockDialect() {
    assertDialect(MockDatabaseDialect.class, "jdbc:mock:argle");
  }

  @Test(expected = ConnectException.class)
  public void shouldNotFindDialectForInvalidUrl() {
    DatabaseDialects.extractJdbcUrlInfo("jdbc:protocolinvalid;field=value;");
  }

  @Test(expected = ConnectException.class)
  public void shouldNotFindDialectForInvalidUrlMissingJdbcPrefix() {
    DatabaseDialects.extractJdbcUrlInfo("mysql://Server:port");
  }


  private void assertDialect(
      Class<? extends DatabaseDialect> clazz,
      String url
  ) {
    Map<String, String> props = new HashMap<>();
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, url);
    props.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, "prefix");
    props.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
    JdbcSinkConfig config = new JdbcSinkConfig(props);
    DatabaseDialect dialect = DatabaseDialects.findBestFor(url, config);
    assertSame(dialect.getClass(), clazz);
  }

  private void assertContainsInstanceOf(
      Collection<? extends DatabaseDialectProvider> providers,
      Class<? extends DatabaseDialectProvider> clazz
  ) {
    for (DatabaseDialectProvider provider : providers) {
      if (provider.getClass().equals(clazz))
        return;
    }
    fail("Missing " + clazz.getName());
  }

}
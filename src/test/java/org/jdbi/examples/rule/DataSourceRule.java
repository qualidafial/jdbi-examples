package org.jdbi.examples.rule;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.rules.ExternalResource;

public class DataSourceRule extends ExternalResource {
  JdbcConnectionPool dataSource;

  @Override
  protected void before() throws Throwable {
    dataSource = JdbcConnectionPool.create("jdbc:h2:mem:test", "username", "password");
  }

  @Override
  protected void after() {
    dataSource.dispose();
  }

  public DataSource getDataSource() {
    return dataSource;
  }
}

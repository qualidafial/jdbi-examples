package org.jdbi.examples.v2;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.jdbi.examples.rule.DataSourceRule;
import org.junit.Rule;
import org.junit.Test;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;

public class Example01FluentApi {
  @Rule
  public DataSourceRule ds = new DataSourceRule();

  @Test
  public void test() throws Exception {
    DBI dbi = new DBI(ds.getDataSource());

    try (Handle h = dbi.open()) {
      h.execute("create table contacts (id int primary key, name varchar(100))");

      h.execute("insert into contacts (id, name) values (?, ?)", 1, "Alice");
      h.execute("insert into contacts (id, name) values (?, ?)", 2, "Bob");

      List<String> names = h.createQuery("select name from contacts order by id")
          .mapTo(String.class)
          .list();
      assertThat(names)
          .containsExactly("Alice", "Bob");

      String name = h.createQuery("select name from contacts where id = :id")
          .bind("id", 1)
          .mapTo(String.class)
          .first();
      assertThat(name)
          .isEqualTo("Alice");

    }
  }

}

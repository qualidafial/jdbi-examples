package org.jdbi.examples.v3;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.jdbi.examples.rule.DataSourceRule;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.junit.Rule;
import org.junit.Test;

public class Example01FluentApi {
  @Rule
  public DataSourceRule ds = new DataSourceRule();

  @Test
  public void test() throws Exception {
    Jdbi jdbi = Jdbi.create(ds.getDataSource());

    try (Handle h = jdbi.open()) {
      h.execute("create table something (id int primary key, name varchar(100))");

      h.execute("insert into something (id, name) values (?, ?)", 1, "Alice");
      h.execute("insert into something (id, name) values (?, ?)", 2, "Bob");

      List<String> names = h.createQuery("select name from something order by id")
          .mapTo(String.class)
          .list();
      assertThat(names)
          .containsExactly("Alice", "Bob");

      String name = h.createQuery("select name from something where id = :id")
          .bind("id", 1)
          .mapTo(String.class)
          .findOnly();
      assertThat(name)
          .isEqualTo("Alice");

    }
  }

}

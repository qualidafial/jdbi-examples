package org.jdbi.examples.v3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.jdbi.examples.rule.DataSourceRule;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.StatementContext;
import org.jdbi.v3.core.mapper.RowMapper;
import org.junit.Rule;
import org.junit.Test;

public class Example02RowMapper {
  @Rule
  public DataSourceRule ds = new DataSourceRule();

  @Test
  public void test() throws Exception {
    Jdbi jdbi = Jdbi.create(ds.getDataSource());

    try (Handle h = jdbi.open()) {
      h.execute("create table something (id int primary key, name varchar(100))");

      h.execute("insert into something (id, name) values (?, ?)", 1, "Alice");
      h.execute("insert into something (id, name) values (?, ?)", 2, "Bob");

      List<Something> list = h.createQuery("select * from something order by id")
          .map(new SomethingMapper())
          .list();
      assertThat(list)
          .extracting(Something::getId, Something::getName)
          .containsExactly(tuple(1, "Alice"),
                           tuple(2, "Bob"));

      Something bob = h.createQuery("select * from something where id = :id")
          .bind("id", 2)
          .map(new SomethingMapper())
          .findOnly();
      assertThat(bob)
          .extracting(Something::getId, Something::getName)
          .containsExactly(2, "Bob");
    }
  }

  public static class Something {
    private final int id;
    private final String name;

    public Something(int id, String name) {
      this.id = id;
      this.name = name;
    }

    public int getId() {
      return id;
    }

    public String getName() {
      return name;
    }
  }

  public static class SomethingMapper implements RowMapper<Something> {
    @Override
    public Something map(ResultSet r, StatementContext ctx) throws SQLException {
      int id = r.getInt("id");
      String name = r.getString("name");
      return new Something(id, name);
    }
  }

}

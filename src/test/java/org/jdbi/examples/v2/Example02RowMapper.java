package org.jdbi.examples.v2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.jdbi.examples.rule.DataSourceRule;
import org.junit.Rule;
import org.junit.Test;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

public class Example02RowMapper {
  @Rule
  public DataSourceRule ds = new DataSourceRule();

  @Test
  public void test() throws Exception {
    DBI dbi = new DBI(ds.getDataSource());

    try (Handle h = dbi.open()) {
      h.execute("create table contacts (id int primary key, name varchar(100))");

      h.execute("insert into contacts (id, name) values (?, ?)", 1, "Alice");
      h.execute("insert into contacts (id, name) values (?, ?)", 2, "Bob");

      List<Contact> list = h.createQuery("select * from contacts order by id")
          .map(new ContactMapper())
          .list();
      assertThat(list)
          .extracting(Contact::getId, Contact::getName)
          .containsExactly(tuple(1, "Alice"),
                           tuple(2, "Bob"));

      Contact bob = h.createQuery("select * from contacts where id = :id")
          .bind("id", 2)
          .map(new ContactMapper())
          .first();
      assertThat(bob)
          .extracting(Contact::getId, Contact::getName)
          .containsExactly(2, "Bob");
    }
  }

  public static class Contact {
    private final int id;
    private final String name;

    public Contact(int id, String name) {
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

  public static class ContactMapper implements ResultSetMapper<Contact> {
    @Override
    public Contact map(int index, ResultSet r, StatementContext ctx) throws SQLException {
      int id = r.getInt("id");
      String name = r.getString("name");
      return new Contact(id, name);
    }
  }

}

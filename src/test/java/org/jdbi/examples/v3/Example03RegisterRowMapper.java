package org.jdbi.examples.v3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.jdbi.examples.rule.DataSourceRule;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;
import org.junit.Rule;
import org.junit.Test;

public class Example03RegisterRowMapper {
  @Rule
  public DataSourceRule ds = new DataSourceRule();

  @Test
  public void test() throws Exception {
    Jdbi jdbi = Jdbi.create(ds.getDataSource());
    jdbi.registerRowMapper(new ContactMapper());

    try (Handle h = jdbi.open()) {
      h.execute("create table contacts (id int primary key, name varchar(100))");

      h.execute("insert into contacts (id, name) values (?, ?)", 1, "Alice");
      h.execute("insert into contacts (id, name) values (?, ?)", 2, "Bob");

      List<Contact> list = h.createQuery("select * from contacts order by id")
          .mapTo(Contact.class)
          .list();
      assertThat(list)
          .extracting(Contact::getId, Contact::getName)
          .containsExactly(tuple(1, "Alice"),
                           tuple(2, "Bob"));

      Contact bob = h.createQuery("select * from contacts where id = :id")
          .bind("id", 2)
          .mapTo(Contact.class)
          .findOnly();
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

  public static class ContactMapper implements RowMapper<Contact> {
    @Override
    public Contact map(ResultSet r, StatementContext ctx) throws SQLException {
      int id = r.getInt("id");
      String name = r.getString("name");
      return new Contact(id, name);
    }
  }

}

package org.jdbi.examples.v2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.joda.money.CurrencyUnit.USD;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.jdbi.examples.rule.DataSourceRule;
import org.joda.money.Money;
import org.junit.Rule;
import org.junit.Test;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.Argument;
import org.skife.jdbi.v2.tweak.ArgumentFactory;
import org.skife.jdbi.v2.tweak.BeanMapperFactory;
import org.skife.jdbi.v2.tweak.ResultColumnMapper;

public class Example04ColumnMapper {
  @Rule
  public DataSourceRule ds = new DataSourceRule();

  @Test
  public void test() throws Exception {
    DBI dbi = new DBI(ds.getDataSource());
    dbi.registerMapper(new BeanMapperFactory());
    dbi.registerColumnMapper(new MoneyMapper());
    dbi.registerArgumentFactory(new MoneyArgumentFactory());

    try (Handle h = dbi.open()) {
      Money tenDollars = Money.of(USD, 10);
      Money fiveDollars = Money.of(USD, 5);

      h.execute("create table something (id int primary key, name varchar(100), amount decimal)");

      h.execute("insert into something (id, name, amount) values (?, ?, ?)", 1, "Alice", tenDollars);
      h.execute("insert into something (id, name, amount) values (?, ?, ?)", 2, "Bob", fiveDollars);

      List<Something> list = h.createQuery("select * from something order by id")
          .mapTo(Something.class)
          .list();
      assertThat(list)
          .extracting(Something::getId, Something::getName, Something::getAmount)
          .containsExactly(tuple(1, "Alice", tenDollars),
                           tuple(2, "Bob", fiveDollars));

      Something bob = h.createQuery("select * from something where id = :id")
          .bind("id", 2)
          .mapTo(Something.class)
          .first();
      assertThat(bob)
          .extracting(Something::getId, Something::getName, Something::getAmount)
          .containsExactly(2, "Bob", fiveDollars);
    }
  }

  public static class Something {
    private int id;
    private String name;
    private Money amount;

    public int getId() {
      return id;
    }

    public void setId(int id) {
      this.id = id;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public Money getAmount() {
      return amount;
    }

    public void setAmount(Money amount) {
      this.amount = amount;
    }
  }

  public static class MoneyArgumentFactory implements ArgumentFactory<Money> {
    @Override
    public boolean accepts(Class<?> expectedType, Object value, StatementContext ctx) {
      return Money.class.equals(expectedType);
    }

    @Override
    public Argument build(Class<?> expectedType, Money value, StatementContext ctx) {
      return (pos, stmt, context) -> stmt.setBigDecimal(pos, value.getAmount());
    }
  }

  public static class MoneyMapper implements ResultColumnMapper<Money> {
    @Override
    public Money mapColumn(ResultSet r, int columnNumber, StatementContext ctx) throws SQLException {
      return Money.of(USD, r.getBigDecimal(columnNumber));
    }

    @Override
    public Money mapColumn(ResultSet r, String columnLabel, StatementContext ctx) throws SQLException {
      return Money.of(USD, r.getBigDecimal(columnLabel));
    }
  }
}

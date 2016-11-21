package org.jdbi.examples.v3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.joda.money.CurrencyUnit.USD;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

import org.jdbi.examples.rule.DataSourceRule;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.StatementContext;
import org.jdbi.v3.core.argument.Argument;
import org.jdbi.v3.core.argument.ArgumentFactory;
import org.jdbi.v3.core.mapper.BeanMapper;
import org.jdbi.v3.core.mapper.ColumnMapper;
import org.joda.money.Money;
import org.junit.Rule;
import org.junit.Test;

public class Example04ColumnMapper {
  @Rule
  public DataSourceRule ds = new DataSourceRule();

  @Test
  public void test() throws Exception {
    Jdbi jdbi = Jdbi.create(ds.getDataSource());
    jdbi.registerRowMapper(BeanMapper.of(Something.class));
    jdbi.registerColumnMapper(new MoneyMapper());
    jdbi.registerArgumentFactory(new MoneyArgumentFactory());

    try (Handle h = jdbi.open()) {
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
          .findOnly();
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

  public static class MoneyArgumentFactory implements ArgumentFactory {
    @Override
    public Optional<Argument> build(Type type, Object value, StatementContext ctx) {
      if (Money.class.equals(type)) {
        BigDecimal amount = value == null ? null : ((Money) value).getAmount();
        return Optional.of((pos, stmt, context) -> stmt.setBigDecimal(pos, amount));
      }
      return Optional.empty();
    }
  }

  public static class MoneyMapper implements ColumnMapper<Money> {
    @Override
    public Money map(ResultSet r, int columnNumber, StatementContext ctx) throws SQLException {
      return Money.of(USD, r.getBigDecimal(columnNumber));
    }
  }
}

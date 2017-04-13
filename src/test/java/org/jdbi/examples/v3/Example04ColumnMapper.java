package org.jdbi.examples.v3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.joda.money.CurrencyUnit.USD;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

import org.jdbi.examples.rule.DataSourceRule;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.argument.AbstractArgumentFactory;
import org.jdbi.v3.core.argument.Argument;
import org.jdbi.v3.core.config.ConfigRegistry;
import org.jdbi.v3.core.mapper.ColumnMapper;
import org.jdbi.v3.core.mapper.reflect.BeanMapper;
import org.jdbi.v3.core.statement.StatementContext;
import org.joda.money.Money;
import org.junit.Rule;
import org.junit.Test;

public class Example04ColumnMapper {
  @Rule
  public DataSourceRule ds = new DataSourceRule();

  @Test
  public void test() throws Exception {
    Jdbi jdbi = Jdbi.create(ds.getDataSource());
    jdbi.registerRowMapper(BeanMapper.factory(Account.class));
    jdbi.registerColumnMapper(new MoneyMapper());
    jdbi.registerArgument(new MoneyArgumentFactory());

    try (Handle h = jdbi.open()) {
      Money tenDollars = Money.of(USD, 10);
      Money fiveDollars = Money.of(USD, 5);

      h.execute("create table accounts (id int primary key, name varchar(100), balance decimal)");

      h.execute("insert into accounts (id, name, balance) values (?, ?, ?)", 1, "Alice", tenDollars);
      h.execute("insert into accounts (id, name, balance) values (?, ?, ?)", 2, "Bob", fiveDollars);

      List<Account> list = h.createQuery("select * from accounts order by id")
          .mapTo(Account.class)
          .list();
      assertThat(list)
          .extracting(Account::getId, Account::getName, Account::getBalance)
          .containsExactly(tuple(1, "Alice", tenDollars),
                           tuple(2, "Bob", fiveDollars));

      Account bob = h.createQuery("select * from accounts where id = :id")
          .bind("id", 2)
          .mapTo(Account.class)
          .findOnly();
      assertThat(bob)
          .extracting(Account::getId, Account::getName, Account::getBalance)
          .containsExactly(2, "Bob", fiveDollars);
    }
  }

  public static class Account {
    private int id;
    private String name;
    private Money balance;

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

    public Money getBalance() {
      return balance;
    }

    public void setBalance(Money balance) {
      this.balance = balance;
    }
  }

  public static class MoneyArgumentFactory extends AbstractArgumentFactory<Money> {
    protected MoneyArgumentFactory() {
      super(Types.NUMERIC);
    }

    @Override
    protected Argument build(Money value, ConfigRegistry config) {
      BigDecimal amount = value == null ? null : value.getAmount();
      return (pos, stmt, context) -> stmt.setBigDecimal(pos, amount);
    }
  }

  public static class MoneyMapper implements ColumnMapper<Money> {
    @Override
    public Money map(ResultSet r, int columnNumber, StatementContext ctx) throws SQLException {
      return Money.of(USD, r.getBigDecimal(columnNumber));
    }
  }
}

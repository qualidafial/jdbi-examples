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
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.BindBean;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.customizers.RegisterArgumentFactory;
import org.skife.jdbi.v2.sqlobject.customizers.RegisterColumnMapper;
import org.skife.jdbi.v2.sqlobject.customizers.RegisterMapperFactory;
import org.skife.jdbi.v2.tweak.Argument;
import org.skife.jdbi.v2.tweak.ArgumentFactory;
import org.skife.jdbi.v2.tweak.BeanMapperFactory;
import org.skife.jdbi.v2.tweak.ResultColumnMapper;

public class Example05SqlObjectApi {
  @Rule
  public DataSourceRule ds = new DataSourceRule();

  @RegisterMapperFactory(BeanMapperFactory.class)
  @RegisterColumnMapper(MoneyMapper.class)
  @RegisterArgumentFactory(MoneyArgumentFactory.class)
  public interface AccountDao extends AutoCloseable {
    @SqlUpdate("create table accounts (id int primary key, name varchar(100), balance decimal)")
    void createTable();

    @SqlUpdate("insert into accounts (id, name, balance) values (:id, :name, :balance)")
    void insert(@BindBean Account account);

    @SqlUpdate("update accounts set name = :name, balance = :balance where id = :id")
    void update(@BindBean Account account);

    @SqlQuery("select * from accounts order by id")
    List<Account> list();

    @SqlQuery("select * from accounts where id = :id")
    Account getById(@Bind("id") int id);
  }

  @Test
  public void test() throws Exception {
    DBI dbi = new DBI(ds.getDataSource());

    try (AccountDao dao = dbi.open(AccountDao.class)) {
      Money tenDollars = Money.of(USD, 10);
      Money fiveDollars = Money.of(USD, 5);

      dao.createTable();
      dao.insert(new Account(1, "Alice", tenDollars));
      dao.insert(new Account(2, "Bob", fiveDollars));

      assertThat(dao.list())
          .extracting(Account::getId, Account::getName, Account::getBalance)
          .containsExactly(tuple(1, "Alice", tenDollars),
                           tuple(2, "Bob", fiveDollars));

      assertThat(dao.getById(2))
          .extracting(Account::getId, Account::getName, Account::getBalance)
          .containsExactly(2, "Bob", fiveDollars);

      dao.update(new Account(2, "Robert", tenDollars));

      assertThat(dao.getById(2))
          .extracting(Account::getId, Account::getName, Account::getBalance)
          .containsExactly(2, "Robert", tenDollars);
    }
  }

  public static class Account {
    private int id;
    private String name;
    private Money balance;

    public Account() {
    }

    public Account(int id, String name, Money balance) {
      this.id = id;
      this.name = name;
      this.balance = balance;
    }

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

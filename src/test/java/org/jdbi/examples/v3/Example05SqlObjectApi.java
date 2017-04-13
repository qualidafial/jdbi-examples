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
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.argument.AbstractArgumentFactory;
import org.jdbi.v3.core.argument.Argument;
import org.jdbi.v3.core.config.ConfigRegistry;
import org.jdbi.v3.core.mapper.ColumnMapper;
import org.jdbi.v3.core.statement.StatementContext;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.jdbi.v3.sqlobject.config.RegisterArgumentFactory;
import org.jdbi.v3.sqlobject.config.RegisterBeanMapper;
import org.jdbi.v3.sqlobject.config.RegisterColumnMapper;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.joda.money.Money;
import org.junit.Rule;
import org.junit.Test;

public class Example05SqlObjectApi {
  @Rule
  public DataSourceRule ds = new DataSourceRule();

  @RegisterBeanMapper(Account.class)
  @RegisterColumnMapper(MoneyMapper.class)
  @RegisterArgumentFactory(MoneyArgumentFactory.class)
  public interface AccountDao {
    @SqlUpdate("create table accounts (id int primary key, name varchar(100), balance decimal)")
    void createTable();

    @SqlUpdate("insert into accounts (id, name, balance) values (:id, :name, :balance)")
    void insert(@BindBean Account accounts);

    @SqlUpdate("update accounts set name = :name, balance = :balance where id = :id")
    void update(@BindBean Account accounts);

    @SqlQuery("select * from accounts order by id")
    List<Account> list();

    @SqlQuery("select * from accounts where id = :id")
    Account getById(int id);
  }

  @Test
  public void test() throws Exception {
    Jdbi jdbi = Jdbi.create(ds.getDataSource());
    jdbi.installPlugin(new SqlObjectPlugin());

    jdbi.useExtension(AccountDao.class, dao -> {
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
    });
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

  public static class MoneyArgumentFactory extends AbstractArgumentFactory<Money> {
    public MoneyArgumentFactory() {
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

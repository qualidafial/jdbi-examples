package org.jdbi.examples.v3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.jdbi.examples.v3.Example06Joins.PhoneType.MOBILE;
import static org.jdbi.examples.v3.Example06Joins.PhoneType.WORK;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.jdbi.examples.rule.DataSourceRule;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.mapper.ConstructorMapper;
import org.jdbi.v3.sqlobject.BindBean;
import org.jdbi.v3.sqlobject.SqlBatch;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.jdbi.v3.sqlobject.SqlUpdate;
import org.jdbi.v3.sqlobject.mixins.GetHandle;
import org.junit.Rule;
import org.junit.Test;

public class Example06Joins {
  @Rule
  public DataSourceRule ds = new DataSourceRule();

  public interface ContactDao extends GetHandle {
    @SqlUpdate("create table contact (id int primary key, name varchar(100))")
    void createContactTable();

    @SqlUpdate("create table phone ("
        + "id int primary key, "
        + "contactId int, "
        + "foreign key (contactId) references contact(id) on delete cascade, "
        + "type varchar(20), "
        + "phone varchar(20))")
    void createPhoneTable();

    @SqlUpdate("insert into contact (id, name) values (:id, :name)")
    void insertContact(@BindBean Contact contact);

    @SqlBatch("INSERT INTO phone (id, contactId, type, phone) VALUES (:id, :contactId, :type, :phone)")
    void insertPhones(@BindBean List<Phone> phones, int contactId);

    default void insertFullContact(Contact contact) {
      insertContact(contact);
      insertPhones(contact.getPhones(), contact.getId());
    }

    default Contact getFullContactById(int id) {
      return getHandle().createQuery("select contact.id c_id, name c_name, "
                                         + "phone.id p_id, type p_type, phone.phone p_phone "
                                         + "from contact left join phone on contact.id = phone.contactId "
                                         + "where contact.id = :id")
          .bind("id", id)
          .registerRowMapper(ConstructorMapper.of(Contact.class, "c_"))
          .registerRowMapper(ConstructorMapper.of(Phone.class, "p_"))
          .reduceRows(null, (contact, rowView) -> {
            if (contact == null) {
              contact = rowView.getRow(Contact.class);
            }

            if (rowView.getColumn("p_id", Integer.class) != null) {
              contact.addPhone(rowView.getRow(Phone.class));
            }

            return contact;
          });
    }
  }

  @Test
  public void test() throws Exception {
    Jdbi jdbi = Jdbi.create(ds.getDataSource());
    jdbi.installPlugin(new SqlObjectPlugin());

    jdbi.useExtension(ContactDao.class, dao -> {
      dao.createContactTable();
      dao.createPhoneTable();

      dao.insertFullContact(Contact.create(1, "Alice",
                                           new Phone(2, WORK, "800-555-1234"),
                                           new Phone(3, MOBILE, "801-555-1212")));

      dao.insertFullContact(Contact.create(4, "Bob"));

      Contact alice = dao.getFullContactById(1);
      assertThat(alice)
          .extracting(Contact::getId, Contact::getName)
          .containsExactly(1, "Alice");
      assertThat(alice.getPhones())
          .extracting(Phone::getId, Phone::getType, Phone::getPhone)
          .containsExactly(tuple(2, WORK, "800-555-1234"),
                           tuple(3, MOBILE, "801-555-1212"));

      Contact bob = dao.getFullContactById(4);
      assertThat(bob)
          .extracting(Contact::getId, Contact::getName)
          .containsExactly(4, "Bob");
      assertThat(bob.getPhones())
          .isEmpty();
    });
  }

  public static class Contact {
    public static Contact create(int id, String name, Phone... phones) {
      Contact contact = new Contact(id, name);
      contact.phones.addAll(Arrays.asList(phones));
      return contact;
    }

    private final int id;
    private final String name;
    private final List<Phone> phones;

    public Contact(int id, String name) {
      this.id = id;
      this.name = name;
      this.phones = new ArrayList<>();
    }

    public int getId() {
      return id;
    }

    public String getName() {
      return name;
    }

    public List<Phone> getPhones() {
      return phones;
    }

    public void addPhone(Phone phone) {
      phones.add(phone);
    }
  }

  public static class Phone {
    private final int id;
    private final PhoneType type;
    private final String phone;

    public Phone(int id, PhoneType type, String phone) {
      this.id = id;
      this.type = type;
      this.phone = phone;
    }

    public int getId() {
      return id;
    }

    public PhoneType getType() {
      return type;
    }

    public String getPhone() {
      return phone;
    }
  }

  public enum PhoneType {
    WORK, MOBILE, HOME
  }
}
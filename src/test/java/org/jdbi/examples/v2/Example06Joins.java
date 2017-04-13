package org.jdbi.examples.v2;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.jdbi.examples.v2.Example06Joins.PhoneType.MOBILE;
import static org.jdbi.examples.v2.Example06Joins.PhoneType.WORK;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

import org.jdbi.examples.rule.DataSourceRule;
import org.junit.Rule;
import org.junit.Test;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.BindBean;
import org.skife.jdbi.v2.sqlobject.SqlBatch;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.mixins.GetHandle;

public class Example06Joins {
  @Rule
  public DataSourceRule ds = new DataSourceRule();

  public interface ContactDao extends GetHandle, AutoCloseable {
    @SqlUpdate("create table contacts (id int primary key, name varchar(100))")
    void createContactTable();

    @SqlUpdate("create table phones ("
        + "id int primary key, "
        + "contactId int, "
        + "foreign key (contactId) references contacts(id) on delete cascade, "
        + "type varchar(20), "
        + "phone varchar(20))")
    void createPhoneTable();

    @SqlUpdate("insert into contacts (id, name) values (:id, :name)")
    void insertContact(@BindBean Contact contact);

    @SqlBatch("INSERT INTO phones (id, contactId, type, phone) VALUES (:id, :contactId, :type, :phone)")
    void insertPhones(@BindBean List<Phone> phones, @Bind("contactId") int contactId);

    default void insertFullContact(Contact contact) {
      insertContact(contact);
      insertPhones(contact.getPhones(), contact.getId());
    }

    default Contact getFullContactById(int id) {
      return getHandle().createQuery("select c.id c_id, c.name c_name, " 
                                         + "p.id p_id, p.type p_type, p.phone p_phone "
                                         + "from contacts c left join phones p on c.id = p.contactId "
                                         + "where c.id = :id")
          .bind("id", id)
          .fold(null, (contact, rs, ctx) -> {
            if (contact == null) {
              contact = new Contact(rs.getInt("c_id"),
                                    rs.getString("c_name"));
            }

            int phoneId = rs.getInt("p_id");
            if (!rs.wasNull()) {
              contact.addPhone(new Phone(phoneId,
                                         PhoneType.valueOf(rs.getString("p_type")),
                                         rs.getString("p_phone")));
            }

            return contact;
          });
    }

    default List<Contact> listFullContacts() {
      return getHandle().createQuery("select c.id c_id, c.name c_name, "
                                         + "p.id p_id, p.type p_type, p.phone p_phone "
                                         + "from contacts c left join phones p on c.id = p.contactId "
                                         + "order by c.name")
          .fold(new LinkedHashMap<Integer, Contact>(), (map, rs, ctx) -> {
            int contactId = rs.getInt("c_id");
            Contact contact = map.get(contactId);
            if (contact == null) {
              contact = new Contact(contactId,
                                    rs.getString("c_name"));
              map.put(contactId, contact);
            }

            int phoneId = rs.getInt("p_id");
            if (!rs.wasNull()) {
              contact.addPhone(new Phone(phoneId,
                                         PhoneType.valueOf(rs.getString("p_type")),
                                         rs.getString("p_phone")));
            }

            return map;
          })
          .values()
          .stream()
          .collect(toList());
    }
  }

  @Test
  public void test() throws Exception {
    DBI dbi = new DBI(ds.getDataSource());

    try (ContactDao dao = dbi.open(ContactDao.class)) {
      dao.createContactTable();
      dao.createPhoneTable();

      dao.insertFullContact(new Contact(1, "Alice",
                                        new Phone(2, WORK, "800-555-1234"),
                                        new Phone(3, MOBILE, "801-555-1212")));

      dao.insertFullContact(new Contact(4, "Bob"));

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

      List<Contact> fullContacts = dao.listFullContacts();
      assertThat(fullContacts)
          .extracting(Contact::getId, Contact::getName)
          .containsExactly(tuple(1, "Alice"),
                           tuple(4, "Bob"));
      assertThat(fullContacts.get(0).getPhones())
          .extracting(Phone::getId, Phone::getType, Phone::getPhone)
          .containsExactly(tuple(2, WORK, "800-555-1234"),
                           tuple(3, MOBILE, "801-555-1212"));
      assertThat(fullContacts.get(1).getPhones())
          .isEmpty();
    }
  }

  public static class Contact {
    private final int id;
    private final String name;
    private final List<Phone> phones;

    public Contact(int id, String name, Phone... phones) {
      this.id = id;
      this.name = name;
      this.phones = new ArrayList<>(Arrays.asList(phones));
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

package com.dentinger.sample;

import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.dentinger.config.SpringConfig;
import com.dentinger.dao.ds.LinksDao;
import com.dentinger.dao.ds.LinksDaoImpl;
import com.dentinger.dao.ds.UserDao;
import com.dentinger.domain.Links;
import com.dentinger.domain.User;
import com.dentinger.ds.util.CassandraSessionFactory;

/**
 * Created by dan on 8/20/14.
 */
public class LinksCassandra {
	public static void main(final String[] args) {
		final AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
		ctx.register(SpringConfig.class);
		ctx.refresh();
		final LinksCassandra linksCassandra = new LinksCassandra();
		final CassandraSessionFactory cassandraSessionFactory = ctx.getBean(CassandraSessionFactory.class);

		final UserDao userDao = ctx.getBean(UserDao.class);
		final LinksDao linksDao = ctx.getBean(LinksDaoImpl.class);

		linksCassandra.serviceUsers(linksDao, userDao);

		cassandraSessionFactory.shutdown();
	}

	public void addLink(final LinksDao dao, final Links links) {
		dao.updateLink(links);
	}

	public void addUser(final UserDao dao) {
		User u = new User();

		u.setPassword("javaAlso");
		u.setDescription("DriverCreated");
		u.setEmail("java@m.m");
		u.setUsername("java_user");
		u.setFirstname("Java");
		u.setLastname("CafeBabe");
		dao.addUser(u);
		u = new User();
		u.setFirstname("Harry");
		u.setLastname("Dresdin");
		u.setPassword("javaAlso");
		u.setDescription("Super cool guy");
		u.setEmail("Dan@cool.c");
		u.setUsername("Dan");
		dao.addUser(u);
	}

	public void deleteUser(final UserDao dao) {
		dao.deleteUser("java");
	}

	public Links getLinks(final LinksDao dao, final String id) {
		return dao.getLinks(id);
	}

	public void getUsers(final UserDao dao) {
		final List<User> users = dao.getAllUsers();
		System.out
		.println("----------------------------------------------------------------------------------------------------------------------------------------------------------------");
		System.out.printf("%15s%20s%20s%20s%15s%12s%40s%30s%30s\n", "UserName", "firstname", "lastname", "password",
				"description", "email", "created_date", "modified_date");
		System.out
		.println("----------------------------------------------------------------------------------------------------------------------------------------------------------------");
		for (final User user : users) {
			System.out.printf("%15s%20s%15s%12s%40s%30s%30s\n", user.getUsername(), user.getPassword(),
					user.getFirstname(), user.getLastname(), user.getDescription(), user.getEmail(),
					user.getCreatedDate(), user.getModified_date());
		}
	}

	public void listUsers(final CassandraSessionFactory cassandraSessionFactory) {
		final Session session = cassandraSessionFactory.getSession();
		final ResultSet resultSet = session.execute("SELECT * FROM LINKED_OUT.USERS;");
		final List<Row> allRows = resultSet.all();
		System.out
		.println("----------------------------------------------------------------------------------------------------------------------------------------------------------------");
		System.out.printf("%15s%20s%20s%20s%15s%12s%40s%30s%30s\n", "UserName", "firstname", "lastname", "password",
				"description", "email", "created_date", "modified_date");
		System.out
		.println("----------------------------------------------------------------------------------------------------------------------------------------------------------------");
		for (final Row row : allRows) {

			final String username = row.getString("username");
			final String password = row.getString("password");
			final String description = row.getString("description");
			final String email = row.getString("email");
			final Date created_date = row.getDate("created_date");
			final Date modified_date = row.getDate("modified_date");
			final String ln = row.getString("lastname");
			final String fn = row.getString("firstname");
			System.out.printf("%15s%20s%20s%20s%15s%12s%40s%30s%30s\n", username, password, fn, ln, description, email,
					created_date, modified_date);
		}

	}

	public void serviceUsers(final LinksDao linkDao, final UserDao userDao) {
		addUser(userDao);
		getUsers(userDao);
		validateLinks(linkDao, "1");
		final Set<String> ins = new HashSet<String>();
		ins.add("daa");
		final Links links = new Links();
		links.setIns(ins);
		links.setId("1");
		addLink(linkDao, links);
		validateLinks(linkDao, "1");
	}

	private void validateLinks(final LinksDao linkDao, final String id) {
		final Links one = getLinks(linkDao, id);
		if (one.getIns() == null) {
			System.out.print("ID: " + id + " -- Not linked In");
		} else {
			System.out.print("ID: " + id + " -- Linked to " + one.getIns());
		}
		if (one.getOuts() == null) {
			System.out.print(" and seems to like everyone also...");
		}
		System.out.print("\n");
	}
}

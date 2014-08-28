package com.dentinger.sample;

import java.util.List;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.dentinger.config.SpringConfig;
import com.dentinger.dao.ds.UserDao;
import com.dentinger.domain.User;
import com.dentinger.ds.util.CassandraSessionFactory;

/**
 * Created by dan on 8/20/14.
 */
public class SampleCassandra {
	public static void main(final String[] args) {
		final AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
		ctx.register(SpringConfig.class);
		ctx.refresh();
		final SampleCassandra sampleCassandra = new SampleCassandra();
		final CassandraSessionFactory cassandraSessionFactory = ctx.getBean(CassandraSessionFactory.class);

		final UserDao userDao = ctx.getBean(UserDao.class);
		sampleCassandra.listHosts(cassandraSessionFactory);

		sampleCassandra.addUser(userDao);
		System.out.println("Addded a user");
		sampleCassandra.getUsers(userDao);
		sampleCassandra.deleteUser(userDao);
		System.out.println("Deleted a user");
		sampleCassandra.getUsers(userDao);
		// sampleCassandra.listUsers(cassandraSessionFactory);
		cassandraSessionFactory.shutdown();
	}

	public void addUser(final UserDao dao) {
		final User u = new User();

		u.setPassword("javaAlso");
		u.setDescription("DriverCreated");
		u.setEmail("java@m.m");
		u.setUsername("java_user");
		u.setFirstname("Java");
		u.setLastname("CafeBabe");
		dao.addUser(u);
	}

	public void deleteUser(final UserDao dao) {
		dao.deleteUser("java_user");
	}

	public void getUsers(final UserDao dao) {
		final List<User> users = dao.getAllUsers();
		System.out
		.println("----------------------------------------------------------------------------------------------------------------------------------------------------------------");
		System.out.printf("%20s%20s%20s%15s%12s%40s%30s%30s\n", "UserName", "password", "firstname", "lastname",
				"description", "email", "created_date", "modified_date");
		System.out
		.println("----------------------------------------------------------------------------------------------------------------------------------------------------------------");
		for (final User user : users) {
			System.out.printf("%20s%20s%20s%15s%12s%40s%30s%30s\n", user.getUsername(), user.getPassword(),
					user.getFirstname(), user.getLastname(), user.getDescription(), user.getEmail(),
					user.getCreatedDate(), user.getModified_date());
		}
	}

	public void listHosts(final CassandraSessionFactory cassandraSessionFactory) {
		final Cluster cluster = cassandraSessionFactory.getCluster();
		final Metadata metadata = cluster.getMetadata();
		System.out.printf("Connected to cluster: %s\n", metadata.getClusterName());
		for (final Host host : metadata.getAllHosts()) {
			System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(), host.getAddress(),
					host.getRack());
		}
	}

}

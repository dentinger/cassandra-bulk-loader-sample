package com.dentinger.dao.ds;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.dentinger.domain.User;
import com.dentinger.ds.util.CassandraSessionFactory;

/**
 * Created by dan on 8/20/14.
 */
@Component
public class UserDao {
	private static User convertRowToUser(final Row row) {
		final User u = new User();
		if (row.getString("username") != null) {
			u.setUsername(row.getString("username"));
		}
    if(row.getString("password") != null) {
      u.setPassword(row.getString("password"));
    }
		if (row.getString("email") != null) {
			u.setEmail(row.getString("email"));
		}
		if (row.getString("description") != null) {
			u.setDescription(row.getString("description"));
		}
		if (row.getDate("created_date") != null) {
			u.setCreatedDate(row.getDate("created_date"));
		}
		if (row.getDate("modified_date") != null) {
			u.setModified_date(row.getDate("modified_date"));
		}
		if (row.getString("firstname") != null) {
			u.setFirstname(row.getString("firstname"));
		}
		if (row.getString("lastname") != null) {
			u.setLastname(row.getString("lastname"));
		}
		return u;
	}

	@Autowired
	private CassandraSessionFactory cassandraSessionFactory;
	private PreparedStatement singleSelectStatement;
	private PreparedStatement insertstatement;
	private PreparedStatement allUserStatement;
	private PreparedStatement deleteStatement;

	public void addUser(final User user) {
		final BoundStatement boundStatement = new BoundStatement(this.insertstatement);
		// ( username, description, password, email, firstname, lastname,
		getSession().execute(
				boundStatement.bind(user.getUsername(), user.getDescription(), user.getPassword(), user.getEmail(),
						user.getFirstname(), user.getLastname()));
	}

	public void deleteUser(final String id) {
		final BoundStatement boundStatement = new BoundStatement(this.deleteStatement);
		getSession().execute(boundStatement.bind(id));
	}

	public User findUser(final String id) {
		final BoundStatement boundStatement = new BoundStatement(this.singleSelectStatement);
		final ResultSet resultSet = getSession().execute(boundStatement.bind(id));
		final Row row = resultSet.one();

		if (row != null) {
			final User u = convertRowToUser(row);
			return u;
		} else {
			return new User();
		}
	}

	public List<User> getAllUsers() {
		final BoundStatement bs = new BoundStatement(this.allUserStatement);
		final ResultSet resultSet = getSession().execute(bs);
		final List<User> results = new ArrayList<User>();

		for (final Row row : resultSet) {
			final User u = convertRowToUser(row);
			results.add(u);
		}
		return results;
	}

	private Session getSession() {
		return this.cassandraSessionFactory.getSession();
	}

	@PostConstruct
	public void prepareStatements() {
		this.insertstatement = getSession()
				.prepare(
						"INSERT INTO LINKED_OUT.USERS2 "
								+ "( username, description, password, email, firstname, lastname, created_date, modified_date) "
								+ "VALUES ( ?,?,?,?,?,?,dateOf(now()),dateOf(now())) ;");

		this.allUserStatement = getSession().prepare("SELECT * FROM LINKED_OUT.USERS2;");
		this.singleSelectStatement = getSession().prepare("SELECT * FROM LINKED_OUT.USERS2 where username = ?;");
		this.deleteStatement = getSession().prepare("DELETE FROM LINKED_OUT.USERS2 WHERE username = ?;");
	}
}

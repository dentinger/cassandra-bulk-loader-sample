package com.dentinger.sample;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;

import com.dentinger.domain.Links;
import com.dentinger.domain.User;
import com.dentinger.sample.util.CreateSampleData;

/**
 * Created by dan on 8/26/14.
 */
public class SampleBulkLoader {
	public static void main(final String[] args) throws IOException {

		final SampleBulkLoader sampleBulkLoader = new SampleBulkLoader();
		sampleBulkLoader.loadRandomUserData("linked_out", "users2");
		sampleBulkLoader.loadRandomLinksData("linked_out", "links");
	}

	private final Random rand;
	private BufferedWriter out;
	private final int totalTrans = 50000;
	private File filePath;
	private static final String links_schema = "CREATE TABLE linked_out.links ( " + "  id text, " + "  ins set<text>, "
			+ "  outs set<text>, " + "  PRIMARY KEY ((id)));";
	private static final String users2_schema = "CREATE TABLE linked_out.users2 (" + "  created_date timestamp, "
			+ "  description text, " + "  email text, " + "  modified_date timestamp, " + "  password text, "
			+ "  username text, " + "  firstname text, " + "  lastname text, " + "  PRIMARY KEY ((username)) " + ") ;";

	private static final String users2_insert = "insert into linked_out.users2 (username, email,description ,"
			+ "password , firstname ,lastname, created_date, modified_date) VALUES ( ?,?,?,?,?,?,dateOf(now()),dateOf(now()))";

	private static final String links_insert = "insert into linked_out.links (id, ins, outs) values (?,?,?);";

	private CQLSSTableWriter writer;

	private CreateSampleData csd;

	public SampleBulkLoader() {
		this.rand = new Random(System.currentTimeMillis());
	}

	public void createDirectories(final String keyspace, final String table) {
		final File dir = new File(keyspace);
		if (!dir.exists()) {
			dir.mkdir();
		}
		this.filePath = new File(dir, table);
		if (!this.filePath.exists()) {
			this.filePath.mkdir();
		}
		System.out.println("Using directory: " + this.filePath.getAbsolutePath());
	}

	public void loadRandomLinksData(final String keyspace, final String table) {
		int counter = 0;
		createDirectories(keyspace, table);

		if (this.csd == null) {
			try {
				this.csd = new CreateSampleData();
			} catch (final Exception e) {
				e.printStackTrace();
			}
		}
		final List<User> users = this.csd.getList();

		this.writer = CQLSSTableWriter.builder().forTable(links_schema).using(links_insert)
				.inDirectory(this.filePath.getAbsolutePath()).build();
		final int maxLinks = users.size() % 15;
		for (final User u : users) {
			final Links l = new Links();
			l.setId(u.getUsername());
			final int linkCount = this.rand.nextInt(maxLinks);
			final Set<String> ins = new HashSet<String>();
			final Set<String> outs = new HashSet<String>();
			for (int i = 0; i < linkCount; i++) {
				ins.add(users.get(this.rand.nextInt(users.size())).getUsername());
				outs.add(users.get(this.rand.nextInt(users.size())).getUsername());
			}
			l.setIns(ins);
			l.setOuts(outs);

			try {
				this.writer.addRow(l.getId(), l.getIns(), l.getOuts());
				counter++;
			} catch (final InvalidRequestException e) {
				e.printStackTrace();
			} catch (final IOException e) {
				e.printStackTrace();
			}
		}
		System.out.println("Number of Links writen: " + counter);
		try {
			this.writer.close();
		} catch (final IOException e) {
			e.printStackTrace();
		}

	}

	public void loadRandomUserData(final String keyspace, final String table) {
		int counter = 0;
		createDirectories(keyspace, table);
		if (this.csd == null) {
			try {
				this.csd = new CreateSampleData();
			} catch (final Exception e) {
				e.printStackTrace();
			}
		}
		final List<User> users = this.csd.getList();

		this.writer = CQLSSTableWriter.builder().forTable(users2_schema).using(users2_insert)
				.inDirectory(this.filePath.getAbsolutePath()).build();

		for (final User u : users) {
			try {
				this.writer.addRow(u.getUsername(), u.getEmail(), u.getDescription(), u.getPassword(),
						u.getFirstname(), u.getLastname());
				counter++;
			} catch (final InvalidRequestException e) {
				e.printStackTrace();
			} catch (final IOException e) {
				e.printStackTrace();
			}
		}
		System.out.println("Number of Users written: " + counter);
		try {
			this.writer.close();
		} catch (final IOException e) {
			e.printStackTrace();
		}

	}
}

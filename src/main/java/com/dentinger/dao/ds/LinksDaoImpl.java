package com.dentinger.dao.ds;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.dentinger.domain.Links;
import com.dentinger.ds.util.CassandraSessionFactory;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class LinksDaoImpl implements LinksDao {
  @Autowired
  private CassandraSessionFactory cassandraSessionFactory;

  private PreparedStatement insertstatement;
  private PreparedStatement selectLinksStatement;
  private PreparedStatement updateInLinks;
  private PreparedStatement updateOutLinks;

  @PostConstruct
  public void prepareStatements() {
    //Not idea for the prepared statement.  Have a prepared statement for the scenarios of no outs so a null is not created
    // and where ins is null so a null is not created.
    // For the next exercise think about what happens when a user had ins or outs, but the now are empty?
    insertstatement = getSession().prepare(
        "INSERT INTO LINKED_OUT.links " +
            "(id, ins, outs) VALUES " +
            "(?, ?, ?);"
    );

    selectLinksStatement = getSession().prepare("select * from linked_out.links where id = ?;");
    updateInLinks = getSession().prepare("update linked_out.links set outs = outs + ? where id =?");

    updateInLinks = getSession().prepare("update linked_out.links set ins = ins + ? where id =?");
  }
  @Override public void updateLink(Links links) {
    BoundStatement boundStatement = new BoundStatement(insertstatement);
    getSession().execute(boundStatement.bind(links.getId(), links.getIns(), links.getOuts()));

  }
  public void addInlink(String id, String inLink) {
    BoundStatement boundStatement = new BoundStatement(updateInLinks);
    Set<String> s = new HashSet<String>();
    s.add(inLink);
    getSession().execute(boundStatement.bind(s, id));
  }
  public void addOutlink(String id, String outLink) {
    BoundStatement boundStatement = new BoundStatement(updateOutLinks);
    Set<String> s = new HashSet<String>();
    s.add(outLink);
    getSession().execute(boundStatement.bind(outLink, id));
  }

  @Override public Links getLinks(String id) {
    BoundStatement boundStatement = new BoundStatement(selectLinksStatement);
    ResultSet resultSet = getSession().execute(boundStatement.bind(id));
    Links linksForId = new Links();
    linksForId.setId(id);

    Row row = resultSet.one();
    if(row != null) {

      linksForId.setIns(row.getSet("ins", String.class));
      linksForId.setOuts(row.getSet("outs", String.class));
    }
    return linksForId;
  }

  private Session getSession() {
    return cassandraSessionFactory.getSession();
  }
}

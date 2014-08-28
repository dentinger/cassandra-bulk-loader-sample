package com.dentinger.ds.util;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;

import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.List;

import javax.annotation.PreDestroy;
import javax.net.ssl.SSLContext;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

/**
 * The Class CassandraSessionFacotry.
 */
@Component
public class CassandraSessionFactory
{
  
  /** The CassandraSessionFacotry Constant DEFAULT_CIPHERS. */
  private static final String[] DEFAULT_CIPHERS = {"TLS_RSA_WITH_AES_128_CBC_SHA"};
  /** The CassandraSessionFacotry Constant CONSTANT_RECONNECTION_POLICY. */
  private static final long CONSTANT_RECONNECTION_POLICY = 100L;
  
  /** The CassandraSessionFacotry cluster. */
  private Cluster cluster;
  
  /** The CassandraSessionFacotry session. */
  private Session session;
  
  /** The CassandraSessionFacotry nodes. */
  @Value("#{'${cassandra.nodes}'.split(',')}")
  private List<String> nodes;
  
  /** The CassandraSessionFacotry data center. */
  @Value("${cassandra.dataCenter:#{'DC1'}}")
  private String dataCenter;
  
  /** The CassandraSessionFacotry keyspace. */
  @Value("${cassandra.keySpace:#{'test'}}")
  private String keyspace;
  
  /** The CassandraSessionFacotry username. */
  @Value("${cassandra.username:#{null}}")
  private String username;
  
  /** The CassandraSessionFacotry password. */
  @Value("${cassandra.password:#{null}}")
  private String password;

  @Value("${cassandra.ssl:#{'false'}}")
  private String useSSL;

  @Value("${cassandra.policy:#{'tokenrr'}}")
  private String policy;

  /**
   * Instantiates a new cassandra session facotry.
   */
  public CassandraSessionFactory()
  {
    super();
  }
  
  /**
   * Gets the CassandraSessionFacotry cluster.
   * 
   * @return the cluster
   */
  public Cluster getCluster()
  {
    if(cluster == null) {
      final Builder builder = Cluster.builder();
      if (policy.equalsIgnoreCase("tokenrr")) {
        builder.withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
      } else if (policy.equalsIgnoreCase("DCAWARE")) {
        builder.withLoadBalancingPolicy(new DCAwareRoundRobinPolicy(dataCenter));

      } else if (policy.equalsIgnoreCase("TOKENDCAWARE")) {
        builder
            .withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy(dataCenter)));

      }
      builder.withReconnectionPolicy(
          new ConstantReconnectionPolicy(CONSTANT_RECONNECTION_POLICY));
      builder.withCredentials(username, password);
      if (Boolean.valueOf(useSSL)) {
        final SSLOptions op = new SSLOptions(makeDefaultContext(), DEFAULT_CIPHERS);
        builder.withSSL(op);
      }
      for (final String nodeName : nodes) {
        builder.addContactPoint(nodeName);
      }

      cluster = builder.build();

    }
    return cluster;
  }
  
  /**
   * Gets the CassandraSessionFacotry session.
   * 
   * @return the session
   */
  public synchronized Session getSession()
  {
    if (session == null)
    {
      session = getCluster().connect(keyspace);
    }
    return session;
  }
  
  /**
   * Needed to create SSLOptions from JVM Parameters. this is needed since the current Datastax Driver does not
   * have a constructor that allows the overriding of only the Ciphers.
   * 
   * @return default ssl context
   * @throws IllegalStateException the illegal state exception
   */
  private SSLContext makeDefaultContext() throws IllegalStateException
  {
    try
    {
      final SSLContext ctx = SSLContext.getInstance("TLS");
      ctx.init(null, null, null); // use defaults
      return ctx;
    }
    catch (final NoSuchAlgorithmException e)
    {
      throw new RuntimeException("This JVM doesn't support TLS, this shouldn't happen", e);
    }
    catch (final KeyManagementException e)
    {
      throw new IllegalStateException("Cannot initialize SSL Context", e);
    }
  }
  
  /**
   * CassandraSessionFacotry Shutdown.
   */
  @PreDestroy
  public synchronized void shutdown()
  {
    if (session != null)
    {
      session.close();
    }
    if (cluster != null)
    {
      cluster.close();
    }
  }
}

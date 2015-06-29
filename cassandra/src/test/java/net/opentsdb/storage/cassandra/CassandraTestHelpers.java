package net.opentsdb.storage.cassandra;


import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;

/**
 * Utility methods that are useful for testing the cassandra store.
 */
class CassandraTestHelpers {
  /**
   * A default timeout in milliseconds to wait for Cassandra in tests.
   */
  public static final long TIMEOUT = 50;

  /**
   * Clear the data in all tables.
   *
   * @param session A live session to talk to
   */
  static void truncate(final Session session) {
    session.execute(QueryBuilder.truncate(Tables.KEYSPACE, Tables.DATAPOINTS));
    session.execute(QueryBuilder.truncate(Tables.KEYSPACE, Tables.TS_INVERTED_INDEX));
    session.execute(QueryBuilder.truncate(Tables.KEYSPACE, Tables.ID_TO_NAME));
    session.execute(QueryBuilder.truncate(Tables.KEYSPACE, Tables.NAME_TO_ID));
  }
}

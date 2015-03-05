package net.opentsdb.storage.cassandra;


import com.datastax.driver.core.Session;

/**
 * Utility methods that are useful for testing the cassandra store
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
    session.execute("TRUNCATE tsdb." + Tables.DATAPOINTS);
    session.execute("TRUNCATE tsdb." + Tables.TS_INVERTED_INDEX);
    session.execute("TRUNCATE tsdb." + Tables.ID_TO_NAME);
    session.execute("TRUNCATE tsdb." + Tables.NAME_TO_ID);
  }
}

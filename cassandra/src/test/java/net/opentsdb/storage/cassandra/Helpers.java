package net.opentsdb.storage.cassandra;


import com.datastax.driver.core.Session;

/**
 * Utility methods that are useful for testing the cassandra store
 */
class Helpers {
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
    session.execute("TRUNCATE tsdb." + Tables.MAX_ID);
    session.execute("TRUNCATE tsdbunique." + Tables.ID_NAME_LOCK);
  }
}

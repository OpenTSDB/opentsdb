package net.opentsdb.storage.cassandra.statements;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.timestamp;

import net.opentsdb.storage.cassandra.Tables;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

/**
 * A collection of {@link com.datastax.driver.core.PreparedStatement PreparedStatements} that are
 * used to add data points.
 *
 * @see net.opentsdb.storage.cassandra.CassandraStore#addPoint
 */
public class AddPointStatements {
  private final PreparedStatement addFloatStatement;
  private final PreparedStatement addDoubleStatement;
  private final PreparedStatement addLongStatement;

  /**
   * Instantiate the statements and prepare them with the provided session.
   *
   * @param session The session to prepare the statements with.
   */
  public AddPointStatements(final Session session) {
    addFloatStatement = session.prepare(
        insertInto(Tables.KEYSPACE, Tables.DATAPOINTS)
            .value("timeseries_id", bindMarker())
            .value("basetime", bindMarker())
            .value("timestamp", bindMarker())
            .value("float_value", bindMarker())
            .using(timestamp(bindMarker())));

    addDoubleStatement = session.prepare(
        insertInto(Tables.KEYSPACE, Tables.DATAPOINTS)
            .value("timeseries_id", bindMarker())
            .value("basetime", bindMarker())
            .value("timestamp", bindMarker())
            .value("double_value", bindMarker())
            .using(timestamp(bindMarker())));

    addLongStatement = session.prepare(
        insertInto(Tables.KEYSPACE, Tables.DATAPOINTS)
            .value("timeseries_id", bindMarker())
            .value("basetime", bindMarker())
            .value("timestamp", bindMarker())
            .value("long_value", bindMarker())
            .using(timestamp(bindMarker())));
  }

  public PreparedStatement addFloatStatement() {
    return addFloatStatement;
  }

  public PreparedStatement addDoubleStatement() {
    return addDoubleStatement;
  }

  public PreparedStatement addLongStatement() {
    return addLongStatement;
  }
}

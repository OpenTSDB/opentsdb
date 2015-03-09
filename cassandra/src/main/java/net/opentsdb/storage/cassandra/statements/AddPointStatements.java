
package net.opentsdb.storage.cassandra.statements;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import net.opentsdb.storage.cassandra.Tables;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.timestamp;

/**
 * A collection of {@link com.datastax.driver.core.PreparedStatement
 * PreparedStatements} that are used to add data points.
 *
 * @see net.opentsdb.storage.cassandra.CassandraStore#addPoint
 */
public class AddPointStatements {
  public final PreparedStatement addFloatStatement;
  public final PreparedStatement addDoubleStatement;
  public final PreparedStatement addLongStatement;

  public AddPointStatements(final Session session) {
    addFloatStatement = session.prepare(
        insertInto(Tables.KEYSPACE, Tables.DATAPOINTS)
            .value("tsid", bindMarker())
            .value("basetime", bindMarker())
            .value("timestamp", bindMarker())
            .value("fval", bindMarker())
            .using(timestamp(bindMarker())));

    addDoubleStatement = session.prepare(
        insertInto(Tables.KEYSPACE, Tables.DATAPOINTS)
            .value("tsid", bindMarker())
            .value("basetime", bindMarker())
            .value("timestamp", bindMarker())
            .value("dval", bindMarker())
            .using(timestamp(bindMarker())));

    addLongStatement = session.prepare(
        insertInto(Tables.KEYSPACE, Tables.DATAPOINTS)
            .value("tsid", bindMarker())
            .value("basetime", bindMarker())
            .value("timestamp", bindMarker())
            .value("lval", bindMarker())
            .using(timestamp(bindMarker())));
  }
}

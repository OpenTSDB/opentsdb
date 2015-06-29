package net.opentsdb.storage.cassandra;

import static com.datastax.driver.core.querybuilder.QueryBuilder.batch;
import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.delete;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import static com.datastax.driver.core.querybuilder.QueryBuilder.update;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.Futures.transform;
import static net.opentsdb.storage.cassandra.CassandraConst.CHARSET;
import static net.opentsdb.storage.cassandra.CassandraLabelId.fromLong;
import static net.opentsdb.storage.cassandra.CassandraLabelId.toLong;

import net.opentsdb.core.DataPoints;
import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.LabelMeta;
import net.opentsdb.search.ResolvedSearchQuery;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.storage.cassandra.functions.FirstOrAbsentFunction;
import net.opentsdb.storage.cassandra.functions.IsEmptyFunction;
import net.opentsdb.storage.cassandra.functions.ResultSetToVoid;
import net.opentsdb.storage.cassandra.statements.AddPointStatements;
import net.opentsdb.time.JdkTimeProvider;
import net.opentsdb.uid.IdException;
import net.opentsdb.uid.IdQuery;
import net.opentsdb.uid.IdentifierDecorator;
import net.opentsdb.uid.LabelId;
import net.opentsdb.uid.TimeseriesId;
import net.opentsdb.uid.UniqueIdType;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * An implementation of {@link TsdbStore} that uses Cassandra as the underlying storage backend.
 */
public class CassandraStore extends TsdbStore {
  /**
   * The Cassandra cluster that we are connected to.
   */
  private final Cluster cluster;
  /**
   * The current Cassandra session.
   */
  private final Session session;

  /**
   * A time provider to tell the current time.
   */
  private final JdkTimeProvider timeProvider;

  /**
   * The statement used by the {@link #addPoint} method.
   */
  private final PreparedStatement addFloatStatement;
  private final PreparedStatement addDoubleStatement;
  private final PreparedStatement addLongStatement;
  private PreparedStatement insertTagsStatement;
  /**
   * The statement used by the {@link #allocateLabel} method.
   */
  private PreparedStatement createIdStatement;
  /**
   * Used for {@link #allocateLabel}, the one that does rename.
   */
  private PreparedStatement updateUidNameStatement;
  private PreparedStatement updateNameUidStatement;
  /**
   * The statement used when trying to get name or id.
   */
  private PreparedStatement getNameStatement;
  private PreparedStatement getIdStatement;


  /**
   * Create a new instance that will use the provided Cassandra cluster and session instances.
   *
   * @param cluster A built and configured cluster instance
   * @param session A configured and connected session instance
   */
  public CassandraStore(final Cluster cluster,
                        final Session session) {
    this.cluster = cluster;
    this.session = session;

    this.timeProvider = new JdkTimeProvider();

    final AddPointStatements addPointStatements = new AddPointStatements(session);
    this.addFloatStatement = addPointStatements.addFloatStatement();
    this.addDoubleStatement = addPointStatements.addDoubleStatement();
    this.addLongStatement = addPointStatements.addLongStatement();

    prepareStatements();
  }

  /**
   * Calculate the base time based on a timestamp to be used in a row key.
   */
  static long buildBaseTime(final long timestamp) {
    return (timestamp - (timestamp % CassandraConst.BASE_TIME_PERIOD));
  }

  /**
   * In this method we prepare all the statements used for accessing Cassandra.
   */
  private void prepareStatements() {
    checkNotNull(session);

    createIdStatement = session.prepare(
        batch(
            insertInto(Tables.KEYSPACE, Tables.ID_TO_NAME)
                .value("label_id", bindMarker())
                .value("type", bindMarker())
                .value("creation_time", bindMarker())
                .value("name", bindMarker()),
            insertInto(Tables.KEYSPACE, Tables.NAME_TO_ID)
                .value("name", bindMarker())
                .value("type", bindMarker())
                .value("creation_time", bindMarker())
                .value("label_id", bindMarker())))
        .setConsistencyLevel(ConsistencyLevel.ALL);

    updateUidNameStatement = session.prepare(
        update(Tables.KEYSPACE, Tables.ID_TO_NAME)
            .with(set("name", bindMarker()))
            .where(eq("label_id", bindMarker()))
            .and(eq("type", bindMarker())));

    updateNameUidStatement = session.prepare(
        batch(
            delete()
                .from(Tables.KEYSPACE, Tables.NAME_TO_ID)
                .where(eq("name", bindMarker()))
                .and(eq("type", bindMarker())),
            insertInto(Tables.KEYSPACE, Tables.NAME_TO_ID)
                .value("name", bindMarker())
                .value("type", bindMarker())
                .value("label_id", bindMarker())));

    getNameStatement = session.prepare(
        select()
            .all()
            .from(Tables.KEYSPACE, Tables.ID_TO_NAME)
            .where(eq("label_id", bindMarker()))
            .and(eq("type", bindMarker()))
            .limit(2));

    getIdStatement = session.prepare(
        select()
            .all()
            .from(Tables.KEYSPACE, Tables.NAME_TO_ID)
            .where(eq("name", bindMarker()))
            .and(eq("type", bindMarker()))
            .limit(2));

    insertTagsStatement = session.prepare(
        insertInto(Tables.KEYSPACE, Tables.TS_INVERTED_INDEX)
            .value("label_id", bindMarker())
            .value("type", bindMarker())
            .value("timeseries_id", bindMarker()));
  }

  public Session getSession() {
    return session;
  }

  @Nonnull
  @Override
  public ListenableFuture<Annotation> getAnnotation(final LabelId metric,
                                                    final ImmutableMap<LabelId, LabelId> tags,
                                                    final long startTime) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Nonnull
  @Override
  public ListenableFuture<Void> addPoint(final TimeseriesId tsuid,
                                         final long timestamp,
                                         final float value) {
    final BoundStatement addPointStatement = addFloatStatement.bind()
        .setFloat(3, value);
    return addPoint(addPointStatement, tsuid.metric(), tsuid.tags(), timestamp);
  }

  @Nonnull
  @Override
  public ListenableFuture<Void> addPoint(final TimeseriesId tsuid,
                                         final long timestamp,
                                         final double value) {
    final BoundStatement addPointStatement = addDoubleStatement.bind()
        .setDouble(3, value);
    return addPoint(addPointStatement, tsuid.metric(), tsuid.tags(), timestamp);
  }

  @Nonnull
  @Override
  public ListenableFuture<Void> addPoint(final TimeseriesId tsuid,
                                         final long timestamp,
                                         final long value) {
    final BoundStatement addPointStatement = addLongStatement.bind()
        .setLong(3, value);
    return addPoint(addPointStatement, tsuid.metric(), tsuid.tags(), timestamp);
  }

  @Nonnull
  private ListenableFuture<Void> addPoint(final BoundStatement addPointStatement,
                                          final LabelId metric,
                                          final List<LabelId> tags,
                                          final long timestamp) {
    Hasher tsidHasher = Hashing.murmur3_128().newHasher()
        .putLong(toLong(metric));

    for (final LabelId tag : tags) {
      tsidHasher.putLong(toLong(tag));
    }

    final ByteBuffer tsid = ByteBuffer.wrap(tsidHasher.hash().asBytes());

    final long baseTime = buildBaseTime(timestamp);

    addPointStatement.setBytesUnsafe(0, tsid);
    addPointStatement.setLong(1, baseTime);
    addPointStatement.setLong(2, timestamp);
    addPointStatement.setLong(4, timestamp);

    final ResultSetFuture future = session.executeAsync(addPointStatement);

    return transform(future, new ResultSetToVoid());
  }

  private void writeTimeseriesIdIndex(final LabelId metric,
                                      final Map<LabelId, LabelId> tags,
                                      final ByteBuffer tsid) {
    session.executeAsync(insertTagsStatement.bind()
        .setLong(0, toLong(metric))
        .setString(1, UniqueIdType.METRIC.toValue())
        .setBytesUnsafe(2, tsid));

    for (final Map.Entry<LabelId, LabelId> entry : tags.entrySet()) {
      session.executeAsync(insertTagsStatement.bind()
          .setLong(0, toLong(entry.getKey()))
          .setString(1, UniqueIdType.TAGK.toValue())
          .setBytesUnsafe(2, tsid));

      session.executeAsync(insertTagsStatement.bind()
          .setLong(0, toLong(entry.getValue()))
          .setString(1, UniqueIdType.TAGV.toValue())
          .setBytesUnsafe(2, tsid));
    }
  }

  @Override
  public void close() {
    cluster.close();
  }

  @Nonnull
  @Override
  public ListenableFuture<Optional<LabelId>> getId(final String name,
                                                   final UniqueIdType type) {
    ListenableFuture<List<LabelId>> idsFuture = getIds(name, type);
    return transform(idsFuture, new FirstOrAbsentFunction<LabelId>());
  }

  /**
   * Fetch the first two IDs that are associated with the provided name and type.
   *
   * @param name The name to fetch IDs for
   * @param type The type of IDs to fetch
   * @return A future with a list of the first two found IDs
   */
  @Nonnull
  ListenableFuture<List<LabelId>> getIds(final String name,
                                         final UniqueIdType type) {
    ResultSetFuture idsFuture = session.executeAsync(
        getIdStatement.bind(name, type.toValue()));

    return transform(idsFuture, new Function<ResultSet, List<LabelId>>() {
      @Override
      public List<LabelId> apply(@Nullable final ResultSet result) {
        ImmutableList.Builder<LabelId> builder = ImmutableList.builder();

        for (final Row row : result) {
          final long id = row.getLong("label_id");
          builder.add(fromLong(id));
        }

        return builder.build();
      }
    });
  }

  @Nonnull
  @Override
  public ListenableFuture<Optional<String>> getName(final LabelId id,
                                                    final UniqueIdType type) {
    ListenableFuture<List<String>> namesFuture = getNames(id, type);
    return transform(namesFuture, new FirstOrAbsentFunction<String>());
  }

  /**
   * Fetch the first two names that are associated with the provided id and type.
   *
   * @param id The id to fetch names for
   * @param type The type of names to fetch
   * @return A future with a list of the first two found names
   */
  @Nonnull
  ListenableFuture<List<String>> getNames(final LabelId id,
                                          final UniqueIdType type) {
    ResultSetFuture namesFuture = session.executeAsync(
        getNameStatement.bind(id, type.toValue()));

    return transform(namesFuture, new Function<ResultSet, List<String>>() {
      @Override
      public List<String> apply(@Nullable final ResultSet result) {
        ImmutableList.Builder<String> builder = ImmutableList.builder();

        for (final Row row : result) {
          final String name = row.getString("name");
          builder.add(name);
        }

        return builder.build();
      }
    });
  }

  @Nonnull
  @Override
  public ListenableFuture<LabelMeta> getMeta(final LabelId uid,
                                             final UniqueIdType type) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Nonnull
  @Override
  public ListenableFuture<Boolean> updateMeta(LabelMeta meta) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Nonnull
  @Override
  public ListenableFuture<Void> deleteLabel(final String name,
                                            final UniqueIdType type) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  /**
   * Check if (id, type) is available and return a future that contains a boolean that will be true
   * if the id is available or false if otherwise.
   *
   * @param id The name to check
   * @param type The type to check
   * @return A future that contains a boolean that indicates if the id was available
   */
  private ListenableFuture<Boolean> isIdAvailable(final long id,
                                                  final UniqueIdType type) {
    return transform(getNames(fromLong(id), type), new IsEmptyFunction());
  }

  /**
   * Check if (name, type) is available and return a future that contains a boolean that will be
   * true if the name is available or false if otherwise.
   *
   * @param name The name to check
   * @param type The type to check
   * @return A future that contains a boolean that indicates if the name was available
   */
  private ListenableFuture<Boolean> isNameAvailable(final String name,
                                                    final UniqueIdType type) {
    return transform(getIds(name, type), new IsEmptyFunction());
  }

  /**
   * Check if either of (id, type) and (name, type) are taken or if both are available. If either of
   * the combinations already are taken the returned future will contain an {@link
   * net.opentsdb.uid.IdException}.
   *
   * @param id The id to check if it is available
   * @param name The name to check if it is available
   * @param type The type of id and name to check if it available
   * @return A future that contains an exception if either of the above combinations were taken.
   * Otherwise a future with meaningless contents will be returned.
   */
  private ListenableFuture<Void> checkAvailable(final long id,
                                                final String name,
                                                final UniqueIdType type) {
    ImmutableList<ListenableFuture<Boolean>> availableList =
        ImmutableList.of(isIdAvailable(id, type), isNameAvailable(name, type));
    final ListenableFuture<List<Boolean>> availableFuture = Futures.allAsList(availableList);

    return transform(availableFuture, new AsyncFunction<List<Boolean>, Void>() {
      @Override
      public ListenableFuture<Void> apply(final List<Boolean> available) {
        // These are in the same order as they are provided in the call
        // to Futures#allAsList.
        final Boolean idAvailable = available.get(0);
        final Boolean nameAvailable = available.get(1);

        if (!idAvailable) {
          return Futures.immediateFailedFuture(
              new IdException(id, type, "Id was already taken"));
        }

        if (!nameAvailable) {
          return Futures.immediateFailedFuture(
              new IdException(name, type, "Name was already taken"));
        }

        return Futures.immediateFuture(null);
      }
    });
  }

  /**
   * Save a new identifier with the provided information in Cassandra. This will not perform any
   * checks to see if the id already exists, you are expected to have done so already.
   *
   * @param id The id to associate with the provided name
   * @param name The name to save
   * @param type The type of id to save
   * @return A future containing the newly saved identifier
   */
  private ListenableFuture<LabelId> createId(final long id,
                                             final String name,
                                             final UniqueIdType type) {
    final Date createTimestamp = timeProvider.now();
    final ResultSetFuture save = session.executeAsync(
        createIdStatement.bind(createTimestamp.getTime(),
            id, type.toValue(), createTimestamp, name,
            name, type.toValue(), createTimestamp, id));

    return transform(save, new AsyncFunction<ResultSet, LabelId>() {
      @Override
      public ListenableFuture<LabelId> apply(final ResultSet result) {
        // The Cassandra driver will have thrown an exception if the insertion
        // failed in which case we would not be here so just return the id we
        // sent to Cassandra.
        return Futures.immediateFuture(fromLong(id));
      }
    });
  }

  /**
   * Allocate an ID for the provided (name, type). This will attempt to generate an ID that is
   * likely to be available. It will then check if this information is available and finally save
   * the information if it is. If the information could be saved the ID will be returned in a
   * future, otherwise the future will contain an {@link net.opentsdb.uid.IdException}.
   *
   * @param name The name to allocate an ID for
   * @param type The type of name to allocate an ID for
   * @return A future that contains the newly allocated ID if successful, otherwise the future will
   * contain a {@link net.opentsdb.uid.IdException}.
   */
  @Nonnull
  @Override
  public ListenableFuture<LabelId> allocateLabel(final String name,
                                                 final UniqueIdType type) {
    // This discards half the hash but it should still work ok with murmur3.
    final long id = Hashing.murmur3_128().hashString(name, CHARSET).asLong();

    // This does not protect us against someone trying to create the same
    // information in parallel but it is a convenience to the user so that we
    // do not even try to create if we can find an existing ID with the
    // information we are trying to allocate now.
    ListenableFuture<Void> availableFuture = checkAvailable(id, name, type);

    return transform(availableFuture, new AsyncFunction<Void, LabelId>() {
      @Override
      public ListenableFuture<LabelId> apply(final Void available) {
        // #checkAvailable will have thrown an exception if the id or name was
        // not available and if it did we would not be there. Thus we are now
        // free to create the id.
        return createId(id, name, type);
      }
    });
  }

  /**
   * For all intents and purposes this function works as a rename. In the HBase implementation the
   * other method {@link #allocateLabel} uses this method that basically overwrites the value no
   * matter what. This method is also used by the function {@link net.opentsdb.uid.UniqueId#rename}.
   *
   * @param name The name to write.
   * @param id The uid to use.
   * @param type The type of UID
   * @return The uid that was used.
   */
  @Nonnull
  @Override
  public ListenableFuture<LabelId> allocateLabel(final String name,
                                                 final LabelId id,
                                                 final UniqueIdType type) {
    /*
    TODO #zeeck this method should be considered to be changed to rename and the implementation
    changed in the HBaseStore. One of the prerequisites of this function is that the UID already
    exists.
     */

    // Get old name, we do this manually because the other method returns
    // a deferred and we want to avoid to mix deferreds between functions.
    final ResultSetFuture getNameFuture = session.executeAsync(
        getNameStatement.bind(toLong(id), type.toValue()));

    return transform(getNameFuture, new Function<ResultSet, LabelId>() {
      @Override
      public LabelId apply(@Nullable final ResultSet rows) {
        final String oldName = rows.one().getString("name");

        session.executeAsync(updateUidNameStatement.bind(name, toLong(id), type.toValue()));
        session.executeAsync(
            updateNameUidStatement.bind(oldName, type.toValue(), name, type.toValue(), toLong(id)));

        //TODO (zeeck) maybe check if this was ok
        return id;
      }
    });
  }

  @Nonnull
  @Override
  public ListenableFuture<Void> deleteAnnotation(final LabelId metric,
                                                 final ImmutableMap<LabelId, LabelId> tags,
                                                 final long startTime) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Nonnull
  @Override
  public ListenableFuture<Boolean> updateAnnotation(Annotation annotation) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Nonnull
  @Override
  public ListenableFuture<Integer> deleteAnnotations(final LabelId metric,
                                                     final ImmutableMap<LabelId, LabelId> tags,
                                                     final long startTime,
                                                     final long endTime) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public ListenableFuture<ImmutableList<DataPoints>> executeQuery(Object query) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public ListenableFuture<List<IdentifierDecorator>> executeIdQuery(final IdQuery query) {
    throw new UnsupportedOperationException("Not implemented yet!");
  }

  @Override
  public ListenableFuture<List<byte[]>> executeTimeSeriesQuery(final ResolvedSearchQuery query) {
    throw new UnsupportedOperationException("Not implemented yet");
  }
}

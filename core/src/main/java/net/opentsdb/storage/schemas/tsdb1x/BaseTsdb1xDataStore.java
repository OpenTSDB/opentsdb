package net.opentsdb.storage.schemas.tsdb1x;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import net.opentsdb.auth.AuthState;
import net.opentsdb.common.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.LowLevelMetricData;
import net.opentsdb.data.LowLevelTimeSeriesData;
import net.opentsdb.data.TimeSeriesDatum;
import net.opentsdb.data.TimeSeriesDatumStringId;
import net.opentsdb.data.TimeSeriesSharedTagsAndTimeData;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.stats.Span;
import net.opentsdb.storage.StorageException;
import net.opentsdb.storage.WriteStatus;
import net.opentsdb.uid.IdOrError;
import net.opentsdb.uid.UniqueIdStore;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class BaseTsdb1xDataStore implements Tsdb1xDataStore {

  private static ThreadLocal<MutableNumericValue> TL_NUMERIC_TYPES =
          ThreadLocal.withInitial(() -> new MutableNumericValue());
  protected final String id;
  protected final TSDB tsdb;

  protected final Schema schema;

  /** UGG! Set it! */
  protected UniqueIdStore uid_store;

  protected boolean write_appends;
  protected boolean encode_as_appends;

  protected Set<Class<? extends Exception>> retryExceptions;

  public BaseTsdb1xDataStore(final String id,
                             final TSDB tsdb,
                             final Schema schema) {
    this.id = id;
    this.tsdb = tsdb;
    this.schema = schema;
    this.uid_store = uid_store;
  }

  @Override
  public Deferred<WriteStatus> write(final AuthState state,
                                     final TimeSeriesDatum datum,
                                     final Span span) {
    final Span child;
    if (span != null && span.isDebug()) {
      child = span.newChild(getClass().getName() + ".write")
              .start();
    } else {
      child = null;
    }

    class RowKeyCB implements Callback<Deferred<WriteStatus>, IdOrError> {

      @Override
      public Deferred<WriteStatus> call(final IdOrError ioe) throws Exception {
        if (ioe.id() == null) {
          if (child != null) {
            child.setErrorTags(ioe.exception())
                    .setTag("state", ioe.state().toString())
                    .setTag("message", ioe.error())
                    .finish();
          }
          switch (ioe.state()) {
            case RETRY:
              return Deferred.fromResult(WriteStatus.retry(ioe.error()));
            case REJECTED:
              return Deferred.fromResult(WriteStatus.rejected(ioe.error()));
            case ERROR:
              return Deferred.fromResult(WriteStatus.error(ioe.error(), ioe.exception()));
            default:
              throw new StorageException("Unexpected resolution state: "
                      + ioe.state());
          }
        }

        // TODO - handle different types
        long base_time = datum.value().timestamp().epoch();
        base_time = base_time - (base_time % Schema.MAX_RAW_TIMESPAN);
        final Codec codec = schema.getEncoder(datum.value().type());
        if (codec == null) {
          return Deferred.fromResult(WriteStatus.error("No codec for type: "
                  + datum.value().type(), null));
        }
        WriteStatus status = codec.encode(datum.value(),
                write_appends || encode_as_appends, (int) base_time, null);
        if (status.state() != WriteStatus.WriteState.OK) {
          return Deferred.fromResult(status);
        }

        if (write_appends) {
          // TODO - Copying the arrays sucks! We have to for now though as the
          // asynchbase client can req-ueue the RPCs so we'd lose thread locality.
          return writeAppend(ioe.id(),
                  Arrays.copyOf(codec.qualifiers()[0], codec.qualifierLengths()[0]),
                  Arrays.copyOf(codec.values()[0], codec.valueLengths()[0]),
                  child);
        } else {
          // same for co-proc and puts. The encode method figures out
          // the qualifier and values.
          return write(ioe.id(),
                  Arrays.copyOf(codec.qualifiers()[0], codec.qualifierLengths()[0]),
                  Arrays.copyOf(codec.values()[0], codec.valueLengths()[0]),
                  child);
        }
      }

    }

    return schema.createRowKey(state, datum, null, child)
            .addCallbackDeferring(new RowKeyCB())
            .addErrback(new WriteErrorCB(child));
  }

  @Override
  public Deferred<List<WriteStatus>> write(final AuthState state,
                                           final TimeSeriesSharedTagsAndTimeData data,
                                           final Span span) {
    final Span child;
    if (span != null && span.isDebug()) {
      child = span.newChild(getClass().getName() + ".write")
              .start();
    } else {
      child = null;
    }

    final Iterator<TimeSeriesDatum> iterator = data.iterator();
    if (!iterator.hasNext()) {
      if (child != null) {
        child.finish();
      }
      return Deferred.fromResult(Collections.emptyList());
    }

    long temp_time = data.timestamp().epoch();
    temp_time = temp_time - (temp_time % Schema.MAX_RAW_TIMESPAN);
    final int base_timestamp = (int) temp_time;

    final TimeSeriesDatum datum = iterator.next();

    class MetricCB implements Callback<Deferred<WriteStatus>, IdOrError> {
      final IdOrError tag_ioe;
      final TimeSeriesDatum datum;

      MetricCB(final TimeSeriesDatum datum, final IdOrError tag_ioe) {
        this.datum = datum;
        this.tag_ioe = tag_ioe;
      }

      @Override
      public Deferred<WriteStatus> call(final IdOrError metric_ioe) throws Exception {
        switch (metric_ioe.state()) {
          case RETRY:
            return Deferred.fromResult(WriteStatus.RETRY);
          case REJECTED:
            return Deferred.fromResult(WriteStatus.REJECTED);
          case ERROR:
            return Deferred.fromResult(WriteStatus.error(metric_ioe.error(),
                    metric_ioe.exception()));
        }

        // TODO - someday, re-use arrays
        boolean has_salt = schema.saltBuckets() > 0 && schema.saltWidth() > 0;
        final int len = (has_salt ? schema.saltWidth() : 0) +
                metric_ioe.id().length +
                Schema.TIMESTAMP_BYTES +
                tag_ioe.id().length;
        final byte[] key = new byte[len];
        int index = has_salt ? 1 : 0;
        System.arraycopy(metric_ioe.id(), 0, key, 0, schema.metricWidth());
        index += schema.metricWidth();
        index += Schema.TIMESTAMP_BYTES;
        System.arraycopy(tag_ioe.id(), 0, key, index, tag_ioe.id().length);
        schema.setBaseTime(key, base_timestamp);
        if (has_salt) {
          schema.prefixKeyWithSalt(key);
        }

        final Codec codec = schema.getEncoder(datum.value().value().type());
        if (codec == null) {
          return Deferred.fromResult(WriteStatus.error("No codec for type: "
                  + datum.value().value().type(), null));
        }
        WriteStatus status = codec.encode(datum.value(),
                write_appends || encode_as_appends, (int) base_timestamp, null);
        if (status.state() != WriteStatus.WriteState.OK) {
          return Deferred.fromResult(status);
        }

        if (write_appends) {
          return writeAppend(key,
                  Arrays.copyOf(codec.qualifiers()[0], codec.qualifierLengths()[0]),
                  Arrays.copyOf(codec.values()[0], codec.valueLengths()[0]),
                  child);
        } else {
          // same for co-proc and puts. The encode method figures out
          // the qualifier and values.
          return write(key,
                  Arrays.copyOf(codec.qualifiers()[0], codec.qualifierLengths()[0]),
                  Arrays.copyOf(codec.values()[0], codec.valueLengths()[0]),
                  child);
        }
      }
    }

    class TagCB implements Callback<Deferred<List<WriteStatus>>, IdOrError> {
      @Override
      public Deferred<List<WriteStatus>> call(final IdOrError tag_ioe) throws Exception {
        WriteStatus status = null;
        switch (tag_ioe.state()) {
          case RETRY:
            status = WriteStatus.retry(tag_ioe.error());
            break;
          case REJECTED:
            status = WriteStatus.rejected(tag_ioe.error());
            break;
          case ERROR:
            status = WriteStatus.error(tag_ioe.error(), tag_ioe.exception());
            break;
          case OK:
            break;
          default:
            throw new StorageException("Unexpected resolution state: "
                    + tag_ioe.state());
        }

        if (status != null) {
          List<WriteStatus> statuses = Lists.newArrayList();
          for (int i = 0; i < data.size(); i++) {
            statuses.set(i, status);
          }
          return Deferred.fromResult(statuses);
        }

        // good so we have a tag set!
        final List<Deferred<WriteStatus>> deferreds =
                Lists.newArrayListWithCapacity(data.size());
        deferreds.add(schema.createRowMetric(state,
                (TimeSeriesDatumStringId) datum.id(), child)
                .addCallbackDeferring(new MetricCB(datum, tag_ioe))
                .addErrback(new WriteErrorCB(child)));
        int index = 1;
        while (iterator.hasNext()) {
          TimeSeriesDatum seriesDatum = iterator.next();
          deferreds.add(schema.createRowMetric(state,
                  (TimeSeriesDatumStringId) seriesDatum.id(), span)
                  .addCallbackDeferring(new MetricCB(seriesDatum, tag_ioe))
                  .addErrback(new WriteErrorCB(child)));
        }
        return Deferred.group(deferreds).addCallback(new GroupCB());
      }
    }

    return schema.createRowTags(state,
            ((TimeSeriesDatumStringId) datum.id()).metric(),
            ((TimeSeriesDatumStringId) datum.id()).tags(),
            span)
            .addCallbackDeferring(new TagCB());
  }

  @Override
  public Deferred<List<WriteStatus>> write(final AuthState state,
                                           final LowLevelTimeSeriesData data,
                                           final Span span) {
    final Span child;
    if (span != null && span.isDebug()) {
      child = span.newChild(getClass().getName() + ".write")
              .start();
    } else {
      child = null;
    }

    if (!(data instanceof LowLevelMetricData)) {
      if (child != null) {
        child.finish();
      }
      return Deferred.fromError(new UnsupportedOperationException(
              "Not supporting instances of " + data.getClass() + " at this time."));
    }

    if (!data.advance()) {
      if (child != null) {
        child.finish();
      }
      return Deferred.fromResult(Collections.emptyList());
    }

    long temp_time = data.timestamp().epoch();
    temp_time = temp_time - (temp_time % Schema.MAX_RAW_TIMESPAN);
    final int base_timestamp = (int) temp_time;

    // TODO - it's possible the data will NOT have shared tags. Handle that case.
    final Map<String, String> tags = Maps.newHashMap();
    while (data.advanceTagPair()) {
      String tag_key = new String(data.tagsBuffer(),
              data.tagKeyStart(), data.tagKeyLength(), Const.UTF8_CHARSET);
      String tag_value = new String(data.tagsBuffer(),
              data.tagValueStart(), data.tagValueLength(), Const.UTF8_CHARSET);
      tags.put(tag_key, tag_value);
    }

    class MetricCB implements Callback<Deferred<WriteStatus>, IdOrError> {
      boolean is_int;
      long long_value;
      double double_value;
      final IdOrError tag_ioe;

      MetricCB(long value, final IdOrError tag_ioe) {
        this.is_int = true;
        long_value = value;
        this.tag_ioe = tag_ioe;
      }

      MetricCB(double value, final IdOrError tag_ioe) {
        this.is_int = false;
        double_value = value;
        this.tag_ioe = tag_ioe;
      }

      @Override
      public Deferred<WriteStatus> call(final IdOrError metric_ioe) throws Exception {
        switch (metric_ioe.state()) {
          case RETRY:
            return Deferred.fromResult(WriteStatus.RETRY);
          case REJECTED:
            return Deferred.fromResult(WriteStatus.REJECTED);
          case ERROR:
            return Deferred.fromResult(WriteStatus.error(metric_ioe.error(),
                    metric_ioe.exception()));
        }

        // TODO - someday, re-use arrays
        boolean has_salt = schema.saltBuckets() > 0 && schema.saltWidth() > 0;
        final int len = (has_salt ? schema.saltWidth() : 0) +
                metric_ioe.id().length +
                Schema.TIMESTAMP_BYTES +
                tag_ioe.id().length;
        final byte[] key = new byte[len];
        int index = has_salt ? 1 : 0;
        System.arraycopy(metric_ioe.id(), 0, key, 0, schema.metricWidth());
        index += schema.metricWidth();
        index += Schema.TIMESTAMP_BYTES;
        System.arraycopy(tag_ioe.id(), 0, key, index, tag_ioe.id().length);
        schema.setBaseTime(key, base_timestamp);
        if (has_salt) {
          schema.prefixKeyWithSalt(key);
        }

        MutableNumericValue mutable = TL_NUMERIC_TYPES.get();
        if (is_int) {
          mutable.reset(data.timestamp(), long_value);
        } else {
          mutable.reset(data.timestamp(), double_value);
        }

        final Codec codec = schema.getEncoder(mutable.value().type());
        if (codec == null) {
          return Deferred.fromResult(WriteStatus.error("No codec for type: "
                  + mutable.value().type(), null));
        }
        WriteStatus status = codec.encode(mutable,
                write_appends || encode_as_appends, (int) base_timestamp, null);
        if (status.state() != WriteStatus.WriteState.OK) {
          return Deferred.fromResult(status);
        }

        if (write_appends) {
          return writeAppend(key,
                  Arrays.copyOf(codec.qualifiers()[0], codec.qualifierLengths()[0]),
                  Arrays.copyOf(codec.values()[0], codec.valueLengths()[0]),
                  child);
//          return client.append(new AppendRequest(data_table, key,
//                  DATA_FAMILY,
//                  Arrays.copyOf(codec.qualifiers()[0], codec.qualifierLengths()[0]),
//                  Arrays.copyOf(codec.values()[0], codec.valueLengths()[0])))
//                  .addCallbacks(new SuccessCB(child), new WriteErrorCB(child));
        } else {
          // same for co-proc and puts. The encode method figures out
          // the qualifier and values.
          return write(key,
                  Arrays.copyOf(codec.qualifiers()[0], codec.qualifierLengths()[0]),
                  Arrays.copyOf(codec.values()[0], codec.valueLengths()[0]),
                  child);
//          return client.put(new PutRequest(data_table, key,
//                  DATA_FAMILY,
//                  Arrays.copyOf(codec.qualifiers()[0], codec.qualifierLengths()[0]),
//                  Arrays.copyOf(codec.values()[0], codec.valueLengths()[0])))
//                  .addCallbacks(new SuccessCB(child), new WriteErrorCB(child));
        }
      }
    }

    class TagCB implements Callback<Deferred<List<WriteStatus>>, IdOrError> {
      @Override
      public Deferred<List<WriteStatus>> call(final IdOrError tag_ioe) throws Exception {
        WriteStatus status = null;
        switch (tag_ioe.state()) {
          case RETRY:
            status = WriteStatus.retry(tag_ioe.error());
            break;
          case REJECTED:
            status = WriteStatus.rejected(tag_ioe.error());
            break;
          case ERROR:
            status = WriteStatus.error(tag_ioe.error(), tag_ioe.exception());
            break;
          case OK:
            break;
          default:
            throw new StorageException("Unexpected resolution state: "
                    + tag_ioe.state());
        }

        if (status != null) {
          List<WriteStatus> statuses = Lists.newArrayList(status);
          while (data.advance()) {
            statuses.add(status);
          }
          return Deferred.fromResult(statuses);
        }

        // good so we have a tag set!
        final List<Deferred<WriteStatus>> deferreds = Lists.newArrayList();
        Deferred deferred = schema.createRowMetric(state,
                new String(((LowLevelMetricData) data).metricBuffer(),
                        ((LowLevelMetricData) data).metricStart(),
                        ((LowLevelMetricData) data).metricLength(),
                        Const.UTF8_CHARSET),
                tags,
                child);
        switch (((LowLevelMetricData) data).valueFormat()) {
          case DOUBLE:
            deferred.addCallbackDeferring(new MetricCB(
                    ((LowLevelMetricData) data).doubleValue(), tag_ioe));
            break;
          case FLOAT:
            deferred.addCallbackDeferring(new MetricCB(
                    ((LowLevelMetricData) data).floatValue(), tag_ioe));
          case INTEGER:
            deferred.addCallbackDeferring(new MetricCB(
                    ((LowLevelMetricData) data).longValue(), tag_ioe));
        }
        deferreds.add(deferred.addErrback(new WriteErrorCB(child)));
        int index = 1;
        while (data.advance()) {
          deferred = schema.createRowMetric(state,
                  new String(((LowLevelMetricData) data).metricBuffer(),
                          ((LowLevelMetricData) data).metricStart(),
                          ((LowLevelMetricData) data).metricLength(),
                          Const.UTF8_CHARSET),
                  tags,
                  child);
          switch (((LowLevelMetricData) data).valueFormat()) {
            case DOUBLE:
              deferred.addCallbackDeferring(new MetricCB(
                      ((LowLevelMetricData) data).doubleValue(), tag_ioe));
              break;
            case FLOAT:
              deferred.addCallbackDeferring(new MetricCB(
                      ((LowLevelMetricData) data).floatValue(), tag_ioe));
            case INTEGER:
              deferred.addCallbackDeferring(new MetricCB(
                      ((LowLevelMetricData) data).longValue(), tag_ioe));
          }
          deferreds.add(deferred.addErrback(new WriteErrorCB(child)));
        }
        return Deferred.groupInOrder(deferreds).addCallback(new GroupCB());
      }
    }

    return schema.createRowTags(state,
            new String(((LowLevelMetricData) data).metricBuffer(),
                    ((LowLevelMetricData) data).metricStart(),
                    ((LowLevelMetricData) data).metricLength(),
                    Const.UTF8_CHARSET),
            tags,
            span)
            .addCallbackDeferring(new TagCB());
  }

  protected class SuccessCB implements Callback<WriteStatus, Object> {
    final Span child;

    public SuccessCB(final Span child) {
      this.child = child;
    }

    @Override
    public WriteStatus call(final Object ignored) throws Exception {
      if (child != null) {
        child.setSuccessTags().finish();
      }
      return WriteStatus.OK;
    }
  }

  protected class WriteErrorCB implements Callback<WriteStatus, Exception> {
    final Span child;

    public WriteErrorCB(final Span child) {
      this.child = child;
    }

    @Override
    public WriteStatus call(final Exception ex) throws Exception {
      // TODO log?
      if (retryExceptions != null && retryExceptions.contains(ex.getClass())) {
        if (child != null) {
          child.setErrorTags(ex)
                  .finish();
        }
        return WriteStatus.RETRY;
      }
      if (child != null) {
        child.setErrorTags(ex)
                .finish();
      }
      // TODO - watch out, garbage here.
      return WriteStatus.error(ex.getMessage(), ex);
    }
  }

  protected class GroupCB implements Callback<List<WriteStatus>, ArrayList<WriteStatus>> {
    @Override
    public List<WriteStatus> call(final ArrayList<WriteStatus> results) throws Exception {
      return results;
    }

  }

  protected abstract Deferred<WriteStatus> write(final byte[] key,
                                                 final byte[] qualifier,
                                                 final byte[] value,
                                                 final Span span);

  protected abstract Deferred<WriteStatus> writeAppend(final byte[] key,
                                                       final byte[] qualifier,
                                                       final byte[] value,
                                                       final Span span);
}

// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.meta;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.opentsdb.core.TSDB;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueId.UniqueIdType;
import net.opentsdb.utils.JSON;
import net.opentsdb.utils.JSONException;

import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.Bytes;
import org.hbase.async.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseException;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonGenerator;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

/**
 * Timeseries Metadata is associated with a particular series of data points
 * and includes user configurable values and some stats calculated by OpenTSDB.
 * Whenever a new timeseries is recorded, an associated TSMeta object will
 * be stored with only the tsuid field configured. These meta objects may then
 * be used to determine what combinations of metrics and tags exist in the
 * system.
 * <p>
 * When you call {@link #syncToStorage} on this object, it will verify that the
 * associated UID objects this meta data is linked with still exist. Then it 
 * will fetch the existing data and copy changes, overwriting the user fields if
 * specific (e.g. via a PUT command). If overwriting is not called for (e.g. a 
 * POST was issued), then only the fields provided by the user will be saved, 
 * preserving all of the other fields in storage. Hence the need for the 
 * {@code changed} hash map and the {@link #syncMeta} method. 
 * <p>
 * The metric and tag UIDMeta objects may be loaded from their respective 
 * locations in the data storage system if requested. Note that this will cause
 * at least 3 extra storage calls when loading.
 * @since 2.0
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = Visibility.PUBLIC_ONLY)
public class TSMeta {
  private static final Logger LOG = LoggerFactory.getLogger(TSMeta.class);

  /** Charset used to convert Strings to byte arrays and back. */
  private static final Charset CHARSET = Charset.forName("ISO-8859-1");
  
  /** The single column family used by this class. */
  public static final byte[] FAMILY = "name".getBytes(CHARSET);
  
  /** The cell qualifier to use for timeseries meta */
  private static final byte[] META_QUALIFIER = "ts_meta".getBytes(CHARSET);
  
  /** The cell qualifier to use for timeseries meta */
  private static final byte[] COUNTER_QUALIFIER = "ts_ctr".getBytes(CHARSET);
  
  /** Hexadecimal representation of the TSUID this metadata is associated with */
  private String tsuid = "";
  
  /** The metric associated with this timeseries */
  private UIDMeta metric = null;
  
  /** A list of tagk/tagv pairs of UIDMetadata associated with this timeseries */
  private ArrayList<UIDMeta> tags = null;
  
  /** An optional, user supplied descriptive name */
  private String display_name = "";
  
  /** An optional short description of the timeseries */
  private String description = "";
  
  /** Optional detailed notes about the timeseries */
  private String notes = "";
  
  /** A timestamp of when this timeseries was first recorded in seconds */
  private long created = 0;
  
  /** Optional user supplied key/values */
  private HashMap<String, String> custom = null;
  
  /** An optional field recording the units of data in this timeseries */
  private String units = "";
  
  /** An optional field used to record the type of data, e.g. counter, gauge */
  private String data_type = "";
  
  /** How long to keep raw data in this timeseries */
  private int retention = 0;
  
  /** 
   * A user defined maximum value for this timeseries, can be used to 
   * calculate percentages
   */
  private double max = Double.NaN;
  
  /** 
   * A user defined minimum value for this timeseries, can be used to 
   * calculate percentages
   */
  private double min = Double.NaN; 
  
  /** The last time this data was recorded in seconds */
  private long last_received = 0;
  
  /** The total number of data points recorded since meta has been enabled */
  private long total_dps;

  /** Tracks fields that have changed by the user to avoid overwrites */
  private final HashMap<String, Boolean> changed = 
    new HashMap<String, Boolean>();

  /**
   * Default constructor necessary for POJO de/serialization
   */
  public TSMeta() {
    initializeChangedMap();
  }
  
  /**
   * Constructor for RPC timeseries parsing that will not set the timestamps
   * @param tsuid The UID of the timeseries
   */
  public TSMeta(final String tsuid) {
    this.tsuid = tsuid;
    initializeChangedMap();
  }
  
  /**
   * Constructor for new timeseries that initializes the created and 
   * last_received times to the current system time
   * @param tsuid The UID of the timeseries
   */
  public TSMeta(final byte[] tsuid, final long created) {
    this.tsuid = UniqueId.uidToString(tsuid);
    // downgrade to seconds
    this.created = created > 9999999999L ? created / 1000 : created;
    initializeChangedMap();
    changed.put("created", true);
  }
  
  /** @return a string with details about this object */
  @Override
  public String toString() {
    return tsuid;
  }
  
  /**
   * Attempts to delete the meta object from storage
   * @param tsdb The TSDB to use for access to storage
   * @return A deferred without meaning. The response may be null and should
   * only be used to track completion.
   * @throws HBaseException if there was an issue
   * @throws IllegalArgumentException if data was missing (uid and type)
   */
  public Deferred<Object> delete(final TSDB tsdb) {
    if (tsuid == null || tsuid.isEmpty()) {
      throw new IllegalArgumentException("Missing UID");
    }

    final DeleteRequest delete = new DeleteRequest(tsdb.metaTable(), 
        UniqueId.stringToUid(tsuid), FAMILY, META_QUALIFIER);
    return tsdb.getClient().delete(delete);
  }
  
  /**
   * Attempts a CompareAndSet storage call, loading the object from storage, 
   * synchronizing changes, and attempting a put. Also verifies that associated 
   * UID name mappings exist before merging.
   * <b>Note:</b> If the local object didn't have any fields set by the caller
   * or there weren't any changes, then the data will not be written and an 
   * exception will be thrown.
   * <b>Note:</b> We do not store the UIDMeta information with TSMeta's since
   * users may change a single UIDMeta object and we don't want to update every
   * TSUID that includes that object with the new data. Instead, UIDMetas are
   * merged into the TSMeta on retrieval so we always have canonical data. This
   * also saves space in storage. 
   * @param tsdb The TSDB to use for storage access
   * @param overwrite When the RPC method is PUT, will overwrite all user
   * accessible fields
   * @return True if the storage call was successful, false if the object was
   * modified in storage during the CAS call. If false, retry the call. Other 
   * failures will result in an exception being thrown.
   * @throws HBaseException if there was an issue
   * @throws IllegalArgumentException if parsing failed
   * @throws NoSuchUniqueId If any of the UID name mappings do not exist
   * @throws IllegalStateException if the data hasn't changed. This is OK!
   * @throws JSONException if the object could not be serialized
   */
  public Deferred<Boolean> syncToStorage(final TSDB tsdb, 
      final boolean overwrite) {
    if (tsuid == null || tsuid.isEmpty()) {
      throw new IllegalArgumentException("Missing TSUID");
    }
    
    boolean has_changes = false;
    for (Map.Entry<String, Boolean> entry : changed.entrySet()) {
      if (entry.getValue()) {
        has_changes = true;
        break;
      }
    }
    if (!has_changes) {
      LOG.debug(this + " does not have changes, skipping sync to storage");
      throw new IllegalStateException("No changes detected in TSUID meta data");
    }

    /**
     * Callback used to verify that the UID name mappings exist. We don't need
     * to process the actual name, we just want it to throw an error if any
     * of the UIDs don't exist.
     */
    class UidCB implements Callback<Object, String> {

      @Override
      public Object call(String name) throws Exception {
        // nothing to do as missing mappings will throw a NoSuchUniqueId
        return null;
      }
      
    }
    
    // parse out the tags from the tsuid
    final List<byte[]> parsed_tags = UniqueId.getTagsFromTSUID(tsuid);
    
    // Deferred group used to accumulate UidCB callbacks so the next call
    // can wait until all of the UIDs have been verified
    ArrayList<Deferred<Object>> uid_group = 
      new ArrayList<Deferred<Object>>(parsed_tags.size() + 1);
    
    // calculate the metric UID and fetch it's name mapping
    final byte[] metric_uid = UniqueId.stringToUid(
        tsuid.substring(0, TSDB.metrics_width() * 2));
    uid_group.add(tsdb.getUidName(UniqueIdType.METRIC, metric_uid)
        .addCallback(new UidCB()));
    
    int idx = 0;
    for (byte[] tag : parsed_tags) {
      if (idx % 2 == 0) {
        uid_group.add(tsdb.getUidName(UniqueIdType.TAGK, tag)
            .addCallback(new UidCB()));
      } else {
        uid_group.add(tsdb.getUidName(UniqueIdType.TAGV, tag)
            .addCallback(new UidCB()));
      }
      idx++;
    }
    
    /**
     * Callback executed after all of the UID mappings have been verified. This
     * will then proceed with the CAS call.
     */
    final class ValidateCB implements Callback<Deferred<Boolean>, 
      ArrayList<Object>> {
      private final TSMeta local_meta;
      
      public ValidateCB(final TSMeta local_meta) {
        this.local_meta = local_meta;
      }
      
      /**
       * Nested class that executes the CAS after retrieving existing TSMeta
       * from storage.
       */
      final class StoreCB implements Callback<Deferred<Boolean>, TSMeta> {

        /**
         * Executes the CAS if the TSMeta was successfully retrieved
         * @return True if the CAS was successful, false if the stored data
         * was modified in flight
         * @throws IllegalArgumentException if the TSMeta did not exist in
         * storage. Only the TSD should be able to create TSMeta objects.
         */
        @Override
        public Deferred<Boolean> call(TSMeta stored_meta) throws Exception {
          if (stored_meta == null) {
            throw new IllegalArgumentException("Requested TSMeta did not exist");
          }
          
          final byte[] original_meta = stored_meta.getStorageJSON();
          local_meta.syncMeta(stored_meta, overwrite);

          final PutRequest put = new PutRequest(tsdb.metaTable(), 
              UniqueId.stringToUid(local_meta.tsuid), FAMILY, META_QUALIFIER, 
              local_meta.getStorageJSON());

          return tsdb.getClient().compareAndSet(put, original_meta);
        }
        
      }
      
      /**
       * Called on UID mapping verification and continues executing the CAS 
       * procedure.
       * @return Results from the {@link #StoreCB} callback
       */
      @Override
      public Deferred<Boolean> call(ArrayList<Object> validated) 
        throws Exception {
        return getFromStorage(tsdb, UniqueId.stringToUid(tsuid))
          .addCallbackDeferring(new StoreCB());
      }
      
    }
    
    // Begins the callback chain by validating that the UID mappings exist
    return Deferred.group(uid_group).addCallbackDeferring(new ValidateCB(this));
  }
  
  /**
   * Attempts to store a new, blank timeseries meta object
   * <b>Note:</b> This should not be called by user accessible methods as it will 
   * overwrite any data already in the column.
   * <b>Note:</b> This call does not guarantee that the UIDs exist before
   * storing as it should only be called *after* a data point has been recorded
   * or during a meta sync. 
   * @param tsdb The TSDB to use for storage access
   * @return True if the TSMeta created(or updated) successfully
   * @throws HBaseException if there was an issue fetching
   * @throws IllegalArgumentException if parsing failed
   * @throws JSONException if the object could not be serialized
   */
  public Deferred<Boolean> storeNew(final TSDB tsdb) {
    if (tsuid == null || tsuid.isEmpty()) {
      throw new IllegalArgumentException("Missing TSUID");
    }

    final PutRequest put = new PutRequest(tsdb.metaTable(), 
        UniqueId.stringToUid(tsuid), FAMILY, META_QUALIFIER, getStorageJSON());
    
    final class PutCB implements Callback<Deferred<Boolean>, Object> {
      @Override
      public Deferred<Boolean> call(Object arg0) throws Exception {
        return Deferred.fromResult(true);
      }      
    }
    
    return tsdb.getClient().put(put).addCallbackDeferring(new PutCB());
  }
  
  /**
   * Attempts to fetch the timeseries meta data and associated UIDMeta objects
   * from storage.
   * <b>Note:</b> Until we have a caching layer implemented, this will make at
   * least 4 reads to the storage system, 1 for the TSUID meta, 1 for the 
   * metric UIDMeta and 1 each for every tagk/tagv UIDMeta object.
   * <p>
   * See {@link #getFromStorage(TSDB, byte[])} for details.
   * @param tsdb The TSDB to use for storage access
   * @param tsuid The UID of the meta to fetch
   * @return A TSMeta object if found, null if not
   * @throws HBaseException if there was an issue fetching
   * @throws IllegalArgumentException if parsing failed
   * @throws JSONException if the data was corrupted
   * @throws NoSuchUniqueName if one of the UIDMeta objects does not exist
   */
  public static Deferred<TSMeta> getTSMeta(final TSDB tsdb, final String tsuid) {
    return getFromStorage(tsdb, UniqueId.stringToUid(tsuid))
      .addCallbackDeferring(new LoadUIDs(tsdb, tsuid));
  }
  
  /**
   * Parses a TSMeta object from the given column, optionally loading the 
   * UIDMeta objects
   * @param tsdb The TSDB to use for storage access
   * @param column The KeyValue column to parse
   * @param load_uidmetas Whether or not UIDmeta objects should be loaded
   * @return A TSMeta if parsed successfully
   * @throws NoSuchUniqueName if one of the UIDMeta objects does not exist
   * @throws JSONException if the data was corrupted
   */
  public static Deferred<TSMeta> parseFromColumn(final TSDB tsdb, 
      final KeyValue column, final boolean load_uidmetas) {
    if (column.value() == null || column.value().length < 1) {
      throw new IllegalArgumentException("Empty column value");
    }

    final TSMeta parsed_meta = JSON.parseToObject(column.value(), TSMeta.class);
    
    // fix in case the tsuid is missing
    if (parsed_meta.tsuid == null || parsed_meta.tsuid.isEmpty()) {
      parsed_meta.tsuid = UniqueId.uidToString(column.key());
    }

    Deferred<TSMeta> meta = getFromStorage(tsdb, UniqueId.stringToUid(parsed_meta.tsuid));
    
    if (!load_uidmetas) {
      return meta;
    }
    
    return meta.addCallbackDeferring(new LoadUIDs(tsdb, parsed_meta.tsuid));
  }
  
  /**
   * Determines if an entry exists in storage or not. 
   * This is used by the UID Manager tool to determine if we need to write a 
   * new TSUID entry or not. It will not attempt to verify if the stored data is 
   * valid, just checks to see if something is stored in the proper column.
   * @param tsdb  The TSDB to use for storage access
   * @param tsuid The UID of the meta to verify
   * @return True if data was found, false if not
   * @throws HBaseException if there was an issue fetching
   */
  public static Deferred<Boolean> metaExistsInStorage(final TSDB tsdb, 
      final String tsuid) {
    final GetRequest get = new GetRequest(tsdb.metaTable(), 
        UniqueId.stringToUid(tsuid));
    get.family(FAMILY);
    get.qualifier(META_QUALIFIER);
    
    /**
     * Callback from the GetRequest that simply determines if the row is empty
     * or not
     */
    final class ExistsCB implements Callback<Boolean, ArrayList<KeyValue>> {

      @Override
      public Boolean call(ArrayList<KeyValue> row) throws Exception {
        if (row == null || row.isEmpty() || row.get(0).value() == null) {
          return false;
        }
        return true;
      }
      
    }
    
    return tsdb.getClient().get(get).addCallback(new ExistsCB());
  }
  
  /**
   * Determines if the counter column exists for the TSUID. 
   * This is used by the UID Manager tool to determine if we need to write a 
   * new TSUID entry or not. It will not attempt to verify if the stored data is 
   * valid, just checks to see if something is stored in the proper column.
   * @param tsdb The TSDB to use for storage access
   * @param tsuid The UID of the meta to verify
   * @return True if data was found, false if not
   * @throws HBaseException if there was an issue fetching
   */
  public static Deferred<Boolean> counterExistsInStorage(final TSDB tsdb, 
      final byte[] tsuid) {
    final GetRequest get = new GetRequest(tsdb.metaTable(), tsuid);
    get.family(FAMILY);
    get.qualifier(COUNTER_QUALIFIER);
    
    /**
     * Callback from the GetRequest that simply determines if the row is empty
     * or not
     */
    final class ExistsCB implements Callback<Boolean, ArrayList<KeyValue>> {

      @Override
      public Boolean call(ArrayList<KeyValue> row) throws Exception {
        if (row == null || row.isEmpty() || row.get(0).value() == null) {
          return false;
        }
        return true;
      }
      
    }
    
    return tsdb.getClient().get(get).addCallback(new ExistsCB());
  }
  
  /**
   * Increments the tsuid datapoint counter or creates a new counter. Also
   * creates a new meta data entry if the counter did not exist.
   * <b>Note:</b> This method also:
   * <ul><li>Passes the new TSMeta object to the Search plugin after loading 
   * UIDMeta objects</li>
   * <li>Passes the new TSMeta through all configured trees if enabled</li></ul>
   * @param tsdb The TSDB to use for storage access
   * @param tsuid The TSUID to increment or create
   * @return 0 if the put failed, a positive LONG if the put was successful
   * @throws HBaseException if there was a storage issue
   * @throws JSONException if the data was corrupted
   * @throws NoSuchUniqueName if one of the UIDMeta objects does not exist
   */
  public static Deferred<Long> incrementAndGetCounter(final TSDB tsdb, 
      final byte[] tsuid) {
    
    /**
     * Callback that will create a new TSMeta if the increment result is 1 or
     * will simply return the new value.
     */
    final class TSMetaCB implements Callback<Deferred<Long>, Long> {

      /**
       * Called after incrementing the counter and will create a new TSMeta if
       * the returned value was 1 as well as pass the new meta through trees
       * and the search indexer if configured.
       * @return 0 if the put failed, a positive LONG if the put was successful
       */
      @Override
      public Deferred<Long> call(final Long incremented_value) 
        throws Exception {
        LOG.debug("Value: " + incremented_value);
        if (incremented_value > 1) {
          // TODO - maybe update the search index every X number of increments?
          // Otherwise the search engine would only get last_updated/count 
          // whenever the user runs the full sync CLI
          return Deferred.fromResult(incremented_value);
        }
        
        // create a new meta object with the current system timestamp. Ideally
        // we would want the data point's timestamp, but that's much more data
        // to keep track of and may not be accurate.
        final TSMeta meta = new TSMeta(tsuid, 
            System.currentTimeMillis() / 1000);
        
        /**
         * Called after the meta has been passed through tree processing. The 
         * result of the processing doesn't matter and the user may not even
         * have it enabled, so we'll just return the counter.
         */
        final class TreeCB implements Callback<Deferred<Long>, Boolean> {

          @Override
          public Deferred<Long> call(Boolean success) throws Exception {
            return Deferred.fromResult(incremented_value);
          }
          
        }
        
        /**
         * Called after retrieving the newly stored TSMeta and loading
         * associated UIDMeta objects. This class will also pass the meta to the
         * search plugin and run it through any configured trees
         */
        final class FetchNewCB implements Callback<Deferred<Long>, TSMeta> {

          @Override
          public Deferred<Long> call(TSMeta stored_meta) throws Exception {
            
            // pass to the search plugin
            tsdb.indexTSMeta(stored_meta);
            
            // pass through the trees
            return tsdb.processTSMetaThroughTrees(stored_meta)
              .addCallbackDeferring(new TreeCB());
          }
          
        }
        
        /**
         * Called after the CAS to store the new TSMeta object. If the CAS
         * failed then we return immediately with a 0 for the counter value.
         * Otherwise we keep processing to load the meta and pass it on.
         */
        final class StoreNewCB implements Callback<Deferred<Long>, Boolean> {

          @Override
          public Deferred<Long> call(Boolean success) throws Exception {
            if (!success) {
              LOG.warn("Unable to save metadata: " + meta);
              return Deferred.fromResult(0L);
            }
            
            LOG.info("Successfullly created new TSUID entry for: " + meta);
            return new LoadUIDs(tsdb, UniqueId.uidToString(tsuid)).call(meta)
                    .addCallbackDeferring(new FetchNewCB());
          }
          
        }
        
        // store the new TSMeta object and setup the callback chain
        return meta.storeNew(tsdb).addCallbackDeferring(new StoreNewCB());          
      }
      
    }

    // setup the increment request and execute
    final AtomicIncrementRequest inc = new AtomicIncrementRequest(
        tsdb.metaTable(), tsuid, FAMILY, COUNTER_QUALIFIER);
    // if the user has disabled real time TSMeta tracking (due to OOM issues)
    // then we only want to increment the data point count.
    if (!tsdb.getConfig().enable_realtime_ts()) {
      return tsdb.getClient().atomicIncrement(inc);
    }
    return tsdb.getClient().atomicIncrement(inc).addCallbackDeferring(
        new TSMetaCB());
  }

  /**
   * Attempts to fetch the meta column and if null, attempts to write a new 
   * column using {@link #storeNew}.
   * @param tsdb The TSDB instance to use for access.
   * @param tsuid The TSUID of the time series.
   * @return A deferred with a true if the meta exists or was created, false
   * if the meta did not exist and writing failed.
   */
  public static Deferred<Boolean> storeIfNecessary(final TSDB tsdb, 
      final byte[] tsuid) {
    final GetRequest get = new GetRequest(tsdb.metaTable(), tsuid);
    get.family(FAMILY);
    get.qualifier(META_QUALIFIER);

    final class CreateNewCB implements Callback<Deferred<Boolean>, Object> {

      @Override
      public Deferred<Boolean> call(Object arg0) throws Exception {
        final TSMeta meta = new TSMeta(tsuid, System.currentTimeMillis() / 1000);

        final class FetchNewCB implements Callback<Deferred<Boolean>, TSMeta> {

          @Override
          public Deferred<Boolean> call(TSMeta stored_meta) throws Exception {

            // pass to the search plugin
            tsdb.indexTSMeta(stored_meta);

            // pass through the trees
            tsdb.processTSMetaThroughTrees(stored_meta);

            return Deferred.fromResult(true);
          }
        }

        final class StoreNewCB implements Callback<Deferred<Boolean>, Boolean> {

          @Override
          public Deferred<Boolean> call(Boolean success) throws Exception {
            if (!success) {
              LOG.warn("Unable to save metadata: " + meta);
              return Deferred.fromResult(false);
            }

            LOG.info("Successfullly created new TSUID entry for: " + meta);
            return new LoadUIDs(tsdb, UniqueId.uidToString(tsuid)).call(meta)
                    .addCallbackDeferring(new FetchNewCB());
          }
        }

        return meta.storeNew(tsdb).addCallbackDeferring(new StoreNewCB());
      }
    }

    final class ExistsCB implements Callback<Deferred<Boolean>, ArrayList<KeyValue>> {

      @Override
      public Deferred<Boolean> call(ArrayList<KeyValue> row) throws Exception {
        if (row == null || row.isEmpty() || row.get(0).value() == null) {
          return new CreateNewCB().call(null);
        }
        return Deferred.fromResult(true);
      }
    }

    return tsdb.getClient().get(get).addCallbackDeferring(new ExistsCB());
  }
  
  /**
   * Attempts to fetch the timeseries meta data from storage. 
   * This method will fetch the {@code counter} and {@code meta} columns.
   * <b>Note:</b> This method will not load the UIDMeta objects.
   * @param tsdb The TSDB to use for storage access
   * @param tsuid The UID of the meta to fetch
   * @return A TSMeta object if found, null if not
   * @throws HBaseException if there was an issue fetching
   * @throws IllegalArgumentException if parsing failed
   * @throws JSONException if the data was corrupted
   */
  private static Deferred<TSMeta> getFromStorage(final TSDB tsdb, 
      final byte[] tsuid) {
    
    /**
     * Called after executing the GetRequest to parse the meta data.
     */
    final class GetCB implements Callback<Deferred<TSMeta>, ArrayList<KeyValue>> {

      /**
       * @return Null if the meta did not exist or a valid TSMeta object if it
       * did.
       */
      @Override
      public Deferred<TSMeta> call(final ArrayList<KeyValue> row) throws Exception {
        if (row == null || row.isEmpty()) {
          return Deferred.fromResult(null);
        }
        
        long dps = 0;
        long last_received = 0;
        TSMeta meta = null;
        
        for (KeyValue column : row) {
          if (Arrays.equals(COUNTER_QUALIFIER, column.qualifier())) {
            dps = Bytes.getLong(column.value());
            last_received = column.timestamp() / 1000;
          } else if (Arrays.equals(META_QUALIFIER, column.qualifier())) {
            meta = JSON.parseToObject(column.value(), TSMeta.class);
          }
        }
        
        if (meta == null) {
          LOG.warn("Found a counter TSMeta column without a meta for TSUID: " + 
              UniqueId.uidToString(row.get(0).key()));
          return Deferred.fromResult(null);
        }
        
        meta.total_dps = dps;
        meta.last_received = last_received;
        return Deferred.fromResult(meta);
      }
      
    }
    
    final GetRequest get = new GetRequest(tsdb.metaTable(), tsuid);
    get.family(FAMILY);
    get.qualifiers(new byte[][] { COUNTER_QUALIFIER, META_QUALIFIER });
    return tsdb.getClient().get(get).addCallbackDeferring(new GetCB());
  }
  
  /** @return The configured meta data column qualifier byte array*/
  public static byte[] META_QUALIFIER() {
    return META_QUALIFIER;
  }
  
  /** @return The configured counter column qualifier byte array*/
  public static byte[] COUNTER_QUALIFIER() {
    return COUNTER_QUALIFIER;
  }
  
  /** @return The configured meta data family byte array*/
  public static byte[] FAMILY() {
    return FAMILY;
  }
  
  /**
   * Syncs the local object with the stored object for atomic writes, 
   * overwriting the stored data if the user issued a PUT request
   * <b>Note:</b> This method also resets the {@code changed} map to false
   * for every field
   * @param meta The stored object to sync from
   * @param overwrite Whether or not all user mutable data in storage should be
   * replaced by the local object
   */
  private void syncMeta(final TSMeta meta, final boolean overwrite) {
    // storage *could* have a missing TSUID if something went pear shaped so
    // only use the one that's configured. If the local is missing, we're foobar
    if (meta.tsuid != null && !meta.tsuid.isEmpty()) {
      tsuid = meta.tsuid;
    }
    if (tsuid == null || tsuid.isEmpty()) {
      throw new IllegalArgumentException("TSUID is empty");
    }
    if (meta.created > 0 && (meta.created < created || created == 0)) {
      created = meta.created;
    }
    
    // handle user-accessible stuff
    if (!overwrite && !changed.get("display_name")) {
      display_name = meta.display_name;
    }
    if (!overwrite && !changed.get("description")) {
      description = meta.description;
    }
    if (!overwrite && !changed.get("notes")) {
      notes = meta.notes;
    }
    if (!overwrite && !changed.get("custom")) {
      custom = meta.custom;
    }
    if (!overwrite && !changed.get("units")) {
      units = meta.units;
    }
    if (!overwrite && !changed.get("data_type")) {
      data_type = meta.data_type;
    }
    if (!overwrite && !changed.get("retention")) {
      retention = meta.retention;
    }
    if (!overwrite && !changed.get("max")) {
      max = meta.max;
    }
    if (!overwrite && !changed.get("min")) {
      min = meta.min;
    }
    
    last_received = meta.last_received;
    total_dps = meta.total_dps;
    
    // reset changed flags
    initializeChangedMap();
  }
  
  /**
   * Sets or resets the changed map flags
   */
  private void initializeChangedMap() {
    // set changed flags
    changed.put("display_name", false);
    changed.put("description", false);
    changed.put("notes", false);
    changed.put("created", false);
    changed.put("custom", false);
    changed.put("units", false);
    changed.put("data_type", false);
    changed.put("retention", false);
    changed.put("max", false);
    changed.put("min", false);
    changed.put("last_received", false);
    changed.put("created", false); 
  }
  
  /**
   * Formats the JSON output for writing to storage. It drops objects we don't
   * need or want to store (such as the UIDMeta objects or the total dps) to
   * save space. It also serializes in order so that we can make a proper CAS
   * call. Otherwise the POJO serializer may place the fields in any order
   * and CAS calls would fail all the time.
   * @return A byte array to write to storage
   */
  private byte[] getStorageJSON() {
    // 256 bytes is a good starting value, assumes default info
    final ByteArrayOutputStream output = new ByteArrayOutputStream(256);
    try {
      final JsonGenerator json = JSON.getFactory().createGenerator(output); 
      json.writeStartObject();
      json.writeStringField("tsuid", tsuid);
      json.writeStringField("displayName", display_name);
      json.writeStringField("description", description);
      json.writeStringField("notes", notes);
      json.writeNumberField("created", created);
      if (custom == null) {
        json.writeNullField("custom");
      } else {
        json.writeObjectFieldStart("custom");
        for (Map.Entry<String, String> entry : custom.entrySet()) {
          json.writeStringField(entry.getKey(), entry.getValue());
        }
        json.writeEndObject();
      }
      json.writeStringField("units", units);
      json.writeStringField("dataType", data_type);
      json.writeNumberField("retention", retention);
      json.writeNumberField("max", max);
      json.writeNumberField("min", min);
      
      json.writeEndObject(); 
      json.close();
      return output.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException("Unable to serialize TSMeta", e);
    }
  }
  
  /**
   * Asynchronously loads the UIDMeta objects into the given TSMeta object. Used
   * by multiple methods so it's broken into it's own class here.
   */
  private static class LoadUIDs implements Callback<Deferred<TSMeta>, TSMeta> {

    final private TSDB tsdb;
    final private String tsuid;
    
    public LoadUIDs(final TSDB tsdb, final String tsuid) {
      this.tsdb = tsdb;
      this.tsuid = tsuid;
    }
    
    /**
     * @return A TSMeta object loaded with UIDMetas if successful
     * @throws HBaseException if there was a storage issue
     * @throws JSONException if the data was corrupted
     * @throws NoSuchUniqueName if one of the UIDMeta objects does not exist
     */
    @Override
    public Deferred<TSMeta> call(final TSMeta meta) throws Exception {
      if (meta == null) {
        return Deferred.fromResult(null);
      }
      
      // split up the tags
      final List<byte[]> tags = UniqueId.getTagsFromTSUID(tsuid);
      meta.tags = new ArrayList<UIDMeta>(tags.size());
      
      // initialize with empty objects, otherwise the "set" operations in 
      // the callback won't work. Each UIDMeta will be given an index so that 
      // the callback can store it in the proper location
      for (int i = 0; i < tags.size(); i++) {
        meta.tags.add(new UIDMeta());
      }
      
      // list of fetch calls that we can wait on for completion
      ArrayList<Deferred<Object>> uid_group = 
        new ArrayList<Deferred<Object>>(tags.size() + 1);
      
      /**
       * Callback for each getUIDMeta request that will place the resulting 
       * meta data in the proper location. The meta should always be either an
       * actual stored value or a default. On creation, this callback will have
       * an index to associate the UIDMeta with the proper location.
       */
      final class UIDMetaCB implements Callback<Object, UIDMeta> {

        final int index;
        
        public UIDMetaCB(final int index) {
          this.index = index;
        }
        
        /**
         * @return null always since we don't care about the result, just that
         * the callback has completed.
         */
        @Override
        public Object call(final UIDMeta uid_meta) throws Exception {
          if (index < 0) {
            meta.metric = uid_meta;
          } else {
            meta.tags.set(index, uid_meta);
          }
          return null;
        }
        
      }
      
      // for the UIDMeta indexes: -1 means metric, >= 0 means tag. Each 
      // getUIDMeta request must be added to the uid_group array so that we
      // can wait for them to complete before returning the TSMeta object, 
      // otherwise the caller may get a TSMeta with missing UIDMetas
      uid_group.add(UIDMeta.getUIDMeta(tsdb, UniqueIdType.METRIC, 
        tsuid.substring(0, TSDB.metrics_width() * 2)).addCallback(
            new UIDMetaCB(-1)));
      
      int idx = 0;
      for (byte[] tag : tags) {
        if (idx % 2 == 0) {
          uid_group.add(UIDMeta.getUIDMeta(tsdb, UniqueIdType.TAGK, tag)
                .addCallback(new UIDMetaCB(idx)));
        } else {
          uid_group.add(UIDMeta.getUIDMeta(tsdb, UniqueIdType.TAGV, tag)
              .addCallback(new UIDMetaCB(idx)));
        }          
        idx++;
      }
      
      /**
       * Super simple callback that is used to wait on the group of getUIDMeta
       * deferreds so that we return only when all of the UIDMetas have been
       * loaded.
       */
      final class CollateCB implements Callback<Deferred<TSMeta>, 
        ArrayList<Object>> {

        @Override
        public Deferred<TSMeta> call(ArrayList<Object> uids) throws Exception {
          return Deferred.fromResult(meta);
        }
        
      }
      
      // start the callback chain by grouping and waiting on all of the UIDMeta
      // deferreds
      return Deferred.group(uid_group).addCallbackDeferring(new CollateCB());
    }
    
  }
  
  // Getters and Setters --------------
  
  /** @return the TSUID as a hex encoded string */
  public final String getTSUID() {
    return tsuid;
  }

  /** @return the metric UID meta object */
  public final UIDMeta getMetric() {
    return metric;
  }

  /** @return the tag UID meta objects in an array, tagk first, then tagv, etc */
  public final List<UIDMeta> getTags() {
    return tags;
  }

  /** @return optional display name */
  public final String getDisplayName() {
    return display_name;
  }

  /** @return optional description */
  public final String getDescription() {
    return description;
  }

  /** @return optional notes */
  public final String getNotes() {
    return notes;
  }

  /** @return when the TSUID was first recorded, Unix epoch */
  public final long getCreated() {
    return created;
  }

  /** @return optional custom key/value map, may be null */
  public final Map<String, String> getCustom() {
    return custom;
  }

  /** @return optional units */
  public final String getUnits() {
    return units;
  }

  /** @return optional data type */
  public final String getDataType() {
    return data_type;
  }

  /** @return optional retention, default of 0 means retain indefinitely */
  public final int getRetention() {
    return retention;
  }

  /** @return optional max value, set by the user */
  public final double getMax() {
    return max;
  }

  /** @return optional min value, set by the user */
  public final double getMin() {
    return min;
  }

  /** @return the last received timestamp, Unix epoch */
  public final long getLastReceived() {
    return last_received;
  }

  /** @return the total number of data points as tracked by the meta data */
  public final long getTotalDatapoints() {
    return this.total_dps;
  }
  
  /** @param display_name an optional name for the timeseries */
  public final void setDisplayName(final String display_name) {
    if (!this.display_name.equals(display_name)) {
      changed.put("display_name", true);
      this.display_name = display_name;
    }
  }

  /** @param description an optional description */
  public final void setDescription(final String description) {
    if (!this.description.equals(description)) {
      changed.put("description", true);
      this.description = description;
    }
  }

  /** @param notes optional notes */
  public final void setNotes(final String notes) {
    if (!this.notes.equals(notes)) {
      changed.put("notes", true);
      this.notes = notes;
    }
  }

  /** @param created the created timestamp Unix epoch in seconds */
  public final void setCreated(final long created) {
    if (this.created != created) {
      changed.put("created", true);
      this.created = created;
    }
  }
  
  /** @param tsuid The TSUID of the timeseries. */
  public final void setTSUID(final String tsuid) {
    this.tsuid = tsuid;
  }
  
  /** @param custom optional key/value map */
  public final void setCustom(final Map<String, String> custom) {
    // equivalency of maps is a pain, users have to submit the whole map
    // anyway so we'll just mark it as changed every time we have a non-null
    // value
    if (this.custom != null || custom != null) {
      changed.put("custom", true);
      this.custom = new HashMap<String, String>(custom);
    }
  }

  /** @param units optional units designation */
  public final void setUnits(final String units) {
    if (!this.units.equals(units)) {
      changed.put("units", true);
      this.units = units;
    }
  }

  /** @param data_type optional type of data, e.g. "counter", "gauge" */
  public final void setDataType(final String data_type) {
    if (!this.data_type.equals(data_type)) {
      changed.put("data_type", true);
      this.data_type = data_type;
    }
  }

  /** @param retention optional rentention in days, 0 = indefinite */
  public final void setRetention(final int retention) {
    if (this.retention != retention) {
      changed.put("retention", true);
      this.retention = retention;
    }
  }

  /** @param max optional max value for the timeseries, NaN is the default */
  public final void setMax(final double max) {
    if (this.max != max) {
      changed.put("max", true);
      this.max = max;
    }
  }

  /** @param min optional min value for the timeseries, NaN is the default */
  public final void setMin(final double min) {
    if (this.min != min) {
      changed.put("min", true);
      this.min = min;
    }
  }
}

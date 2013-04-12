// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
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

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.opentsdb.core.TSDB;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueId.UniqueIdType;
import net.opentsdb.utils.JSON;
import net.opentsdb.utils.JSONException;

import org.hbase.async.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseException;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.RowLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

/**
 * Timeseries Metadata is associated with a particular series of data points
 * and includes user configurable values and some stats calculated by OpenTSDB.
 * Whenever a new timeseries is recorded, an associated TSMeta object will
 * be recorded with only the tsuid field configured.
 * <p>
 * The metric and tag UIDMeta objects are loaded from their respective locations
 * in the data storage system.
 * @since 2.0
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_NULL)
public final class TSMeta {
  private static final Logger LOG = LoggerFactory.getLogger(TSMeta.class);

  /** Charset used to convert Strings to byte arrays and back. */
  private static final Charset CHARSET = Charset.forName("ISO-8859-1");
  
  /** The single column family used by this class. */
  private static final byte[] FAMILY = "name".getBytes(CHARSET);
  
  /** The cell qualifier to use for timeseries meta */
  private static final byte[] QUALIFIER = "ts_meta".getBytes(CHARSET);
  
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
   * last_received times
   * @param tsuid The UID of the timeseries
   */
  public TSMeta(final byte[] tsuid) {
    this.tsuid = UniqueId.uidToString(tsuid);
    created = System.currentTimeMillis() / 1000;
    last_received = created;
    initializeChangedMap();
  }

  /** @return a string with details about this object */
  @Override
  public String toString() {
    return tsuid;
  }
  
  /**
   * Attempts to delete the meta object from storage
   * @param tsdb The TSDB to use for access to storage
   * @throws HBaseException if there was an issue
   * @throws IllegalArgumentException if data was missing (uid and type)
   */
  public void delete(final TSDB tsdb) {
    if (tsuid == null || tsuid.isEmpty()) {
      throw new IllegalArgumentException("Missing UID");
    }

    final DeleteRequest delete = new DeleteRequest(tsdb.uidTable(), 
        UniqueId.stringToUid(tsuid), FAMILY, QUALIFIER);
    try {
      tsdb.getClient().delete(delete);
    } catch (Exception e) {
      throw new RuntimeException("Unable to delete UID", e);
    }
  }
  
  /**
   * Attempts an atomic write to storage, loading the object first and copying
   * any changes while holding a lock on the row. After calling, this object
   * will have data loaded from storage.
   * <b>Note:</b> If the local object didn't have any fields set by the caller
   * then the data will not be written.
   * <p>
   * <b>Note:</b> We do not store the UIDMeta information with TSMeta's since
   * users may change a single UIDMeta object and we don't want to update every
   * TSUID that includes that object with the new data. Instead, UIDMetas are
   * merged into the TSMeta on retrieval so we always have canonical data. This
   * also saves space in storage. 
   * @param tsdb The TSDB to use for storage access
   * @param overwrite When the RPC method is PUT, will overwrite all user
   * accessible fields
   * @throws HBaseException if there was an issue fetching
   * @throws IllegalArgumentException if parsing failed
   * @throws IllegalStateException if the data hasn't changed. This is OK!
   * @throws JSONException if the object could not be serialized
   */
  public void syncToStorage(final TSDB tsdb, final boolean overwrite) {
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
    
    // before proceeding, make sure each UID object exists by loading the info
    metric = UIDMeta.getUIDMeta(tsdb, UniqueIdType.METRIC, 
        tsuid.substring(0, TSDB.metrics_width() * 2));
    final List<byte[]> parsed_tags = UniqueId.getTagPairsFromTSUID(tsuid, 
        TSDB.metrics_width(), TSDB.tagk_width(), TSDB.tagv_width());
    tags = new ArrayList<UIDMeta>(parsed_tags.size());
    int idx = 0;
    for (byte[] tag : parsed_tags) {
      if (idx % 2 == 0) {
        tags.add(UIDMeta.getUIDMeta(tsdb, UniqueIdType.TAGK, tag));
      } else {
        tags.add(UIDMeta.getUIDMeta(tsdb, UniqueIdType.TAGV, tag));
      }
      idx++;
    }
    
    final RowLock lock = tsdb.hbaseAcquireLock(tsdb.uidTable(), 
        UniqueId.stringToUid(tsuid), (short)3);
    try {
      TSMeta stored_meta = 
        getFromStorage(tsdb, UniqueId.stringToUid(tsuid), lock);
      if (stored_meta != null) {
        syncMeta(stored_meta, overwrite);
      } else {
        // todo - should we prevent users from posting possibly non-existant
        // tsuid metas? 
        // throw new IllegalArgumentException("Requested TSUID did not exist");
      }

      // We don't want to store any loaded UIDMeta objects (metric or tags) here
      // since the UIDMeta's are canonical. We can't just set the fields to null
      // before storage since callers may be looking at them later. So we'll 
      // copy all fields BUT the UIDMetas and serialize those
      stored_meta = copyToStorageObject();
      final PutRequest put = new PutRequest(tsdb.uidTable(), 
          UniqueId.stringToUid(tsuid), FAMILY, QUALIFIER, 
          JSON.serializeToBytes(stored_meta), lock);
      tsdb.hbasePutWithRetry(put, (short)3, (short)800);
      
    } finally {
      // release the lock!
      try {
        tsdb.getClient().unlockRow(lock);
      } catch (HBaseException e) {
        LOG.error("Error while releasing the lock on row: " + tsuid, e);
      }
    }
  }
  
  /**
   * Attempts to fetch the timeseries meta data from storage
   * <b>Note:</b> Until we have a caching layer implemented, this will make at
   * least 4 reads to the storage system, 1 for the TSUID meta, 1 for the 
   * metric UIDMeta and 1 each for every tagk/tagv UIDMeta object.
   * @param tsdb The TSDB to use for storage access
   * @param tsuid The UID of the meta to fetch
   * @return A TSMeta object if found, null if not
   * @throws HBaseException if there was an issue fetching
   * @throws IllegalArgumentException if parsing failed
   * @throws JSONException if the data was corrupted
   * @throws NoSuchUniqueName if one of the UIDMeta objects does not exist
   */
  public static TSMeta getTSMeta(final TSDB tsdb, final String tsuid) {
    final TSMeta meta = getFromStorage(tsdb, UniqueId.stringToUid(tsuid), null);
    if (meta == null) {
      return meta;
    }

    // load each of the UIDMetas parsed from the TSUID
    meta.metric = UIDMeta.getUIDMeta(tsdb, UniqueIdType.METRIC, 
        tsuid.substring(0, TSDB.metrics_width() * 2));

    final List<byte[]> tags = UniqueId.getTagPairsFromTSUID(tsuid, 
        TSDB.metrics_width(), TSDB.tagk_width(), TSDB.tagv_width());
    meta.tags = new ArrayList<UIDMeta>(tags.size());
    int idx = 0;
    for (byte[] tag : tags) {
      if (idx % 2 == 0) {
        meta.tags.add(UIDMeta.getUIDMeta(tsdb, UniqueIdType.TAGK, tag));
      } else {
        meta.tags.add(UIDMeta.getUIDMeta(tsdb, UniqueIdType.TAGV, tag));
      }
      idx++;
    }
    return meta;
  }
  
  /**
   * Attempts to fetch the timeseries meta data from storage
   * @param tsdb The TSDB to use for storage access
   * @param tsuid The UID of the meta to fetch
   * @param lock An optional lock when performing an atomic update, pass null
   * if not needed.
   * @return A TSMeta object if found, null if not
   * @throws HBaseException if there was an issue fetching
   * @throws IllegalArgumentException if parsing failed
   * @throws JSONException if the data was corrupted
   */
  private static TSMeta getFromStorage(final TSDB tsdb, final byte[] tsuid, 
      final RowLock lock) {
    final GetRequest get = new GetRequest(tsdb.uidTable(), tsuid);
    get.family(FAMILY);
    get.qualifier(QUALIFIER);
    if (lock != null) {
      get.withRowLock(lock);
    }    
    
    try {
      final ArrayList<KeyValue> row = 
        tsdb.getClient().get(get).joinUninterruptibly();
      if (row == null || row.isEmpty()) {
        return null;
      }
      return JSON.parseToObject(row.get(0).value(), TSMeta.class);
    } catch (HBaseException e) {
      throw e;
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (JSONException e) {
        throw e;
    } catch (Exception e) {
      throw new RuntimeException("Should never be here", e);
    }
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
    // copy non-user-accessible data first
    tsuid = meta.tsuid;
    created = meta.created;
    last_received = meta.last_received;
    
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
    changed.put("custom", false);
    changed.put("units", false);
    changed.put("data_type", false);
    changed.put("retention", false);
    changed.put("max", false);
    changed.put("min", false);
    changed.put("last_received", false);
  }
  
  /**
   * Copies local values into a new TSMeta object with the UIDMeta's set to
   * null so we don't serialize that data; the UIDMetas are canonical
   * @return A TSMeta object with UIDMetas set to null
   */
  private TSMeta copyToStorageObject() {
    final TSMeta meta = new TSMeta();
    meta.tsuid = tsuid;
    meta.display_name = display_name;
    meta.description = description;
    meta.notes = notes;
    meta.created = created;
    meta.custom = custom;
    meta.units = units;
    meta.data_type = data_type;
    meta.retention = retention;
    meta.max = max;
    meta.min = min;
    meta.last_received = last_received;
    return meta;
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
  public final ArrayList<UIDMeta> getTags() {
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
  public final HashMap<String, String> getCustom() {
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

  /** @param custom optional key/value map */
  public final void setCustom(final HashMap<String, String> custom) {
    // equivalency of maps is a pain, users have to submit the whole map
    // anyway so we'll just mark it as changed every time we have a non-null
    // value
    if (this.custom != null || custom != null) {
      changed.put("custom", true);
      this.custom = custom;
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

  /** @param last_received last time a data point was recorded. Should be
   * set by the TSD only! */
  public final void setLastReceived(final long last_received) {
    if (this.last_received != last_received) {
      changed.put("last_received", true);
      this.last_received = last_received;
    }
  }
}

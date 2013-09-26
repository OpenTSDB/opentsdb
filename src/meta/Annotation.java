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

import org.hbase.async.Bytes;
import org.hbase.async.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseException;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.core.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.JSON;
import net.opentsdb.utils.JSONException;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonGenerator;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

/**
 * Annotations are used to record time-based notes about timeseries events.
 * Every note must have an associated start_time as that determines
 * where the note is stored.
 * <p>
 * Annotations may be associated with a specific timeseries, in which case
 * the tsuid must be configured with a valid TSUID. If no TSUID
 * is provided, the annotation is considered a "global" note that applies
 * to everything stored in OpenTSDB. Global annotations are stored in the rows
 * [ 0, 0, 0, &lt;timestamp&gt;] in the same manner as local annotations and
 * timeseries data.
 * <p>
 * The description field should store a very brief line of information
 * about the event. GUIs can display the description in their "main" view
 * where multiple annotations may appear. Users of the GUI could then click
 * or hover over the description for more detail including the {@link notes}
 * field.
 * <p>
 * Custom data can be stored in the custom hash map for user
 * specific information. For example, you could add a "reporter" key
 * with the name of the person who recorded the note.
 * @since 2.0
 */
@JsonAutoDetect(fieldVisibility = Visibility.PUBLIC_ONLY)
@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public final class Annotation implements Comparable<Annotation> {
  private static final Logger LOG = LoggerFactory.getLogger(Annotation.class);
  
  /** Charset used to convert Strings to byte arrays and back. */
  private static final Charset CHARSET = Charset.forName("ISO-8859-1");
  
  /** Byte used for the qualifier prefix to indicate this is an annotation */
  private static final byte PREFIX = 0x01;
    
  /** The single column family used by this class. */
  private static final byte[] FAMILY = "t".getBytes(CHARSET);
  
  /** If the note is associated with a timeseries, represents the ID */
  private String tsuid = "";
  
  /** The start timestamp associated wit this note in seconds or ms */
  private long start_time = 0;
  
  /** Optional end time if the note represents an event that was resolved */
  private long end_time = 0;
  
  /** A short description of the event, displayed in GUIs */
  private String description = "";  
  
  /** A detailed accounting of the event or note */
  private String notes = "";
  
  /** Optional user supplied key/values */
  private HashMap<String, String> custom = null;

  /** Tracks fields that have changed by the user to avoid overwrites */
  private final HashMap<String, Boolean> changed = 
    new HashMap<String, Boolean>();

  /**
   * Default constructor, initializes the change map
   */
  public Annotation() {
    initializeChangedMap();
  }
  
  /** @return A string with information about the annotation object */
  @Override
  public String toString() {
    return "TSUID: " + tsuid + " Start: " + start_time + "  Description: " + 
      description;
  }
  
  /**
   * Compares the {@code #start_time} of this annotation to the given note
   * @return 1 if the local start time is greater, -1 if it's less or 0 if
   * equal
   */
  @Override
  public int compareTo(Annotation note) {
    return start_time > note.start_time ? 1 : 
      start_time < note.start_time ? -1 : 0;
  }
  
  /**
   * Attempts a CompareAndSet storage call, loading the object from storage, 
   * synchronizing changes, and attempting a put.
   * <b>Note:</b> If the local object didn't have any fields set by the caller
   * or there weren't any changes, then the data will not be written and an 
   * exception will be thrown.
   * @param tsdb The TSDB to use for storage access
   * @param overwrite When the RPC method is PUT, will overwrite all user
   * accessible fields
   * True if the storage call was successful, false if the object was
   * modified in storage during the CAS call. If false, retry the call. Other 
   * failures will result in an exception being thrown.
   * @throws HBaseException if there was an issue
   * @throws IllegalArgumentException if required data was missing such as the 
   * {@code #start_time}
   * @throws IllegalStateException if the data hasn't changed. This is OK!
   * @throws JSONException if the object could not be serialized
   */
  public Deferred<Boolean> syncToStorage(final TSDB tsdb, 
      final Boolean overwrite) {
    if (start_time < 1) {
      throw new IllegalArgumentException("The start timestamp has not been set");
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
      throw new IllegalStateException("No changes detected in Annotation data");
    }
    
    final class StoreCB implements Callback<Deferred<Boolean>, Annotation> {

      @Override
      public Deferred<Boolean> call(final Annotation stored_note) 
        throws Exception {
        final byte[] original_note = stored_note == null ? new byte[0] :
          stored_note.getStorageJSON();
        
        if (stored_note != null) {
          Annotation.this.syncNote(stored_note, overwrite);
        }
        
        final byte[] tsuid_byte = tsuid != null && !tsuid.isEmpty() ? 
            UniqueId.stringToUid(tsuid) : null;
        final PutRequest put = new PutRequest(tsdb.dataTable(), 
            getRowKey(start_time, tsuid_byte), FAMILY, 
            getQualifier(start_time), 
            Annotation.this.getStorageJSON());
        return tsdb.getClient().compareAndSet(put, original_note);
      }
      
    }
    
    if (tsuid != null && !tsuid.isEmpty()) {
      return getAnnotation(tsdb, UniqueId.stringToUid(tsuid), start_time)
        .addCallbackDeferring(new StoreCB());
    }
    return getAnnotation(tsdb, start_time).addCallbackDeferring(new StoreCB());
  }
  
  /**
   * Attempts to mark an Annotation object for deletion. Note that if the
   * annoation does not exist in storage, this delete call will not throw an
   * error.
   * @param tsdb The TSDB to use for storage access
   * @return A meaningless Deferred for the caller to wait on until the call is
   * complete. The value may be null.
   */
  public Deferred<Object> delete(final TSDB tsdb) {
    if (start_time < 1) {
      throw new IllegalArgumentException("The start timestamp has not been set");
    }
    
    final byte[] tsuid_byte = tsuid != null && !tsuid.isEmpty() ? 
        UniqueId.stringToUid(tsuid) : null;
    final DeleteRequest delete = new DeleteRequest(tsdb.dataTable(), 
        getRowKey(start_time, tsuid_byte), FAMILY, 
        getQualifier(start_time));
    return tsdb.getClient().delete(delete);
  }
  
  /**
   * Attempts to fetch a global annotation from storage
   * @param tsdb The TSDB to use for storage access
   * @param start_time The start time as a Unix epoch timestamp
   * @return A valid annotation object if found, null if not
   */
  public static Deferred<Annotation> getAnnotation(final TSDB tsdb, 
      final long start_time) {
    return getAnnotation(tsdb, (byte[])null, start_time);
  }
  
  /**
   * Attempts to fetch a global or local annotation from storage
   * @param tsdb The TSDB to use for storage access
   * @param tsuid The TSUID as a string. May be empty if retrieving a global
   * annotation
   * @param start_time The start time as a Unix epoch timestamp
   * @return A valid annotation object if found, null if not
   */
  public static Deferred<Annotation> getAnnotation(final TSDB tsdb, 
      final String tsuid, final long start_time) {
    if (tsuid != null && !tsuid.isEmpty()) {
      return getAnnotation(tsdb, UniqueId.stringToUid(tsuid), start_time);
    }
    return getAnnotation(tsdb, (byte[])null, start_time);
  }
  
  /**
   * Attempts to fetch a global or local annotation from storage
   * @param tsdb The TSDB to use for storage access
   * @param tsuid The TSUID as a byte array. May be null if retrieving a global
   * annotation
   * @param start_time The start time as a Unix epoch timestamp
   * @return A valid annotation object if found, null if not
   */
  public static Deferred<Annotation> getAnnotation(final TSDB tsdb, 
      final byte[] tsuid, final long start_time) {
    
    /**
     * Called after executing the GetRequest to parse the meta data.
     */
    final class GetCB implements Callback<Deferred<Annotation>, 
      ArrayList<KeyValue>> {

      /**
       * @return Null if the meta did not exist or a valid Annotation object if 
       * it did.
       */
      @Override
      public Deferred<Annotation> call(final ArrayList<KeyValue> row) 
        throws Exception {
        if (row == null || row.isEmpty()) {
          return Deferred.fromResult(null);
        }
        
        Annotation note = JSON.parseToObject(row.get(0).value(), 
            Annotation.class);
        return Deferred.fromResult(note);
      }
      
    }

    final GetRequest get = new GetRequest(tsdb.dataTable(), 
        getRowKey(start_time, tsuid));
    get.family(FAMILY);
    get.qualifier(getQualifier(start_time));
    return tsdb.getClient().get(get).addCallbackDeferring(new GetCB());    
  }
  
  /**
   * Scans through the global annotation storage rows and returns a list of 
   * parsed annotation objects. If no annotations were found for the given
   * timespan, the resulting list will be empty.
   * @param tsdb The TSDB to use for storage access
   * @param start_time Start time to scan from. May be 0
   * @param end_time End time to scan to. Must be greater than 0
   * @return A list with detected annotations. May be empty.
   * @throws IllegalArgumentException if the end timestamp has not been set or 
   * the end time is less than the start time
   */
  public static Deferred<List<Annotation>> getGlobalAnnotations(final TSDB tsdb, 
      final long start_time, final long end_time) {
    if (end_time < 1) {
      throw new IllegalArgumentException("The end timestamp has not been set");
    }
    if (end_time < start_time) {
      throw new IllegalArgumentException(
          "The end timestamp cannot be less than the start timestamp");
    }
    
    /**
     * Scanner that loops through the [0, 0, 0, timestamp] rows looking for
     * global annotations. Returns a list of parsed annotation objects.
     * The list may be empty.
     */
    final class ScannerCB implements Callback<Deferred<List<Annotation>>, 
      ArrayList<ArrayList<KeyValue>>> {
      final Scanner scanner;
      final ArrayList<Annotation> annotations = new ArrayList<Annotation>();
      
      /**
       * Initializes the scanner
       */
      public ScannerCB() {
        final byte[] start = new byte[TSDB.metrics_width() + 
                                      Const.TIMESTAMP_BYTES];
        final byte[] end = new byte[TSDB.metrics_width() + 
                                    Const.TIMESTAMP_BYTES];
        
        final long normalized_start = (start_time - 
            (start_time % Const.MAX_TIMESPAN));
        final long normalized_end = (end_time - 
            (end_time % Const.MAX_TIMESPAN));
        
        Bytes.setInt(start, (int) normalized_start, TSDB.metrics_width());
        Bytes.setInt(end, (int) normalized_end, TSDB.metrics_width());

        scanner = tsdb.getClient().newScanner(tsdb.dataTable());
        scanner.setStartKey(start);
        scanner.setStopKey(end);
        scanner.setFamily(FAMILY);
      }
      
      public Deferred<List<Annotation>> scan() {
        return scanner.nextRows().addCallbackDeferring(this);
      }
      
      @Override
      public Deferred<List<Annotation>> call (
          final ArrayList<ArrayList<KeyValue>> rows) throws Exception {
        if (rows == null || rows.isEmpty()) {
          return Deferred.fromResult((List<Annotation>)annotations);
        }
        
        for (final ArrayList<KeyValue> row : rows) {
          for (KeyValue column : row) {
            if (column.qualifier().length == 3 && 
                column.qualifier()[0] == PREFIX()) {
              Annotation note = JSON.parseToObject(row.get(0).value(), 
                  Annotation.class);
              if (note.start_time < start_time || note.end_time > end_time) {
                continue;
              }
              annotations.add(note);
            }
          }
        }
        
        return scan();
      }
      
    }

    return new ScannerCB().scan();
  }
  
  /** @return The prefix byte for annotation objects */
  public static byte PREFIX() {
    return PREFIX;
  }
  
  /**
   * Serializes the object in a uniform matter for storage. Needed for 
   * successful CAS calls
   * @return The serialized object as a byte array
   */
  private byte[] getStorageJSON() {
    // TODO - precalculate size
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    try {
      final JsonGenerator json = JSON.getFactory().createGenerator(output); 
      json.writeStartObject();
      if (tsuid != null && !tsuid.isEmpty()) {
        json.writeStringField("tsuid", tsuid);  
      }
      json.writeNumberField("startTime", start_time);
      json.writeNumberField("endTime", end_time);
      json.writeStringField("description", description);
      json.writeStringField("notes", notes);
      if (custom == null) {
        json.writeNullField("custom");
      } else {
        json.writeObjectFieldStart("custom");
        for (Map.Entry<String, String> entry : custom.entrySet()) {
          json.writeStringField(entry.getKey(), entry.getValue());
        }
        json.writeEndObject();
      }
      
      json.writeEndObject(); 
      json.close();
      return output.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException("Unable to serialize Annotation", e);
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
  private void syncNote(final Annotation note, final boolean overwrite) {
    if (note.start_time > 0 && (note.start_time < start_time || start_time == 0)) {
      start_time = note.start_time;
    }
    
    // handle user-accessible stuff
    if (!overwrite && !changed.get("end_time")) {
      end_time = note.end_time;
    }
    if (!overwrite && !changed.get("description")) {
      description = note.description;
    }
    if (!overwrite && !changed.get("notes")) {
      notes = note.notes;
    }
    if (!overwrite && !changed.get("custom")) {
      custom = note.custom;
    }
    
    // reset changed flags
    initializeChangedMap();
  }
  
  /**
   * Sets or resets the changed map flags
   */
  private void initializeChangedMap() {
    // set changed flags
    changed.put("end_time", false);
    changed.put("description", false);
    changed.put("notes", false);
    changed.put("custom", false);
  }
  
  /**
   * Calculates and returns the column qualifier. The qualifier is the offset
   * of the {@code #start_time} from the row key's base time stamp in seconds
   * with a prefix of {@code #PREFIX}. Thus if the offset is 0 and the prefix is
   * 1 and the timestamp is in seconds, the qualifier would be [1, 0, 0]. 
   * Millisecond timestamps will have a 5 byte qualifier
   * @return The column qualifier as a byte array
   * @throws IllegalArgumentException if the start_time has not been set
   */
  private static byte[] getQualifier(final long start_time) {
    if (start_time < 1) {
      throw new IllegalArgumentException("The start timestamp has not been set");
    }
    
    final long base_time;
    final byte[] qualifier;
    if ((start_time & Const.SECOND_MASK) != 0) {
      // drop the ms timestamp to seconds to calculate the base timestamp
      base_time = ((start_time / 1000) - 
          ((start_time / 1000) % Const.MAX_TIMESPAN));
      qualifier = new byte[5];
      final int offset = (int) (start_time - (base_time * 1000));
      System.arraycopy(Bytes.fromInt(offset), 0, qualifier, 1, 4);
    } else {
      base_time = (start_time - (start_time % Const.MAX_TIMESPAN));
      qualifier = new byte[3];
      final short offset = (short) (start_time - base_time);
      System.arraycopy(Bytes.fromShort(offset), 0, qualifier, 1, 2);
    }
    qualifier[0] = PREFIX;
    return qualifier;
  }
  
  /**
   * Calculates the row key based on the TSUID and the start time. If the TSUID 
   * is empty, the row key is a 0 filled byte array {@code TSDB.metrics_width()}
   * wide plus the normalized start timestamp without any tag bytes.
   * @param start_time The start time as a Unix epoch timestamp
   * @param tsuid An optional TSUID if storing a local annotation
   * @return The row key as a byte array
   */
  private static byte[] getRowKey(final long start_time, final byte[] tsuid) {
    if (start_time < 1) {
      throw new IllegalArgumentException("The start timestamp has not been set");
    }
    
    final long base_time;
    if ((start_time & Const.SECOND_MASK) != 0) {
      // drop the ms timestamp to seconds to calculate the base timestamp
      base_time = ((start_time / 1000) - 
          ((start_time / 1000) % Const.MAX_TIMESPAN));
    } else {
      base_time = (start_time - (start_time % Const.MAX_TIMESPAN));
    }
    
    // if the TSUID is empty, then we're a global annotation. The row key will 
    // just be an empty byte array of metric width plus the timestamp
    if (tsuid == null || tsuid.length < 1) {
      final byte[] row = new byte[TSDB.metrics_width() + Const.TIMESTAMP_BYTES];
      Bytes.setInt(row, (int) base_time, TSDB.metrics_width());
      return row;
    }
    
    // otherwise we need to build the row key from the TSUID and start time
    final byte[] row = new byte[Const.TIMESTAMP_BYTES + tsuid.length];
    System.arraycopy(tsuid, 0, row, 0, TSDB.metrics_width());
    Bytes.setInt(row, (int) base_time, TSDB.metrics_width());
    System.arraycopy(tsuid, TSDB.metrics_width(), row, TSDB.metrics_width() + 
        Const.TIMESTAMP_BYTES, (tsuid.length - TSDB.metrics_width()));
    return row;
  }
  
// Getters and Setters --------------  
  
  /** @return the tsuid, may be empty if this is a global annotation */
  public final String getTSUID() {
    return tsuid;
  }

  /** @return the start_time */
  public final long getStartTime() {
    return start_time;
  }

  /**  @return the end_time, may be 0 */
  public final long getEndTime() {
    return end_time;
  }

  /** @return the description */
  public final String getDescription() {
    return description;
  }

  /** @return the notes, may be empty */
  public final String getNotes() {
    return notes;
  }

  /** @return the custom key/value map, may be null */
  public final HashMap<String, String> getCustom() {
    return custom;
  }

  /** @param tsuid the tsuid to store*/
  public void setTSUID(final String tsuid) {
    this.tsuid = tsuid;
  }

  /** @param start_time the start_time, required for every annotation */
  public void setStartTime(final long start_time) {
    this.start_time = start_time;
  }

  /** @param end_time the end_time, optional*/
  public void setEndTime(final long end_time) {
    if (this.end_time != end_time) {
      this.end_time = end_time;
      changed.put("end_time", true);
    }
  }

  /** @param description the description, required for every annotation */
  public void setDescription(final String description) {
    if (!this.description.equals(description)) {
      this.description = description;
      changed.put("description", true);
    }
  }

  /** @param notes the notes to set */
  public void setNotes(final String notes) {
    if (!this.notes.equals(notes)) {
      this.notes = notes;
      changed.put("notes", true);
    }
  }

  /** @param custom the custom key/value map */
  public void setCustom(final HashMap<String, String> custom) {
    // equivalency of maps is a pain, users have to submit the whole map
    // anyway so we'll just mark it as changed every time we have a non-null
    // value
    if (this.custom != null || custom != null) {
      changed.put("custom", true);
      this.custom = custom;
    }
  }
}

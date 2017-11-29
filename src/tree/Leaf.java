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
package net.opentsdb.tree;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hbase.async.Bytes;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseException;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonGenerator;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueId.UniqueIdType;
import net.opentsdb.utils.JSON;
import net.opentsdb.utils.JSONException;

/**
 * A leaf in a tree. Each leaf is composed, primarily, of a display name and a 
 * TSUID. When stored, only the display name and TSUID are recorded. When
 * accessed via an RPC call, the leaf should include the metric and tags.
 * <p>
 * Leaves are stored as individual columns in the same row as a branch. When a
 * branch is loaded with leaves, each leaf is parsed and optionally the UID
 * names are loaded from the TSD. Leaf columns are stored with the column
 * qualifier: "leaf:&lt;display_name.hashCode()&gt;". When a leaf is written to
 * storage, a CompareAndSet is executed with a null value expected for the
 * compare. If the compare returns false, we load the leaf at that location and
 * determine if it's the same leaf. If so, it's all good and we ignore the put.
 * If the TSUID is different, we record a collision in the tree so that the user
 * knows their rule set matched a timeseries that was already recorded.
 * @since 2.0
 */
public final class Leaf implements Comparable<Leaf> {
  private static final Logger LOG = LoggerFactory.getLogger(Leaf.class);
  
  /** Charset used to convert Strings to byte arrays and back. */
  private static final Charset CHARSET = Charset.forName("ISO-8859-1");
  /** ASCII Leaf prefix */
  private static final byte[] LEAF_PREFIX = "leaf:".getBytes(CHARSET);

  /** The metric associated with this TSUID */
  private String metric = "";
  
  /** The tags associated with this TSUID for API response purposes */
  private HashMap<String, String> tags = null;
  
  /** Display name for the leaf */
  private String display_name = "";  
  
  /** TSUID the leaf links to */
  private String tsuid = "";

  /**
   * Default empty constructor necessary for des/serialization
   */
  public Leaf() {
    
  }

  /**
   * Optional constructor used when building a tree
   * @param display_name The name of the leaf
   * @param tsuid The TSUID of the leaf
   */
  public Leaf(final String display_name, final String tsuid) {
    this.display_name = display_name;
    this.tsuid = tsuid;
  }
  
  /** @return Hash code of the display name field */
  @Override
  public int hashCode() {
    return display_name.hashCode();
  }
  
  /**
   * Just compares the TSUID of the two objects as we don't care about the rest
   * @param obj The object to compare this to
   * @return True if the TSUIDs are the same or the incoming object has the same
   * address
   */
  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (this.getClass() != obj.getClass()) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    
    final Leaf leaf = (Leaf)obj;
    return tsuid.equals(leaf.tsuid);
  }
  
  /**
   * Sorts on the {@code display_name} alphabetically
   * @param leaf The leaf to compare against
   * @return string comparison
   */
  @Override
  public int compareTo(Leaf leaf) {
    return display_name.compareToIgnoreCase(leaf.display_name);
  }
  
  /** @return A string describing this object */
  @Override
  public String toString() {
    return "name: " + display_name + " tsuid: " + tsuid;
  }
  
  /**
   * Calculates the column qualifier for this leaf. The qualifier is of the
   * format: "leaf:&lt;display_name.hashCode()&gt;"
   * @return The qualifier as a byte array
   * @throws IllegalArgumentException if the {@code display_name} hasn't been 
   * set yet
   */
  public byte[] columnQualifier() {
    if (display_name == null || display_name.isEmpty()) {
      throw new IllegalArgumentException("Missing display name");
    }
    
    final byte[] qualifier = new byte[LEAF_PREFIX.length + 4];
    System.arraycopy(LEAF_PREFIX, 0, qualifier, 0, LEAF_PREFIX.length);
    System.arraycopy(Bytes.fromInt(hashCode()), 0, qualifier, 
        LEAF_PREFIX.length, 4);
    return qualifier;
  }
  
  /**
   * Attempts to write the leaf to storage using a CompareAndSet call. We expect
   * the stored value to be null. If it's not, we fetched the stored leaf. If 
   * the stored value is the TSUID as the local leaf, we return true since the
   * caller is probably reprocessing a timeseries. If the stored TSUID is
   * different, we store a collision in the tree and return false.
   * <b>Note:</b> You MUST write the tree to storage after calling this as there
   * may be a new collision. Check the tree's collision set.
   * @param tsdb The TSDB to use for storage access
   * @param branch_id ID of the branch this leaf belongs to
   * @param tree Tree the leaf and branch belong to
   * @return True if the leaf was stored successful or already existed, false
   * if there was a collision
   * @throws HBaseException if there was an issue
   * @throws JSONException if the object could not be serialized
   */
  public Deferred<Boolean> storeLeaf(final TSDB tsdb, final byte[] branch_id, 
      final Tree tree) {
    
    /**
     * Callback executed with the results of our CAS operation. If the put was
     * successful, we just return. Otherwise we load the existing leaf to
     * determine if there was a collision.
     */
    final class LeafStoreCB implements Callback<Deferred<Boolean>, Boolean> {

      final Leaf local_leaf;
      
      public LeafStoreCB(final Leaf local_leaf) {
        this.local_leaf = local_leaf;
      }
      
      /**
       * @return True if the put was successful or the leaf existed, false if 
       * there was a collision
       */
      @Override
      public Deferred<Boolean> call(final Boolean success) throws Exception {
        if (success) {
          return Deferred.fromResult(success);
        }
        
        /**
         * Called after fetching the existing leaf from storage
         */
        final class LeafFetchCB implements Callback<Deferred<Boolean>, Leaf> {
          
          /**
           * @return True if the put was successful or the leaf existed, false if 
           * there was a collision
           */
          @Override
          public Deferred<Boolean> call(final Leaf existing_leaf) 
            throws Exception {
            if (existing_leaf == null) {
              LOG.error(
                  "Returned leaf was null, stored data may be corrupt for leaf: "
                  + Branch.idToString(columnQualifier()) + " on branch: "
                  + Branch.idToString(branch_id));
              return Deferred.fromResult(false);
            }
            
            if (existing_leaf.tsuid.equals(tsuid)) {
              LOG.debug("Leaf already exists: " + local_leaf);
              return Deferred.fromResult(true);
            }
            
            tree.addCollision(tsuid, existing_leaf.tsuid);
            LOG.warn("Branch ID: [" + Branch.idToString(branch_id)  
                + "] Leaf collision with [" + tsuid + 
                "] on existing leaf [" + existing_leaf.tsuid + 
                "] named [" + display_name + "]");
            return Deferred.fromResult(false);
          }
          
        }
        
        // fetch the existing leaf so we can compare it to determine if we have
        // a collision or an existing leaf
        return Leaf.getFromStorage(tsdb, branch_id, display_name)
          .addCallbackDeferring(new LeafFetchCB());
      }
      
    }
    
    // execute the CAS call to start the callback chain
    final PutRequest put = new PutRequest(tsdb.treeTable(), branch_id, 
        Tree.TREE_FAMILY(), columnQualifier(), toStorageJson());
    return tsdb.getClient().compareAndSet(put, new byte[0])
      .addCallbackDeferring(new LeafStoreCB(this));
  }
  
  /**
   * Attempts to parse the leaf from the given column, optionally loading the
   * UID names. This is used by the branch loader when scanning an entire row.
   * <b>Note:</b> The column better have a qualifier that starts with "leaf:" or
   * we're likely to throw a parsing exception.
   * @param tsdb The TSDB to use for storage access
   * @param column Column to parse a leaf from
   * @param load_uids Whether or not to load UID names from the TSD
   * @return The parsed leaf if successful
   * @throws IllegalArgumentException if the column was missing data
   * @throws NoSuchUniqueId If any of the UID name mappings do not exist
   * @throws HBaseException if there was an issue
   * @throws JSONException if the object could not be serialized
   */
  public static Deferred<Leaf> parseFromStorage(final TSDB tsdb, 
      final KeyValue column, final boolean load_uids) {
    if (column.value() == null) {
      throw new IllegalArgumentException("Leaf column value was null");
    }

    // qualifier has the TSUID in the format  "leaf:<display_name.hashCode()>"
    // and we should only be here if the qualifier matched on "leaf:"
    final Leaf leaf = JSON.parseToObject(column.value(), Leaf.class);
    
    // if there was an error with the data and the tsuid is missing, dump it
    if (leaf.tsuid == null || leaf.tsuid.isEmpty()) {
      LOG.warn("Invalid leaf object in row: " + Branch.idToString(column.key()));
      return Deferred.fromResult(null);
    }
    
    // if we don't need to load UIDs, then return now
    if (!load_uids) {
      return Deferred.fromResult(leaf);
    }
  
    // split the TSUID to get the tags
    final List<byte[]> parsed_tags = UniqueId.getTagsFromTSUID(leaf.tsuid);
    
    // initialize the with empty objects, otherwise the "set" operations in 
    // the callback won't work.
    final ArrayList<String> tags = new ArrayList<String>(parsed_tags.size());
    for (int i = 0; i < parsed_tags.size(); i++) {
      tags.add("");
    }
    
    // setup an array of deferreds to wait on so we can return the leaf only
    // after all of the name fetches have completed
    final ArrayList<Deferred<Object>> uid_group = 
      new ArrayList<Deferred<Object>>(parsed_tags.size() + 1);

    /**
     * Callback executed after the UID name has been retrieved successfully.
     * The {@code index} determines where the result is stored: -1 means metric, 
     * >= 0 means tag
     */
    final class UIDNameCB implements Callback<Object, String> {
      final int index;
      
      public UIDNameCB(final int index) {
        this.index = index;
      }

      @Override
      public Object call(final String name) throws Exception {
        if (index < 0) {
          leaf.metric = name;
        } else {
          tags.set(index, name);
        }
        return null;
      }
      
    }
    
    // fetch the metric name first
    final byte[] metric_uid = UniqueId.stringToUid(
        leaf.tsuid.substring(0, TSDB.metrics_width() * 2));
    uid_group.add(tsdb.getUidName(UniqueIdType.METRIC, metric_uid).addCallback(
            new UIDNameCB(-1)));
    
    int idx = 0;
    for (byte[] tag : parsed_tags) {
      if (idx % 2 == 0) {
        uid_group.add(tsdb.getUidName(UniqueIdType.TAGK, tag)
              .addCallback(new UIDNameCB(idx)));
      } else {
        uid_group.add(tsdb.getUidName(UniqueIdType.TAGV, tag)
            .addCallback(new UIDNameCB(idx)));
      }          
      idx++;
    }

    /**
     * Called after all of the UID name fetches have completed and parses the
     * tag name/value list into name/value pairs for proper display
     */
    final class CollateUIDsCB implements Callback<Deferred<Leaf>, 
      ArrayList<Object>> {

      /**
       * @return A valid Leaf object loaded with UID names
       */
      @Override
      public Deferred<Leaf> call(final ArrayList<Object> name_calls) 
        throws Exception {
        int idx = 0;
        String tagk = "";
        leaf.tags = new HashMap<String, String>(tags.size() / 2);
        for (String name : tags) {
          if (idx % 2 == 0) {
            tagk = name;
          } else {
            leaf.tags.put(tagk, name);
          }
          idx++;
        }
        return Deferred.fromResult(leaf);
      }
      
    }
    
    // wait for all of the UID name fetches in the group to complete before
    // returning the leaf
    return Deferred.group(uid_group).addCallbackDeferring(new CollateUIDsCB());    
  }
  
  /** @return The configured leaf column prefix */
  public static byte[] LEAF_PREFIX() {
    return LEAF_PREFIX;
  }
  
  /**
   * Writes the leaf to a JSON object for storage. This is necessary for the CAS
   * calls and to reduce storage costs since we don't need to store UID names
   * (particularly as someone may rename a UID)
   * @return The byte array to store
   */
  private byte[] toStorageJson() {
    final ByteArrayOutputStream output = new ByteArrayOutputStream(
        display_name.length() + tsuid.length() + 30);
    try {
      final JsonGenerator json = JSON.getFactory().createGenerator(output);
      
      json.writeStartObject();
      
      // we only need to write a small amount of information
      json.writeObjectField("displayName", display_name);
      json.writeObjectField("tsuid", tsuid);
      
      json.writeEndObject();
      json.close();
      
      // TODO zero copy?
      return output.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  
  /**
   * Attempts to fetch the requested leaf from storage.
   * <b>Note:</b> This method will not load the UID names from a TSDB. This is
   * only used to fetch a particular leaf from storage for collision detection
   * @param tsdb The TSDB to use for storage access
   * @param branch_id ID of the branch this leaf belongs to
   * @param display_name Name of the leaf
   * @return A valid leaf if found, null if the leaf did not exist
   * @throws HBaseException if there was an issue
   * @throws JSONException if the object could not be serialized
   */
  private static Deferred<Leaf> getFromStorage(final TSDB tsdb, 
      final byte[] branch_id, final String display_name) {

    final Leaf leaf = new Leaf();
    leaf.setDisplayName(display_name);
    
    final GetRequest get = new GetRequest(tsdb.treeTable(), branch_id);
    get.family(Tree.TREE_FAMILY());
    get.qualifier(leaf.columnQualifier());
    
    /**
     * Called with the results of the fetch from storage
     */
    final class GetCB implements Callback<Deferred<Leaf>, ArrayList<KeyValue>> {

      /**
       * @return null if the row was empty, a valid Leaf if parsing was 
       * successful
       */
      @Override
      public Deferred<Leaf> call(ArrayList<KeyValue> row) throws Exception {
        if (row == null || row.isEmpty()) {
          return Deferred.fromResult(null);
        }
        
        final Leaf leaf = JSON.parseToObject(row.get(0).value(), Leaf.class);
        return Deferred.fromResult(leaf);
      }
      
    }
    
    return tsdb.getClient().get(get).addCallbackDeferring(new GetCB());
  }
  
  // GETTERS AND SETTERS ----------------------------

  /** @return The metric associated with this TSUID */
  public String getMetric() {
    return metric;
  }
  
  /** @return The tags associated with this TSUID */
  public Map<String, String> getTags() {
    return tags;
  }
  
  /** @return The public name of this leaf */
  public String getDisplayName() {
    return display_name;
  }

  /** @return the tsuid */
  public String getTsuid() {
    return tsuid;
  }

  /** @param metric The metric associated with this TSUID */
  public void setMetric(final String metric) {
    this.metric = metric;
  }
  
  /** @param tags The tags associated with this TSUID */
  public void setTags(final HashMap<String, String> tags) {
    this.tags = tags;
  }
  
  /** @param display_name Public display name for the leaf */
  public void setDisplayName(final String display_name) {
    this.display_name = display_name;
  }

  /** @param tsuid the tsuid to set */
  public void setTsuid(final String tsuid) {
    this.tsuid = tsuid;
  }
  
}

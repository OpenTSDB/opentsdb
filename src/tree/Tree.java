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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import net.opentsdb.core.TSDB;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.JSON;
import net.opentsdb.utils.JSONException;

import org.hbase.async.Bytes;
import org.hbase.async.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseException;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.core.JsonGenerator;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

/**
 * Represents a meta data tree in OpenTSDB that organizes timeseries into a
 * hierarchical structure for navigation similar to a file system directory.
 * Actual results are stored in {@link Branch} and {@link Leaf} objects while 
 * meta data about the tree is contained in this object.
 * <p>
 * A tree is built from a set of {@link TreeRule}s. The rules are stored 
 * separately in the same row as the tree definition object, but can be loaded
 * into the tree for processing and return from an RPC request. Building a tree
 * consists of defining a tree, assigning one or more rules, and passing 
 * {@link TSMeta} objects through the rule set using a {@link TreeBuilder}.
 * Results are then stored in separate rows as branch and leaf objects.
 * <p>
 * If TSMeta collides with something that has already been processed by a
 * rule set, a collision will be recorded, via this object, in a separate column
 * in a separate row for collisions. Likewise, if a tree is set to 
 * {@code strict_match}, TSMetas that fail to match the rule set will be 
 * recorded to a separate row. This class provides helper methods for fetching
 * and storing these collisions and non-matched items.
 * @since 2.0
 */
@JsonIgnoreProperties(ignoreUnknown = true) 
@JsonAutoDetect(fieldVisibility = Visibility.PUBLIC_ONLY)
public final class Tree {
  private static final Logger LOG = LoggerFactory.getLogger(Tree.class);
  
  /** Charset used to convert Strings to byte arrays and back. */
  private static final Charset CHARSET = Charset.forName("ISO-8859-1");
  /** Width of tree IDs in bytes */
  private static final short TREE_ID_WIDTH = 2;
  /** Name of the CF where trees and branches are stored */
  private static final byte[] NAME_FAMILY = "name".getBytes(CHARSET);
  /** The tree qualifier */
  private static final byte[] TREE_QUALIFIER = "tree".getBytes(CHARSET);
  /** Integer width in bytes */
  private static final short INT_WIDTH = 4;
  /** Byte suffix for collision rows, appended after the tree ID */
  private static byte COLLISION_ROW_SUFFIX = 0x01;
  /** Byte prefix for collision columns */
  private static byte[] COLLISION_PREFIX = "tree_collision:".getBytes(CHARSET);
  /** Byte suffix for not matched rows, appended after the tree ID */
  private static byte NOT_MATCHED_ROW_SUFFIX = 0x02;
  /** Byte prefix for not matched columns */
  private static byte[] NOT_MATCHED_PREFIX = "tree_not_matched:".getBytes(CHARSET);

  /** The numeric ID of this tree object */
  private int tree_id;
  
  /** Name of the tree */
  private String name = "";
  
  /** A brief description of the tree */
  private String description = "";
  
  /** Notes about the tree */
  private String notes = "";
  
  /** Whether or not strict matching is enabled */
  private boolean strict_match;
  
  /** Whether or not the tree should process meta data or not */
  private boolean enabled;
  
  /** Sorted, two dimensional map of the tree's rules */
  private TreeMap<Integer, TreeMap<Integer, TreeRule>> rules;

  /** List of non-matched TSUIDs that were not included in the tree */
  private HashMap<String, String> not_matched;
  
  /** List of TSUID collisions that were not included in the tree */
  private HashMap<String, String> collisions;

  /** Unix time, in seconds, when the tree was created */
  private long created;

  /** Tracks fields that have changed by the user to avoid overwrites */
  private final HashMap<String, Boolean> changed = 
    new HashMap<String, Boolean>();
  
  /**
   * Default constructor necessary for de/serialization
   */
  public Tree() {
    initializeChangedMap();
  }
  
  /**
   * Constructor that sets the tree ID and the created timestamp to the current
   * time.
   * @param tree_id ID of this tree
   */
  public Tree(final int tree_id) {
    this.tree_id = tree_id;
    this.created = System.currentTimeMillis() / 1000;
    initializeChangedMap();
  }
  
  /** @return Information about the tree */
  @Override
  public String toString() {
    return "treeId: " + tree_id + " name: " + name;
  }
  
  /**
   * Copies changes from the incoming tree into the local tree, overriding if
   * called to. Only parses user mutable fields, excluding rules.
   * @param tree The tree to copy from
   * @param overwrite Whether or not to copy all values from the incoming tree
   * @return True if there were changes, false if not
   * @throws IllegalArgumentException if the incoming tree was invalid
   */
  public boolean copyChanges(final Tree tree, final boolean overwrite) {
    if (tree == null) {
      throw new IllegalArgumentException("Cannot copy a null tree");
    }
    if (tree_id != tree.tree_id) {
      throw new IllegalArgumentException("Tree IDs do not match");
    }
    
    if (overwrite || tree.changed.get("name")) {
      name = tree.name;
      changed.put("name", true);
    }
    if (overwrite || tree.changed.get("description")) {
      description = tree.description;
      changed.put("description", true);
    }
    if (overwrite || tree.changed.get("notes")) {
      notes = tree.notes;
      changed.put("notes", true);
    }
    if (overwrite || tree.changed.get("strict_match")) {
      strict_match = tree.strict_match;
      changed.put("strict_match", true);
    }
    for (boolean has_changes : changed.values()) {
      if (has_changes) {
        return true;
      }
    }
    return false;
  }
  
  /**
   * Adds the given rule to the tree, replacing anything in the designated spot
   * @param rule The rule to add
   * @throws IllegalArgumentException if the incoming rule was invalid
   */
  public void addRule(final TreeRule rule) {
    if (rule == null) {
      throw new IllegalArgumentException("Null rules are not accepted");
    }
    if (rules == null) {
      rules = new TreeMap<Integer, TreeMap<Integer, TreeRule>>();
    }
    
    TreeMap<Integer, TreeRule> level = rules.get(rule.getLevel());
    if (level == null) {
      level = new TreeMap<Integer, TreeRule>();
      level.put(rule.getOrder(), rule);
      rules.put(rule.getLevel(), level);
    } else {
      level.put(rule.getOrder(), rule);
    }
    
    changed.put("rules", true);
  }

  /**
   * Adds a TSUID to the collision local list, must then be synced with storage
   * @param tsuid TSUID to add to the set
   * @throws IllegalArgumentException if the tsuid was invalid
   */
  public void addCollision(final String tsuid, final String existing_tsuid) {
    if (tsuid == null || tsuid.isEmpty()) {
      throw new IllegalArgumentException("Empty or null collisions not allowed");
    }
    if (collisions == null) {
      collisions = new HashMap<String, String>();
    }
    if (!collisions.containsKey(tsuid)) {
      collisions.put(tsuid, existing_tsuid);
      changed.put("collisions", true);
    }
  }
  
  /**
   * Adds a TSUID to the not-matched local list when strict_matching is enabled.
   * Must be synced with storage.
   * @param tsuid TSUID to add to the set
   * @throws IllegalArgumentException if the tsuid was invalid
   */
  public void addNotMatched(final String tsuid, final String message) {
    if (tsuid == null || tsuid.isEmpty()) {
      throw new IllegalArgumentException("Empty or null non matches not allowed");
    }
    if (not_matched == null) {
      not_matched = new HashMap<String, String>();
    }
    if (!not_matched.containsKey(tsuid)) {
      not_matched.put(tsuid, message);
      changed.put("not_matched", true);
    }
  }
  
  /**
   * Attempts to store the tree definition and any local collisions or 
   * not-matched entries via CompareAndSet calls.
   * @param tsdb The TSDB to use for access
   * @param lock An optional lock to use on the row
   * @return A list of deferreds to wait on until all storage calls have
   * completed.
   * @throws IllegalArgumentException if the tree ID is missing or invalid
   * @throws HBaseException if a storage exception occurred
   */
  public Deferred<ArrayList<Object>> storeTree(final TSDB tsdb, 
      final boolean overwrite) {
    if (tree_id < 1 || tree_id > 65535) {
      throw new IllegalArgumentException("Invalid Tree ID");
    }
    
    // if there aren't any changes, save time and bandwidth by not writing to
    // storage
    boolean has_tree_changes = false;
    boolean has_set_changes = false;
    for (Map.Entry<String, Boolean> entry : changed.entrySet()) {
      if (entry.getValue()) {
        if (entry.getKey().equals("collisions") || 
            entry.getKey().equals("not_matched")) {
          has_set_changes = true;
        } else {
          has_tree_changes = true;
        }
      }
    }
    if (!has_tree_changes && !has_set_changes) {
      LOG.debug(this + " does not have changes, skipping sync to storage");
      throw new IllegalStateException("No changes detected in the tree");
    }

    // a list of deferred objects tracking the CAS calls so the caller can wait
    // until their all complete
    final ArrayList<Deferred<Boolean>> storage_results = 
      new ArrayList<Deferred<Boolean>>(3);
      
    // if the tree itself has changes, sync them to storage
    if (has_tree_changes) {
      
      /**
       * Callback executed after loading a tree from storage so that we can
       * synchronize changes to the meta data and write them back to storage.
       */
      final class StoreTreeCB implements Callback<Deferred<Boolean>, Tree> {
        
        final private Tree local_tree;
        
        public StoreTreeCB(final Tree local_tree) {
          this.local_tree = local_tree;
        }
        
        /**
         * Synchronizes the stored tree object (if found) with the local tree 
         * and issues a CAS call to write the update to storage.
         * @return True if the CAS was successful, false if something changed 
         * in flight
         */
        @Override
        public Deferred<Boolean> call(final Tree fetched_tree) throws Exception {
          
          Tree stored_tree = fetched_tree;
          final byte[] original_tree = stored_tree == null ? new byte[0] : 
            stored_tree.toStorageJson();
          
          // now copy changes
          if (stored_tree == null) {
            stored_tree = local_tree;
          } else {
            stored_tree.copyChanges(local_tree, overwrite);
          }
          
          // reset the change map so we don't keep writing
          initializeChangedMap();
          
          final PutRequest put = new PutRequest(tsdb.uidTable(), 
              Tree.idToBytes(tree_id), NAME_FAMILY, TREE_QUALIFIER, 
              stored_tree.toStorageJson());
          return tsdb.getClient().compareAndSet(put, original_tree);
        }
      }
      
      // initiate the sync by attempting to fetch an existing tree from storage
      final Deferred<Boolean> process_tree = fetchTree(tsdb, tree_id)
        .addCallbackDeferring(new StoreTreeCB(this));
      storage_results.add(process_tree);
    }
    
    // if there were any collisions or not-matched entries found, flush them
    // as well
    if (has_set_changes) {
      if (collisions != null && !collisions.isEmpty()) {
        storage_results.add(flushCollisions(tsdb));
      }
      if (not_matched != null && !not_matched.isEmpty()) {
        storage_results.add(flushNotMatched(tsdb));
      }
    }
    
    // return the set of deferred CAS calls for the caller to wait on
    return Deferred.group(storage_results);
  }
  
  /**
   * Retrieves a single rule from the rule set given a level and order
   * @param level The level where the rule resides
   * @param order The order in the level where the rule resides
   * @return The rule if found, null if not found
   */
  public TreeRule getRule(final int level, final int order) {
    if (rules == null || rules.isEmpty()) { 
      return null;
    }
    
    TreeMap<Integer, TreeRule> rule_level = rules.get(level);
    if (rule_level == null || rule_level.isEmpty()) {
      return null;
    }
    
    return rule_level.get(order);
  }
  
  /**
   * Attempts to store the local tree in a new row, automatically assigning a
   * new tree ID and returning the value.
   * This method will scan the UID table for the maximum tree ID, increment it,
   * store the new tree, and return the new ID. If no trees have been created,
   * the returned ID will be "1". If we have reached the limit of trees for the
   * system, as determined by {@link #TREE_ID_WIDTH}, we will throw an exception.
   * @param tsdb The TSDB to use for storage access
   * @return A positive ID, greater than 0 if successful, 0 if there was
   * an error
   */
  public Deferred<Integer> createNewTree(final TSDB tsdb) {
    if (tree_id > 0) {
      throw new IllegalArgumentException("Tree ID has already been set");
    }
    if (name == null || name.isEmpty()) {
      throw new IllegalArgumentException("Tree was missing the name");
    }
    
    /**
     * Called after a successful CAS to store the new tree with the new ID.
     * Returns the new ID if successful, 0 if there was an error
     */
    final class CreatedCB implements Callback<Deferred<Integer>, 
      ArrayList<Object>> {
      
      @Override
      public Deferred<Integer> call(final ArrayList<Object> deferreds) 
        throws Exception {
        return Deferred.fromResult(tree_id);
      }
      
    }
    
    /**
     * Called after fetching all trees. Loops through the tree definitions and
     * determines the max ID so we can increment and write a new one
     */
    final class CreateNewCB implements Callback<Deferred<Integer>, List<Tree>> {

      @Override
      public Deferred<Integer> call(List<Tree> trees) throws Exception {
        int max_id = 0;
        if (trees != null) {
          for (Tree tree : trees) {
            if (tree.tree_id > max_id) {
              max_id = tree.tree_id;
            }
          }
        }
        
        tree_id = max_id + 1;
        if (tree_id > 65535) {
          throw new IllegalStateException("Exhausted all Tree IDs");
        }
        
        return storeTree(tsdb, true).addCallbackDeferring(new CreatedCB());
      }
      
    }
    
    // starts the process by fetching all tree definitions from storage
    return fetchAllTrees(tsdb).addCallbackDeferring(new CreateNewCB());
  }
  
  /**
   * Attempts to fetch the given tree from storage, loading the rule set at
   * the same time.
   * @param tsdb The TSDB to use for access
   * @param tree_id The Tree to fetch
   * @return A tree object if found, null if the tree did not exist
   * @throws IllegalArgumentException if the tree ID was invalid
   * @throws HBaseException if a storage exception occurred
   * @throws JSONException if the object could not be deserialized
   */
  public static Deferred<Tree> fetchTree(final TSDB tsdb, final int tree_id) {
    if (tree_id < 1 || tree_id > 65535) {
      throw new IllegalArgumentException("Invalid Tree ID");
    }

    // fetch the whole row
    final GetRequest get = new GetRequest(tsdb.uidTable(), idToBytes(tree_id));
    get.family(NAME_FAMILY);
    
    /**
     * Called from the GetRequest with results from storage. Loops through the
     * columns and loads the tree definition and rules
     */
    final class FetchTreeCB implements Callback<Deferred<Tree>, 
      ArrayList<KeyValue>> {
  
      @Override
      public Deferred<Tree> call(ArrayList<KeyValue> row) throws Exception {
        if (row == null || row.isEmpty()) {
          return Deferred.fromResult(null);
        }
        
        final Tree tree = new Tree();
        
        // WARNING: Since the JSON in storage doesn't store the tree ID, we need
        // to loadi t from the row key.
        tree.setTreeId(bytesToId(row.get(0).key()));
        
        for (KeyValue column : row) {
          if (Bytes.memcmp(TREE_QUALIFIER, column.qualifier()) == 0) {
            // it's *this* tree. We deserialize to a new object and copy
            // since the columns could be in any order and we may get a rule 
            // before the tree object
            final Tree local_tree = JSON.parseToObject(column.value(), Tree.class);
            tree.created = local_tree.created;
            tree.description = local_tree.description;
            tree.name = local_tree.name;
            tree.notes = local_tree.notes;
            tree.strict_match = tree.strict_match;
            
          // Tree rule
          } else if (Bytes.memcmp(TreeRule.RULE_PREFIX(), column.qualifier(), 0, 
              TreeRule.RULE_PREFIX().length) == 0) {
            final TreeRule rule = TreeRule.parseFromStorage(column);
            tree.addRule(rule);
          }
        }
        
        return Deferred.fromResult(tree);
      }
      
    }
    
    // issue the get request
    return tsdb.getClient().get(get).addCallbackDeferring(new FetchTreeCB());
  }

  /**
   * Attempts to retrieve all trees from the UID table, including their rules.
   * If no trees were found, the result will be an empty list
   * @param tsdb The TSDB to use for storage
   * @return A list of tree objects. May be empty if none were found
   */
  public static Deferred<List<Tree>> fetchAllTrees(final TSDB tsdb) {
    
    final Deferred<List<Tree>> result = new Deferred<List<Tree>>();
    
    /**
     * Scanner callback that recursively calls itself to load the next set of
     * rows from storage. When the scanner returns a null, the callback will
     * return with the list of trees discovered.
     */
    final class AllTreeScanner implements Callback<Object, 
      ArrayList<ArrayList<KeyValue>>> {
  
      private final List<Tree> trees = new ArrayList<Tree>();
      private final Scanner scanner;
      
      public AllTreeScanner() {
        scanner = setupAllTreeScanner(tsdb);
      }
      
      /**
       * Fetches the next set of results from the scanner and adds this class
       * as a callback.
       * @return A list of trees if the scanner has reached the end
       */
      public Object fetchTrees() {
        return scanner.nextRows().addCallback(this);
      }
      
      @Override
      public Object call(ArrayList<ArrayList<KeyValue>> rows)
          throws Exception {
        if (rows == null) {
          result.callback(trees);
          return null;
        }
        
        for (ArrayList<KeyValue> row : rows) {
          final Tree tree = new Tree();
          for (KeyValue column : row) {
            if (column.qualifier().length >= TREE_QUALIFIER.length && 
                Bytes.memcmp(TREE_QUALIFIER, column.qualifier()) == 0) {
              // it's *this* tree. We deserialize to a new object and copy
              // since the columns could be in any order and we may get a rule 
              // before the tree object
              final Tree local_tree = JSON.parseToObject(column.value(), 
                  Tree.class);
              tree.created = local_tree.created;
              tree.description = local_tree.description;
              tree.name = local_tree.name;
              tree.notes = local_tree.notes;
              tree.strict_match = tree.strict_match;
              
              // WARNING: Since the JSON data in storage doesn't contain the tree
              // ID, we need to parse it from the row key
              tree.setTreeId(bytesToId(row.get(0).key()));
              
            // tree rule
            } else if (column.qualifier().length > TreeRule.RULE_PREFIX().length &&
                Bytes.memcmp(TreeRule.RULE_PREFIX(), column.qualifier(), 
                0, TreeRule.RULE_PREFIX().length) == 0) {
              final TreeRule rule = TreeRule.parseFromStorage(column);
              tree.addRule(rule);
            }
          }
          
          // only add the tree if we parsed a valid ID
          if (tree.tree_id > 0) {
            trees.add(tree);
          }
        }
        
        // recurse to get the next set of rows from the scanner
        return fetchTrees();
      }
      
    }
    
    // start the scanning process
    new AllTreeScanner().fetchTrees();
    return result;
  }
  
  /**
   * Returns the collision set from storage for the given tree, optionally for
   * only the list of TSUIDs provided.
   * <b>Note:</b> This can potentially be a large list if the rule set was
   * written poorly and there were many timeseries so only call this
   * without a list of TSUIDs if you feel confident the number is small.
   * @param tsdb TSDB to use for storage access
   * @param tree_id ID of the tree to fetch collisions for
   * @param tsuids An optional list of TSUIDs to fetch collisions for. This may
   * be empty or null, in which case all collisions for the tree will be 
   * returned.
   * @return A list of collisions or null if nothing was found
   * @throws HBaseException if there was an issue
   * @throws IllegalArgumentException if the tree ID was invalid
   */
  public static Deferred<Map<String, String>> fetchCollisions(final TSDB tsdb, 
      final int tree_id, final List<String> tsuids) {
    if (tree_id < 1 || tree_id > 65535) {
      throw new IllegalArgumentException("Invalid Tree ID");
    }
    
    final byte[] row_key = new byte[TREE_ID_WIDTH + 1];
    System.arraycopy(idToBytes(tree_id), 0, row_key, 0, TREE_ID_WIDTH);
    row_key[TREE_ID_WIDTH] = COLLISION_ROW_SUFFIX;
    
    final GetRequest get = new GetRequest(tsdb.uidTable(), row_key);
    get.family(NAME_FAMILY);
    
    // if the caller provided a list of TSUIDs, then we need to compile a list
    // of qualifiers so we only fetch those columns.
    if (tsuids != null && !tsuids.isEmpty()) {
      final byte[][] qualifiers = new byte[tsuids.size()][];
      int index = 0;
      for (String tsuid : tsuids) {
        final byte[] qualifier = new byte[COLLISION_PREFIX.length + 
                                          (tsuid.length() / 2)];
        System.arraycopy(COLLISION_PREFIX, 0, qualifier, 0, 
            COLLISION_PREFIX.length);
        final byte[] tsuid_bytes = UniqueId.stringToUid(tsuid);
        System.arraycopy(tsuid_bytes, 0, qualifier, COLLISION_PREFIX.length, 
            tsuid_bytes.length);
        qualifiers[index] = qualifier;
        index++;
      }
      get.qualifiers(qualifiers);
    }
    
    /**
     * Called after issuing the row get request to parse out the results and
     * compile the list of collisions.
     */
    final class GetCB implements Callback<Deferred<Map<String, String>>, 
      ArrayList<KeyValue>> {

      @Override
      public Deferred<Map<String, String>> call(final ArrayList<KeyValue> row)
          throws Exception {
        if (row == null || row.isEmpty()) {
          final Map<String, String> empty = new HashMap<String, String>(0);
          return Deferred.fromResult(empty);
        }
        
        final Map<String, String> collisions = 
          new HashMap<String, String>(row.size());
        
        for (KeyValue column : row) {
          if (column.qualifier().length > COLLISION_PREFIX.length && 
              Bytes.memcmp(COLLISION_PREFIX, column.qualifier(), 0, 
                  COLLISION_PREFIX.length) == 0) {
            final byte[] parsed_tsuid = Arrays.copyOfRange(column.qualifier(), 
                COLLISION_PREFIX.length, column.qualifier().length);
            collisions.put(UniqueId.uidToString(parsed_tsuid), 
                new String(column.value(), CHARSET));
          }
        }
        
        return Deferred.fromResult(collisions);
      }
      
    }
    
    return tsdb.getClient().get(get).addCallbackDeferring(new GetCB());
  }
  
  /**
   * Returns the not-matched set from storage for the given tree, optionally for
   * only the list of TSUIDs provided.
   * <b>Note:</b> This can potentially be a large list if the rule set was
   * written poorly and there were many timeseries so only call this
   * without a list of TSUIDs if you feel confident the number is small.
   * @param tsdb TSDB to use for storage access
   * @param tree_id ID of the tree to fetch non matches for
   * @param tsuids An optional list of TSUIDs to fetch non-matches for. This may
   * be empty or null, in which case all non-matches for the tree will be 
   * returned.
   * @return A list of not-matched mappings or null if nothing was found
   * @throws HBaseException if there was an issue
   * @throws IllegalArgumentException if the tree ID was invalid
   */
  public static Deferred<Map<String, String>> fetchNotMatched(final TSDB tsdb, 
      final int tree_id, final List<String> tsuids) {
    if (tree_id < 1 || tree_id > 65535) {
      throw new IllegalArgumentException("Invalid Tree ID");
    }
    
    final byte[] row_key = new byte[TREE_ID_WIDTH + 1];
    System.arraycopy(idToBytes(tree_id), 0, row_key, 0, TREE_ID_WIDTH);
    row_key[TREE_ID_WIDTH] = NOT_MATCHED_ROW_SUFFIX;
    
    final GetRequest get = new GetRequest(tsdb.uidTable(), row_key);
    get.family(NAME_FAMILY);
    
    // if the caller provided a list of TSUIDs, then we need to compile a list
    // of qualifiers so we only fetch those columns.
    if (tsuids != null && !tsuids.isEmpty()) {
      final byte[][] qualifiers = new byte[tsuids.size()][];
      int index = 0;
      for (String tsuid : tsuids) {
        final byte[] qualifier = new byte[NOT_MATCHED_PREFIX.length + 
                                          (tsuid.length() / 2)];
        System.arraycopy(NOT_MATCHED_PREFIX, 0, qualifier, 0, 
            NOT_MATCHED_PREFIX.length);
        final byte[] tsuid_bytes = UniqueId.stringToUid(tsuid);
        System.arraycopy(tsuid_bytes, 0, qualifier, NOT_MATCHED_PREFIX.length, 
            tsuid_bytes.length);
        qualifiers[index] = qualifier;
        index++;
      }
      get.qualifiers(qualifiers);
    }
    
    /**
     * Called after issuing the row get request to parse out the results and
     * compile the list of collisions.
     */
    final class GetCB implements Callback<Deferred<Map<String, String>>, 
      ArrayList<KeyValue>> {

      @Override
      public Deferred<Map<String, String>> call(final ArrayList<KeyValue> row)
          throws Exception {
        if (row == null || row.isEmpty()) {
          final Map<String, String> empty = new HashMap<String, String>(0);
          return Deferred.fromResult(empty);
        }
        
        Map<String, String> not_matched = new HashMap<String, String>(row.size());
        
        for (KeyValue column : row) {
          final byte[] parsed_tsuid = Arrays.copyOfRange(column.qualifier(), 
              NOT_MATCHED_PREFIX.length, column.qualifier().length);
          not_matched.put(UniqueId.uidToString(parsed_tsuid), 
              new String(column.value(), CHARSET));
        }
        
        return Deferred.fromResult(not_matched);
      }
      
    }
    
    return tsdb.getClient().get(get).addCallbackDeferring(new GetCB());
  }
  
  /**
   * Attempts to delete all branches, leaves, collisions and not-matched entries
   * for the given tree. Optionally can delete the tree definition and rules as
   * well.
   * <b>Warning:</b> This call can take a long time to complete so it should
   * only be done from a command line or issues once via RPC and allowed to
   * process. Multiple deletes running at the same time on the same tree
   * shouldn't be an issue but it's a waste of resources.
   * @param tsdb The TSDB to use for storage access
   * @param tree_id ID of the tree to delete
   * @param delete_definition Whether or not the tree definition and rule set
   * should be deleted as well
   * @return True if the deletion completed successfully, false if there was an
   * issue.
   * @throws HBaseException if there was an issue
   * @throws IllegalArgumentException if the tree ID was invalid
   */
  public static Deferred<Boolean> deleteTree(final TSDB tsdb, 
      final int tree_id, final boolean delete_definition) {
    if (tree_id < 1 || tree_id > 65535) {
      throw new IllegalArgumentException("Invalid Tree ID");
    }

    // scan all of the rows starting with the tree ID. We can't just delete the
    // rows as there may be other types of data. Thus we have to check the
    // qualifiers of every column to see if it's safe to delete
    final byte[] start = idToBytes(tree_id);
    final byte[] end = idToBytes(tree_id + 1);
    final Scanner scanner = tsdb.getClient().newScanner(tsdb.uidTable());
    scanner.setStartKey(start);
    scanner.setStopKey(end);   
    scanner.setFamily(NAME_FAMILY);
    
    final Deferred<Boolean> completed = new Deferred<Boolean>();
    
    /**
     * Scanner callback that loops through all rows between tree id and 
     * tree id++ searching for tree related columns to delete.
     */
    final class DeleteTreeScanner implements Callback<Deferred<Boolean>, 
      ArrayList<ArrayList<KeyValue>>> {
  
      // list where we'll store delete requests for waiting on
      private final ArrayList<Deferred<Object>> delete_deferreds = 
        new ArrayList<Deferred<Object>>();
      
      /**
       * Fetches the next set of rows from the scanner and adds this class as
       * a callback
       * @return The list of delete requests when the scanner returns a null set
       */
      public Deferred<Boolean> deleteTree() {
        return scanner.nextRows().addCallbackDeferring(this);
      }
      
      @Override
      public Deferred<Boolean> call(ArrayList<ArrayList<KeyValue>> rows)
          throws Exception {
        if (rows == null) {
          completed.callback(true);
          return null;
        }
        
        for (final ArrayList<KeyValue> row : rows) {
          // one delete request per row. We'll almost always delete the whole
          // row, so just preallocate the entire row.
          ArrayList<byte[]> qualifiers = new ArrayList<byte[]>(row.size());
          for (KeyValue column : row) {
            // tree
            if (delete_definition && Bytes.equals(TREE_QUALIFIER, column.qualifier())) {
              LOG.trace("Deleting tree defnition in row: " + 
                  Branch.idToString(column.key()));
              qualifiers.add(column.qualifier());
              
            // branches
            } else if (Bytes.equals(Branch.BRANCH_QUALIFIER(), column.qualifier())) {
              LOG.trace("Deleting branch in row: " + 
                  Branch.idToString(column.key()));
              qualifiers.add(column.qualifier());
            
            // leaves
            } else if (column.qualifier().length > Leaf.LEAF_PREFIX().length &&
                Bytes.memcmp(Leaf.LEAF_PREFIX(), column.qualifier(), 0, 
                    Leaf.LEAF_PREFIX().length) == 0) {
              LOG.trace("Deleting leaf in row: " + 
                  Branch.idToString(column.key()));
              qualifiers.add(column.qualifier());
              
            // collisions
            } else if (column.qualifier().length > COLLISION_PREFIX.length && 
                Bytes.memcmp(COLLISION_PREFIX, column.qualifier(), 0, 
                    COLLISION_PREFIX.length) == 0) {
              LOG.trace("Deleting collision in row: " + 
                  Branch.idToString(column.key()));
              qualifiers.add(column.qualifier());
              
            // not matched
            } else if (column.qualifier().length > NOT_MATCHED_PREFIX.length && 
                Bytes.memcmp(NOT_MATCHED_PREFIX, column.qualifier(), 0, 
                    NOT_MATCHED_PREFIX.length) == 0) {
              LOG.trace("Deleting not matched in row: " + 
                  Branch.idToString(column.key()));
              qualifiers.add(column.qualifier());
              
            // tree rule
            } else if (delete_definition && column.qualifier().length > TreeRule.RULE_PREFIX().length && 
                Bytes.memcmp(TreeRule.RULE_PREFIX(), column.qualifier(), 0, 
                    TreeRule.RULE_PREFIX().length) == 0) {
              LOG.trace("Deleting tree rule in row: " + 
                  Branch.idToString(column.key()));
              qualifiers.add(column.qualifier());
            } 
          }
          
          if (qualifiers.size() > 0) {
            final DeleteRequest delete = new DeleteRequest(tsdb.uidTable(), 
                row.get(0).key(), NAME_FAMILY, 
                qualifiers.toArray(new byte[qualifiers.size()][])
                );
            delete_deferreds.add(tsdb.getClient().delete(delete));
          }
        }
        
        /**
         * Callback used as a kind of buffer so that we don't wind up loading
         * thousands or millions of delete requests into memory and possibly run
         * into a StackOverflowError or general OOM. The scanner defaults are
         * our limit so each pass of the scanner will wait for the previous set
         * of deferreds to complete before continuing
         */
        final class ContinueCB implements Callback<Deferred<Boolean>, 
          ArrayList<Object>> {
          
          public Deferred<Boolean> call(ArrayList<Object> objects) {
            LOG.debug("Purged [" + objects.size() + "] columns, continuing");
            delete_deferreds.clear();
            // call ourself again to get the next set of rows from the scanner
            return deleteTree();
          }
          
        }
        
        // call ourself again after waiting for the existing delete requests 
        // to complete
        Deferred.group(delete_deferreds).addCallbackDeferring(new ContinueCB());
        return null;
      }    
    }
    
    // start the scanner
    new DeleteTreeScanner().deleteTree();
    return completed;
  }
  
  /**
   * Converts the tree ID into a byte array {@link #TREE_ID_WIDTH} in size
   * @param tree_id The tree ID to convert
   * @return The tree ID as a byte array
   * @throws IllegalArgumentException if the Tree ID is invalid
   */
  public static byte[] idToBytes(final int tree_id) {
    if (tree_id < 1 || tree_id > 65535) {
      throw new IllegalArgumentException("Missing or invalid tree ID");
    }
    final byte[] id = Bytes.fromInt(tree_id);
    return Arrays.copyOfRange(id, id.length - TREE_ID_WIDTH, id.length);
  }
  
  /**
   * Attempts to convert the given byte array into an integer tree ID
   * <b>Note:</b> You can give this method a full branch row key and it will
   * only parse out the first {@link #TREE_ID_WIDTH} bytes.
   * @param row_key The row key or tree ID as a byte array
   * @return The tree ID as an integer value
   * @throws IllegalArgumentException if the byte array is less than 
   * {@link #TREE_ID_WIDTH} long
   */
  public static int bytesToId(final byte[] row_key) {
    if (row_key.length < TREE_ID_WIDTH) {
      throw new IllegalArgumentException("Row key was less than " + 
          TREE_ID_WIDTH + " in length");
    }
    
    final byte[] tree_id = new byte[INT_WIDTH];
    System.arraycopy(row_key, 0, tree_id, INT_WIDTH - Tree.TREE_ID_WIDTH(), 
        Tree.TREE_ID_WIDTH());
    return Bytes.getInt(tree_id);    
  }
  
  /** @return The configured collision column qualifier prefix */
  public static byte[] COLLISION_PREFIX() {
    return COLLISION_PREFIX;
  }
  
  /** @return The configured not-matched column qualifier prefix */
  public static byte[] NOT_MATCHED_PREFIX() {
    return NOT_MATCHED_PREFIX;
  }
  
  /**
   * Sets or resets the changed map flags
   */
  private void initializeChangedMap() {
    // set changed flags
    // tree_id can't change
    changed.put("name", false);
    changed.put("field", false);
    changed.put("description", false);
    changed.put("notes", false);
    changed.put("strict_match", false);
    changed.put("rules", false);
    changed.put("not_matched", false);
    changed.put("collisions", false);
    changed.put("created", false);
    changed.put("last_update", false);
    changed.put("version", false);
    changed.put("node_separator", false);
  }
  
  /**
   * Converts the object to a JSON byte array, necessary for CAS calls and to
   * keep redundant data down
   * @return A byte array with the serialized tree
   */
  private byte[] toStorageJson() {
    // TODO - precalc how much memory to grab
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    try {
      final JsonGenerator json = JSON.getFactory().createGenerator(output);
      
      json.writeStartObject();
      
      // we only need to write a small amount of information
      //json.writeNumberField("treeId", tree_id);
      json.writeStringField("name", name);
      json.writeStringField("description", description);
      json.writeStringField("notes", notes);
      json.writeBooleanField("strictMatch", strict_match);
      json.writeNumberField("created", created);
      json.writeBooleanField("enabled", enabled);
      
      json.writeEndObject();
      json.close();
      
      // TODO zero copy?
      return output.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  
  /**
   * Configures a scanner to run through all rows in the UID table that are
   * {@link #TREE_ID_WIDTH} bytes wide using a row key regex filter 
   * @param tsdb The TSDB to use for storage access
   * @return The configured HBase scanner
   */
  private static Scanner setupAllTreeScanner(final TSDB tsdb) {
    final byte[] start = new byte[TREE_ID_WIDTH];
    final byte[] end = new byte[TREE_ID_WIDTH];
    Arrays.fill(end, (byte)0xFF);
    
    final Scanner scanner = tsdb.getClient().newScanner(tsdb.uidTable());
    scanner.setStartKey(start);
    scanner.setStopKey(end);   
    scanner.setFamily(NAME_FAMILY);
    
    // set the filter to match only on TREE_ID_WIDTH row keys
    final StringBuilder buf = new StringBuilder(20);
    buf.append("(?s)"  // Ensure we use the DOTALL flag.
        + "^\\Q");
    buf.append("\\E(?:.{").append(TREE_ID_WIDTH).append("})$");
    scanner.setKeyRegexp(buf.toString(), CHARSET);
    return scanner;
  }

  /**
   * Attempts to flush the collisions to storage. The storage call is a PUT so
   * it will overwrite any existing columns, but since each column is the TSUID
   * it should only exist once and the data shouldn't change.
   * <b>Note:</b> This will also clear the local {@link #collisions} map
   * @param tsdb The TSDB to use for storage access
   * @return A meaningless deferred (will always be true since we need to group
   * it with tree store calls) for the caller to wait on
   * @throws HBaseException if there was an issue
   */
  private Deferred<Boolean> flushCollisions(final TSDB tsdb) {
    final byte[] row_key = new byte[TREE_ID_WIDTH + 1];
    System.arraycopy(idToBytes(tree_id), 0, row_key, 0, TREE_ID_WIDTH);
    row_key[TREE_ID_WIDTH] = COLLISION_ROW_SUFFIX;
    
    final byte[][] qualifiers = new byte[collisions.size()][];
    final byte[][] values = new byte[collisions.size()][];

    int index = 0;
    for (Map.Entry<String, String> entry : collisions.entrySet()) {
      qualifiers[index] = new byte[COLLISION_PREFIX.length + 
                                        (entry.getKey().length() / 2)];
      System.arraycopy(COLLISION_PREFIX, 0, qualifiers[index], 0, 
          COLLISION_PREFIX.length);
      final byte[] tsuid = UniqueId.stringToUid(entry.getKey());
      System.arraycopy(tsuid, 0, qualifiers[index], 
          COLLISION_PREFIX.length, tsuid.length);

      values[index] = entry.getValue().getBytes(CHARSET);
      index++;
    }

    final PutRequest put = new PutRequest(tsdb.uidTable(), row_key, 
        NAME_FAMILY, qualifiers, values);
    collisions.clear();
    
    /**
     * Super simple callback used to convert the Deferred&lt;Object&gt; to a 
     * Deferred&lt;Boolean&gt; so that it can be grouped with other storage
     * calls
     */
    final class PutCB implements Callback<Deferred<Boolean>, Object> {

      @Override
      public Deferred<Boolean> call(Object result) throws Exception {
        return Deferred.fromResult(true);
      }
      
    }
      
    return tsdb.getClient().put(put).addCallbackDeferring(new PutCB());
  }

  /**
   * Attempts to flush the non-matches to storage. The storage call is a PUT so
   * it will overwrite any existing columns, but since each column is the TSUID
   * it should only exist once and the data shouldn't change.
   * <b>Note:</b> This will also clear the local {@link #not_matched} map
   * @param tsdb The TSDB to use for storage access
   * @return A meaningless deferred (will always be true since we need to group
   * it with tree store calls) for the caller to wait on
   * @throws HBaseException if there was an issue
   */
  private Deferred<Boolean> flushNotMatched(final TSDB tsdb) {
    final byte[] row_key = new byte[TREE_ID_WIDTH + 1];
    System.arraycopy(idToBytes(tree_id), 0, row_key, 0, TREE_ID_WIDTH);
    row_key[TREE_ID_WIDTH] = NOT_MATCHED_ROW_SUFFIX;

    final byte[][] qualifiers = new byte[not_matched.size()][];
    final byte[][] values = new byte[not_matched.size()][];
    
    int index = 0;
    for (Map.Entry<String, String> entry : not_matched.entrySet()) {
      qualifiers[index] = new byte[NOT_MATCHED_PREFIX.length + 
                                        (entry.getKey().length() / 2)];
      System.arraycopy(NOT_MATCHED_PREFIX, 0, qualifiers[index], 0, 
          NOT_MATCHED_PREFIX.length);
      final byte[] tsuid = UniqueId.stringToUid(entry.getKey());
      System.arraycopy(tsuid, 0, qualifiers[index], 
          NOT_MATCHED_PREFIX.length, tsuid.length);
      
      values[index] = entry.getValue().getBytes(CHARSET);
      index++;
    }
    
    final PutRequest put = new PutRequest(tsdb.uidTable(), row_key, 
        NAME_FAMILY, qualifiers, values);
    not_matched.clear();
    
    /**
     * Super simple callback used to convert the Deferred&lt;Object&gt; to a 
     * Deferred&lt;Boolean&gt; so that it can be grouped with other storage
     * calls
     */
    final class PutCB implements Callback<Deferred<Boolean>, Object> {

      @Override
      public Deferred<Boolean> call(Object result) throws Exception {
        return Deferred.fromResult(true);
      }
      
    }
      
    return tsdb.getClient().put(put).addCallbackDeferring(new PutCB());
  }

  // GETTERS AND SETTERS ----------------------------
  
  public static int TREE_ID_WIDTH() {
    return TREE_ID_WIDTH;
  }
  
  /** @return The treeId */
  public int getTreeId() {
    return tree_id;
  }

  /** @return The name of the tree */
  public String getName() {
    return name;
  }

  /** @return An optional description of the tree */
  public String getDescription() {
    return description;
  }

  /** @return Optional notes about the tree */
  public String getNotes() {
    return notes;
  }

  /** @return Whether or not strict matching is enabled */
  public boolean getStrictMatch() {
    return strict_match;
  }

  /** @return Whether or not the tree should process TSMeta objects */
  public boolean getEnabled() { 
    return enabled;
  }
  
  /** @return The tree's rule set */
  public Map<Integer, TreeMap<Integer, TreeRule>> getRules() {
    return rules;
  }

  /** @return List of TSUIDs that did not match any rules */
  @JsonIgnore
  public Map<String, String> getNotMatched() {
    return not_matched;
  }

  /** @return List of TSUIDs that were not stored due to collisions */
  @JsonIgnore
  public Map<String, String> getCollisions() {
    return collisions;
  }

  /** @return When the tree was created, Unix epoch in seconds */
  public long getCreated() {
    return created;
  }

  /** @param name A descriptive name for the tree */
  public void setName(String name) {
    if (!this.name.equals(name)) {
      changed.put("name", true);
      this.name = name;
    }
  }

  /** @param description A brief description of the tree */
  public void setDescription(String description) {
    if (!this.description.equals(description)) {
      changed.put("description", true);
      this.description = description;
    }
  }

  /** @param notes Optional notes about the tree */
  public void setNotes(String notes) {
    if (!this.notes.equals(notes)) {
      changed.put("notes", true);
      this.notes = notes;
    }
  }

  /** @param strict_match Whether or not a TSUID must match all rules in the
   * tree to be included */
  public void setStrictMatch(boolean strict_match) {
    if (this.strict_match != strict_match) {
      changed.put("strict_match", true);
      this.strict_match = strict_match;
    }
  }

  /** @param enabled Whether or not this tree should process TSMeta objects */
  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }
  
  /** @param treeId ID of the tree, users cannot modify this */
  public void setTreeId(int treeId) {
    this.tree_id = treeId;
  }

  /** @param created The time when this tree was created, 
   * Unix epoch in seconds */
  public void setCreated(long created) {
    this.created = created;
  }

}

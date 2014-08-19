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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.PatternSyntaxException;

import net.opentsdb.core.Const;
import net.opentsdb.utils.JSON;

import org.hbase.async.Bytes;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.core.JsonGenerator;

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
 * {@link net.opentsdb.meta.TSMeta} objects through the rule set using a 
 * {@link TreeBuilder}. Results are then stored in separate rows as branch 
 * and leaf objects.
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
  
  /** Charset used to convert Strings to byte arrays and back. */
  private static final Charset CHARSET = Charset.forName("ISO-8859-1");
  /** Width of tree IDs in bytes */
  private static final short TREE_ID_WIDTH = 2;
  /** Name of the CF where trees and branches are stored */
  private static final byte[] TREE_FAMILY = "t".getBytes(CHARSET);
  /** Integer width in bytes */
  private static final short INT_WIDTH = 4;
  /** Byte prefix for collision columns */
  private static byte[] COLLISION_PREFIX = "tree_collision:".getBytes(CHARSET);
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

  /** Whether or not to store not matched and collisions */
  private boolean store_failures;
  
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
  
  /**
   * Copy constructor that creates a completely independent copy of the original
   * object.
   * @param original The original object to copy from
   * @throws PatternSyntaxException if one of the rule's regex is invalid
   */
  public Tree(final Tree original) {
    created = original.created;
    description = original.description;
    enabled = original.enabled;
    store_failures = original.store_failures;
    name = original.name;
    notes = original.notes;
    strict_match = original.strict_match;
    tree_id = original.tree_id;
    
    // deep copy rules
    rules = new TreeMap<Integer, TreeMap<Integer, TreeRule>>();
    for (Map.Entry<Integer, TreeMap<Integer, TreeRule>> level : 
      original.rules.entrySet()) {
      
      final TreeMap<Integer, TreeRule> orders = new TreeMap<Integer, TreeRule>();
      for (final TreeRule rule : level.getValue().values()) {
        orders.put(rule.getOrder(), new TreeRule(rule));
      }
      
      rules.put(level.getKey(), orders);
    }
    
    // copy collisions and not matched
    if (original.collisions != null) {
      collisions = new HashMap<String, String>(original.collisions);
    }
    if (original.not_matched != null) {
      not_matched = new HashMap<String, String>(original.not_matched);
    }
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
    if (overwrite || tree.changed.get("enabled")) {
      enabled = tree.enabled;
      changed.put("enabled", true);
    }
    if (overwrite || tree.changed.get("store_failures")) {
      store_failures = tree.store_failures;
      changed.put("store_failures", true);
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
   * Converts the tree ID into a byte array {@link #TREE_ID_WIDTH} in size
   * @param tree_id The tree ID to convert
   * @return The tree ID as a byte array
   * @throws IllegalArgumentException if the Tree ID is invalid
   */
  public static byte[] idToBytes(final int tree_id) {
    validateTreeID(tree_id);
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
  
  /** @return The family to use when storing tree data */
  public static byte[] TREE_FAMILY() {
    return TREE_FAMILY;
  }
  
  /**
   * Sets or resets the changed map flags
   */
  public void initializeChangedMap() {
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
    changed.put("enabled", false);
    changed.put("store_failures", false);
  }
  
  /**
   * Converts the object to a JSON byte array, necessary for CAS calls and to
   * keep redundant data down
   * @return A byte array with the serialized tree
   */
  public byte[] toStorageJson() {
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
      json.writeBooleanField("storeFailures", store_failures);
      json.writeEndObject();
      json.close();
      
      // TODO zero copy?
      return output.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  // GETTERS AND SETTERS ----------------------------
  
  /** @return The width of the tree ID in bytes */
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

  /** @return Whether or not to store not matched and collisions */
  public boolean getStoreFailures() {
    return store_failures;
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
    changed.put("strict_match", true);
    this.strict_match = strict_match;    
  }

  /** @param enabled Whether or not this tree should process TSMeta objects */
  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
    changed.put("enabled", true);
  }
  
  /** @param store_failures Whether or not to store not matched or collisions */
  public void setStoreFailures(boolean store_failures) {
    this.store_failures = store_failures;
    changed.put("store_failures", true);
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

  public boolean hasChanged () {
    //check if anything has changed
    for (Map.Entry<String, Boolean> entry : changed.entrySet()) {
      if (entry.getValue()) {
        return true;
      }
    }
  return false;
  }

  public static void validateTreeID(int id) {
    //magic numbers? maybe moved to constant?
    if (id < Const.MIN_TREE_ID_EXCLUSIVE || id > Const.MAX_TREE_ID_EXCLUSIVE) {
      throw new IllegalArgumentException("Invalid Tree ID");
    }
  }
}

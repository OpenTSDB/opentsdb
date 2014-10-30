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
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.xml.bind.DatatypeConverter;

import net.opentsdb.core.Const;
import org.hbase.async.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.core.JsonGenerator;

import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.JSON;

/**
 * Represents a branch of a meta data tree, used to organize timeseries into 
 * a hierarchy for easy navigation. Each branch is composed of itself and
 * potential child branches and/or child leaves. 
 * <p>
 * Branch IDs are hex encoded byte arrays composed of the tree ID + hash of 
 * the display name for each previous branch. The tree ID is encoded on 
 * {@link Tree#TREE_ID_WIDTH()} bytes, each hash is then {@code INT_WIDTH} 
 * bytes. So the if the tree ID width is 2 bytes and Java Integers are 4 bytes, 
 * the root for tree # 1 is just {@code 0001}. A child of the root could be 
 * {@code 00001A3B190C2} and so on. These IDs are used as the row key in storage.
 * <p>
 * Branch definitions are JSON objects stored in the "branch" column of the 
 * branch ID row. Only the tree ID, path and display name are stored in the
 * definition column to keep space down. Leaves are stored in separate columns
 * and child branch definitions are stored in separate rows. Note that the root
 * branch definition for a tree will be stored in the same row as the tree 
 * definition since they share the same row key.
 * <p>
 * When fetching a branch with children and leaves, a scanner is
 * configured with a row key regex to scan any rows that match the branch ID 
 * plus an additional {@code INT_WIDTH} so that when we scan, we can pick up all
 * of the rows with child branch definitions. Also, when loading a full branch, 
 * any leaves for the request branch can load the associated UID names from 
 * storage, so this can get expensive. Leaves for a child branch will not be 
 * loaded, only leaves that belong directly to the local will. Also, children 
 * branches of children will not be loaded. We only return one branch at a 
 * time since the tree could be HUGE!
 * <p>
 * Storing a branch will only write the definition column for the local branch
 * object. Child branches will not be written to storage. If you've loaded
 * and modified children in this branch, you need to loop through the children
 * and store them individually. Leaves belonging to this branch will be stored
 * and collisions recorded to the given Tree object.
 * @since 2.0
 */
@JsonIgnoreProperties(ignoreUnknown = true) 
@JsonAutoDetect(fieldVisibility = Visibility.PUBLIC_ONLY)
public final class Branch implements Comparable<Branch> {
  private static final Logger LOG = LoggerFactory.getLogger(Branch.class);
  
  /** Charset used to convert Strings to byte arrays and back. */
  private static final Charset CHARSET = Charset.forName("ISO-8859-1");
  /** Name of the branch qualifier ID */
  private static final byte[] BRANCH_QUALIFIER = "branch".getBytes(CHARSET);
  
  /** The tree this branch belongs to */
  private int tree_id;

  /** Display name for the branch */
  private String display_name = "";

  /** Hash map of leaves belonging to this branch */
  private HashMap<Integer, Leaf> leaves;
  
  /** Hash map of child branches */
  private TreeSet<Branch> branches;

  /** The path/name of the branch */
  private TreeMap<Integer, String> path;
  
  /**
   * Default empty constructor necessary for de/serialization
   */
  public Branch() {

  }

  /**
   * Constructor that sets the tree ID
   * @param tree_id ID of the tree this branch is associated with
   */
  public Branch(final int tree_id) {
    this.tree_id = tree_id;
  }
  
  /**
   * Copy constructor that creates a completely independent copy of the original
   * @param original The original object to copy from
   */
  public Branch(final Branch original) {
    tree_id = original.tree_id;
    display_name = original.display_name;
    if (original.leaves != null) {
      leaves = new HashMap<Integer, Leaf>(original.leaves);
    }
    if (original.branches != null) {
      branches = new TreeSet<Branch>(original.branches);
    }
    if (original.path != null) {
      path = new TreeMap<Integer, String>(original.path);
    }
  }
  
  /** @return Returns the {@code display_name}'s hash code or 0 if it's not set */
  @Override
  public int hashCode() {
    if (display_name == null || display_name.isEmpty()) {
      return 0;
    }
    return display_name.hashCode();
  }
  
  /**
   * Just compares the branch display name
   * @param obj The object to compare this to
   * @return True if the branch IDs are the same or the incoming object is 
   * this one
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
    
    final Branch branch = (Branch)obj;
    return display_name.equals(branch.display_name);
  }
  
  /**
   * Comparator based on the {@code display_name} to sort branches when 
   * returning to an RPC calls
   */
  @Override
  public int compareTo(Branch branch) {
    return this.display_name.compareToIgnoreCase(branch.display_name);
  }
  
  /** @return Information about this branch including ID and display name */
  @Override
  public String toString() {
    if (path == null) {
      return "Name: [" + display_name + "]";
    } else {
      return "ID: [" + getBranchId() + "] Name: [" + display_name + "]";
    }
  }
  
  /**
   * Adds a child branch to the local branch set if it doesn't exist. Also
   * initializes the set if it hasn't been initialized yet
   * @param branch The branch to add
   * @return True if the branch did not exist in the set previously
   * @throws IllegalArgumentException if the incoming branch is null
   */
  public boolean addChild(final Branch branch) {
    if (branch == null) {
      throw new IllegalArgumentException("Null branches are not allowed");
    }
    if (branches == null) {
      branches = new TreeSet<Branch>();
      branches.add(branch);
      return true;
    }
    
    if (branches.contains(branch)) {
      return false;
    }
    branches.add(branch);
    return true;
  }
  
  /**
   * Adds a leaf to the local branch, looking for collisions
   * @param leaf The leaf to add
   * @param tree The tree to report to with collisions
   * @return True if the leaf was new, false if the leaf already exists or 
   * would cause a collision
   * @throws IllegalArgumentException if the incoming leaf is null
   */
  public boolean addLeaf(final Leaf leaf, final Tree tree) {
    if (leaf == null) {
      throw new IllegalArgumentException("Null leaves are not allowed");
    }
    if (leaves == null) {
      leaves = new HashMap<Integer, Leaf>();
      leaves.put(leaf.hashCode(), leaf);
      return true;
    }
    
    if (leaves.containsKey(leaf.hashCode())) {
      // if we try to sync a leaf with the same hash of an existing key
      // but a different TSUID, it's a collision, so mark it
      if (!leaves.get(leaf.hashCode()).getTsuid().equals(leaf.getTsuid())) {
        final Leaf collision = leaves.get(leaf.hashCode());
        if (tree != null) {
          tree.addCollision(leaf.getTsuid(), collision.getTsuid());
        }
        
        // log at info or lower since it's not a system error, rather it's
        // a user issue with the rules or naming schema
        LOG.warn("Incoming TSUID [" + leaf.getTsuid() + 
            "] collided with existing TSUID [" + collision.getTsuid() + 
            "] on display name [" + collision.getDisplayName() + "]");
      }
      return false;
    } else {
      leaves.put(leaf.hashCode(), leaf);
      return true;
    }
  }
  
  /**
   * Attempts to compile the branch ID for this branch. In order to successfully
   * compile, the {@code tree_id}, {@code path} and {@code display_name} must
   * be set. The path may be empty, which indicates this is a root branch, but
   * it must be a valid Map object.
   * @return The branch ID as a byte array
   * @throws IllegalArgumentException if any required parameters are missing
   */
  public byte[] compileBranchId() {
    Tree.validateTreeID(this.getTreeId());
    // root branch path may be empty
    if (path == null) {
      throw new IllegalArgumentException("Missing branch path");
    }
    if (display_name == null || display_name.isEmpty()) {
      throw new IllegalArgumentException("Missing display name");
    }
    
    // first, make sure the display name is at the tip of the tree set
    if (path.isEmpty()) {
      path.put(0, display_name);
    } else if (!path.lastEntry().getValue().equals(display_name)) {
      final int depth = path.lastEntry().getKey() + 1;
      path.put(depth, display_name);
    }
    
    final byte[] branch_id = new byte[Tree.TREE_ID_WIDTH() + 
                                      ((path.size() - 1) * Const.INT_WIDTH)];
    int index = 0;
    final byte[] tree_bytes = Tree.idToBytes(tree_id);
    System.arraycopy(tree_bytes, 0, branch_id, index, tree_bytes.length);
    index += tree_bytes.length;
    
    for (Map.Entry<Integer, String> entry : path.entrySet()) {
      // skip the root, keeps the row keys 4 bytes shorter
      if (entry.getKey() == 0) {
        continue;
      }
      
      final byte[] hash = Bytes.fromInt(entry.getValue().hashCode());
      System.arraycopy(hash, 0, branch_id, index, hash.length);
      index += hash.length;
    }
    
    return branch_id;
  }
  
  /**
   * Sets the path for this branch based off the path of the parent. This map
   * may be empty, in which case the branch is considered a root.
   * <b>Warning:</b> If the path has already been set, this will create a new
   * path, clearing out any existing entries
   * @param parent_path The map to store as the path
   * @throws IllegalArgumentException if the parent path is null
   */
  public void prependParentPath(final Map<Integer, String> parent_path) {
    if (parent_path == null) {
      throw new IllegalArgumentException("Parent path was null");
    }
    path = new TreeMap<Integer, String>();
    path.putAll(parent_path);
  }

  /**
   * Converts a branch ID hash to a hex encoded, upper case string with padding
   * @param branch_id The ID to convert
   * @return the branch ID as a character hex string
   */
  public static String idToString(final byte[] branch_id) {
    return DatatypeConverter.printHexBinary(branch_id);
  }
  
  /**
   * Converts a hex string to a branch ID byte array (row key)
   * @param branch_id The branch ID to convert
   * @return The branch ID as a byte array
   * @throws IllegalArgumentException if the string is not valid hex
   */
  public static byte[] stringToId(final String branch_id) {
    if (branch_id == null || branch_id.isEmpty()) {
      throw new IllegalArgumentException("Branch ID was empty");
    }
    if (branch_id.length() < 4) {
      throw new IllegalArgumentException("Branch ID was too short");
    }
    String id = branch_id;
    if (id.length() % 2 != 0) {
      id = "0" + id;
    }
    return DatatypeConverter.parseHexBinary(id);
  }

  /** @return The branch column qualifier name */
  public static byte[] BRANCH_QUALIFIER() {
    return BRANCH_QUALIFIER;
  }
 
  /**
   * Returns serialized data for the branch to put in storage. This is necessary
   * to reduce storage space and for proper CAS calls
   * @return A byte array for storage
   */
  public byte[] toStorageJson() {
    // grab some memory to avoid reallocs
    final ByteArrayOutputStream output = new ByteArrayOutputStream(
        (display_name.length() * 2) + (path.size() * 128));
    try {
      final JsonGenerator json = JSON.getFactory().createGenerator(output);
      
      json.writeStartObject();
      
      // we only need to write a small amount of information
      json.writeObjectField("path", path);
      json.writeStringField("displayName", display_name);
      
      json.writeEndObject();
      json.close();
      
      // TODO zero copy?
      return output.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  
  // GETTERS AND SETTERS ----------------------------
  
  /** @return The ID of the tree this branch belongs to */
  public int getTreeId() {
    return tree_id;
  }

  /** @return The ID of this branch */
  public String getBranchId() {
    final byte[] id = compileBranchId();
    if (id == null) {
      return null;
    }
    return UniqueId.uidToString(id);
  }
  
  /** @return The path of the tree */
  public Map<Integer, String> getPath() {
    compileBranchId();
    return path;
  }

  /** @return Depth of this branch */
  public int getDepth() {
    return path.lastKey();
  }

  /** @return Name to display to the public */
  public String getDisplayName() {
    return display_name;
  }

  /** @return Ordered set of leaves belonging to this branch */
  public TreeSet<Leaf> getLeaves() {
    if (leaves == null) {
      return null;
    }
    return new TreeSet<Leaf>(leaves.values());
  }

  /** @return Ordered set of child branches */
  public TreeSet<Branch> getBranches() {
    return branches;
  }

  /** @param tree_id ID of the tree this branch belongs to */
  public void setTreeId(int tree_id) {
    this.tree_id = tree_id;
  }
  
  /** @param display_name Public name to display */
  public void setDisplayName(String display_name) {
    this.display_name = display_name;
  }

  public void setPath( final Map<Integer, String> path) {
    if (path != null) {
      this.path = new TreeMap<Integer, String>(path);
    }
  }

  public void addLeaf(Leaf leaf) {

    if (this.leaves == null) {
      leaves = new HashMap<Integer, Leaf>();
    }
    leaves.put(leaf.hashCode(), leaf);
  }

 }

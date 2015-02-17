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

import java.nio.charset.Charset;

import com.google.common.collect.ImmutableMap;
import net.opentsdb.storage.hbase.HBaseConst;
import org.hbase.async.Bytes;

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
  /** ASCII Leaf prefix */
  private static final byte[] LEAF_PREFIX = "leaf:".getBytes(HBaseConst.CHARSET);

  /** The metric associated with this TSUID */
  private String metric = "";
  
  /** The tags associated with this TSUID for API response purposes */
  private ImmutableMap<String, String> tags = null;

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


  
  /** @return The configured leaf column prefix */
  public static byte[] LEAF_PREFIX() {
    return LEAF_PREFIX;
  }
  
  // GETTERS AND SETTERS ----------------------------

  /** @return The metric associated with this TSUID */
  public String getMetric() {
    return metric;
  }

  /** @return The tags associated with this TSUID */
  public ImmutableMap<String, String> getTags() {
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
  
  /**
   * @param tags The tags associated with this TSUID  */
  public void setTags(final ImmutableMap<String, String> tags) {
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

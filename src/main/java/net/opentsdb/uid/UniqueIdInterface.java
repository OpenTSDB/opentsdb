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
package net.opentsdb.uid;

import org.hbase.async.HBaseException;

/**
 * Represents a table of Unique IDs, manages the lookup and creation of IDs.
 *
 * For efficiency, various kinds of "names" need to be mapped to small, unique
 * IDs.  For instance, we give a unique ID to each metric name, to each tag
 * name, to each tag value.
 * <p>
 * An instance of this class handles the unique IDs for one kind of ID.  For
 * example:
 *   <pre>
 *   UniqueId metric_names = ...;
 *   byte[] id = metric_names.get("sys.net.rx_bytes");
 *   </pre>
 *
 * IDs are looked up in HBase and cached forever in memory (since they're
 * immutable).  IDs are encoded on a fixed number of bytes, which is
 * implementation dependent.
 */
public interface UniqueIdInterface {

  /**
   * Returns what kind of Unique ID is served by this instance.
   */
  String kind();

  /**
   * Returns the number of bytes on which each Unique ID is encoded.
   */
  short width();

  /**
   * Finds the name associated with a given ID.
   *
   * @param id The ID associated with that name.
   * @see #getId(String)
   * @see #getOrCreateId(String)
   * @throws NoSuchUniqueId if the given ID is not assigned.
   * @throws HBaseException if there is a problem communicating with HBase.
   * @throws IllegalArgumentException if the ID given in argument is encoded
   * on the wrong number of bytes.
   */
  String getName(byte[] id) throws NoSuchUniqueId, HBaseException;

  /**
   * Finds the ID associated with a given name.
   * <p>
   * The length of the byte array is fixed in advance by the implementation.
   *
   * @param name The name to lookup in the table.
   * @see #getName(byte[])
   * @return A non-null, non-empty {@code byte[]} array.
   * @throws NoSuchUniqueName if the name requested doesn't have an ID assigned.
   * @throws HBaseException if there is a problem communicating with HBase.
   * @throws IllegalStateException if the ID found in HBase is encoded on the
   * wrong number of bytes.
   */
  byte[] getId(String name) throws NoSuchUniqueName, HBaseException;

  /**
   * Finds the ID associated with a given name or creates it.
   * <p>
   * The length of the byte array is fixed in advance by the implementation.
   *
   * @param name The name to lookup in the table or to assign an ID to.
   * @throws HBaseException if there is a problem communicating with HBase.
   * @throws IllegalStateException if all possible IDs are already assigned.
   * @throws IllegalStateException if the ID found in HBase is encoded on the
   * wrong number of bytes.
   */
  byte[] getOrCreateId(String name) throws HBaseException, IllegalStateException;

}

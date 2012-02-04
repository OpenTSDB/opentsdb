// This file is part of OpenTSDB.
// Copyright (C) 2011-2012  The OpenTSDB Authors.
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
package net.opentsdb.core;

import java.util.ArrayList;
import java.util.Map;

import org.hbase.async.Bytes;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <strong>This class is not part of the public API.</strong>
 * <p><pre>
 * ,____________________________,
 * | This class is reserved for |
 * | OpenTSDB's internal usage! |
 * `----------------------------'
 *       \                   / \  //\
 *        \    |\___/|      /   \//  \\
 *             /0  0  \__  /    //  | \ \
 *            /     /  \/_/    //   |  \  \
 *            @_^_@'/   \/_   //    |   \   \
 *            //_^_/     \/_ //     |    \    \
 *         ( //) |        \///      |     \     \
 *       ( / /) _|_ /   )  //       |      \     _\
 *     ( // /) '/,_ _ _/  ( ; -.    |    _ _\.-~        .-~~~^-.
 *   (( / / )) ,-{        _      `-.|.-~-.           .~         `.
 *  (( // / ))  '/\      /                 ~-. _ .-~      .-~^-.  \
 *  (( /// ))      `.   {            }                   /      \  \
 *   (( / ))     .----~-.\        \-'                 .~         \  `. \^-.
 *              ///.----../        \             _ -~             `.  ^-`  ^-_
 *                ///-._ _ _ _ _ _ _}^ - - - - ~                     ~-- ,.-~
 *                                                                   /.-~
 *              You've been warned by the dragon!
 * </pre><p>
 * This class is reserved for OpenTSDB's own internal usage only.  If you use
 * anything from this package outside of OpenTSDB, a dragon will spontaneously
 * appear and eat you.  You've been warned.
 * <p>
 * This class only exists because Java's packaging system is annoying as the
 * "package-private" accessibility level only applies to the current package
 * but not its sub-packages,  and because Java doesn't have fine-grained API
 * visibility mechanism such as that of Scala or C++.
 * <p>
 * This package provides access into internal methods for higher-level
 * packages, for the sake of reducing code duplication and (ab)use of
 * reflection.
 */
public final class Internal {

  /** @see Const#FLAG_BITS  */
  public static final short FLAG_BITS = Const.FLAG_BITS;

  /** @see Const#LENGTH_MASK  */
  public static final short LENGTH_MASK = Const.LENGTH_MASK;

  /** @see Const#FLAGS_MASK  */
  public static final short FLAGS_MASK = Const.FLAGS_MASK;

  private Internal() {
    // Can't instantiate.
  }

  /** @see TsdbQuery#getScanner */
  public static Scanner getScanner(final Query query) {
    return ((TsdbQuery) query).getScanner();
  }

  /** @see RowKey#metricName */
  public static String metricName(final TSDB tsdb, final byte[] id) {
    return RowKey.metricName(tsdb, id);
  }

  /** Extracts the timestamp from a row key.  */
  public static long baseTime(final TSDB tsdb, final byte[] row) {
    return Bytes.getUnsignedInt(row, tsdb.metrics.width());
  }

  /** @see Tags#getTags */
  public static Map<String, String> getTags(final TSDB tsdb, final byte[] row) {
    return Tags.getTags(tsdb, row);
  }

  /** @see RowSeq#extractIntegerValue */
  public static long extractIntegerValue(final byte[] values,
                                         final int value_idx,
                                         final byte flags) {
    return RowSeq.extractIntegerValue(values, value_idx, flags);
  }

  /** @see RowSeq#extractFloatingPointValue */
  public static double extractFloatingPointValue(final byte[] values,
                                                 final int value_idx,
                                                 final byte flags) {
    return RowSeq.extractFloatingPointValue(values, value_idx, flags);
  }

  public static short metricWidth(final TSDB tsdb) {
    return tsdb.metrics.width();
  }

  /** @see CompactionQueue#complexCompact  */
  public static KeyValue complexCompact(final KeyValue kv) {
    final ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(1);
    kvs.add(kv);
    return CompactionQueue.complexCompact(kvs, kv.qualifier().length / 2);
  }

}

// This file is part of OpenTSDB.
// Copyright (C) 2010  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.tools;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import java.util.Map;

import org.hbase.async.Bytes;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;

import net.opentsdb.uid.UniqueId;
import net.opentsdb.core.Query;
import net.opentsdb.core.TSDB;

/**
 * Gives access private implementation details from the `core' package.
 * The tools in this Java package need access to implementation details
 * of things in the `core' package.  Since those are implementation
 * details, they're not part of the interface exported by `core'.  Yet,
 * we want to access them and I believe this is legitimate as we're part
 * of the same project, so we access them anyway using reflection.
 * I think this is better than marking those public and adding a javadoc
 * comment "THIS IS INTERNAL DO NOT USE".  If only Java had C++'s
 * "friend" or a less stupid notion of a package...
 */
final class Core {

  /** Access to the package-private class that implements {@link Query}. */
  static final Class<?> TsdbQuery;
  /** Access to the private method that creates the HBase scanner. */
  static final Method getScanner;
  static {
    try {
      TsdbQuery = Class.forName("net.opentsdb.core.TsdbQuery");
      getScanner = TsdbQuery.getDeclaredMethod("getScanner");
      getScanner.setAccessible(true);
    } catch (Exception e) {
      throw new RuntimeException("static initializer failed", e);
    }
  }

  static Scanner getScanner(final Query query) {
    try {
      return (Scanner) getScanner.invoke(query);
    } catch (Exception e) {
      throw new RuntimeException("getScanner=" + getScanner, e);
    }
  }

  /** Access to the package-private helper class for row keys. */
  static final Class<?> RowKey;
  /** Access to the private method that extract the name of a metric. */
  static final Method metricName;
  static {
    try {
      RowKey = Class.forName("net.opentsdb.core.RowKey");
      metricName = RowKey.getDeclaredMethod("metricName",
                                            TSDB.class, byte[].class);
      metricName.setAccessible(true);
    } catch (Exception e) {
      throw new RuntimeException("static initializer failed", e);
    }
  }

  static long baseTime(final TSDB tsdb, final byte[] row) {
    try {
      final Field metrics = TSDB.class.getDeclaredField("metrics");
      metrics.setAccessible(true);
      final short metric_width = ((UniqueId) metrics.get(tsdb)).width();
      return Bytes.getUnsignedInt(row, metric_width);
    } catch (Exception e) {
      throw new RuntimeException("in baseTime", e);
    }
  }

  static String metricName(final TSDB tsdb, final byte[] row) {
    try {
      return (String) metricName.invoke(null, tsdb, row);
    } catch (Exception e) {
      throw new RuntimeException("metricName=" + metricName, e);
    }
  }

  /** Access to the package-private helper class for tags. */
  static final Class<?> Tags;
  /** Access to the private method that extract the tags. */
  static final Method getTags;
  static {
    try {
      Tags = Class.forName("net.opentsdb.core.Tags");
      getTags = Tags.getDeclaredMethod("getTags", TSDB.class, byte[].class);
      getTags.setAccessible(true);
    } catch (Exception e) {
      throw new RuntimeException("static initializer failed", e);
    }
  }

  @SuppressWarnings("unchecked")
  static Map<String, String> getTags(final TSDB tsdb, final byte[] row) {
    try {
      return (Map<String, String>) getTags.invoke(null, tsdb, row);
    } catch (Exception e) {
      throw new RuntimeException("getTags=" + getTags, e);
    }
  }

  static final Class<?> RowSeq;
  static final Method extractLValue;
  static {
    try {
      RowSeq = Class.forName("net.opentsdb.core.RowSeq");
      extractLValue = RowSeq.getDeclaredMethod("extractLValue",
                                               short.class, KeyValue.class);
      extractLValue.setAccessible(true);
    } catch (Exception e) {
      throw new RuntimeException("static initializer failed", e);
    }
  }

  static long extractLValue(final short qualifier, final KeyValue kv) {
    try {
      return (Long) extractLValue.invoke(null, qualifier, kv);
    } catch (Exception e) {
      throw new RuntimeException("extractLValue=" + extractLValue, e);
    }
  }

}

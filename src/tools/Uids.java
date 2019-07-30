// This file is part of OpenTSDB.
// Copyright (C) 2014  The OpenTSDB Authors.
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
package net.opentsdb.tools;

import java.util.ArrayList;
import java.util.HashMap;

import org.slf4j.Logger;

import org.hbase.async.DeleteRequest;
import org.hbase.async.KeyValue;
import org.hbase.async.HBaseClient;
import org.hbase.async.HBaseException;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;

import net.opentsdb.meta.TSMeta;
import net.opentsdb.uid.UniqueId;

/**
 * Utility class for fsck and garbage collection of UIDs.
 *
 * Stores mapping of name to UID and vice versa for particular kind.
 */

final class Uids {
  /* Number of errors found. */
  int errors;
  /* Highest possible UID. */
  long maxid;
  /* Found highest UID. */
  long max_found_id;
  /* Width in bytes of UIDs. */
  short width;
  final HashMap<String, String> id2name = new HashMap<String, String>();
  final HashMap<String, String> name2id = new HashMap<String, String>();
  Logger log;
  byte[] table;
  HBaseClient client;

  Uids(HBaseClient client, byte[] table, Logger log) {
    this.client = client;
    this.log = log;
    this.table = table;
  }

  void error(final KeyValue kv, final String msg) {
    error(msg + ".  kv=" + kv);
  }

  void error(final String msg) {
    log.error(msg);
    errors++;
  }

  
  /**
   * Replaces or creates the reverse map in storage and in the local map
   */
  void restoreReverseMap(final String kind, final String name, 
      final String uid) {
    final PutRequest put = new PutRequest(table, 
        UniqueId.stringToUid(uid), CliUtils.NAME_FAMILY, CliUtils.toBytes(kind), 
        CliUtils.toBytes(name));
    client.put(put);
    id2name.put(uid, name);
    log.info("FIX: Restoring " + kind + " reverse mapping: " 
        + uid + " -> " + name);
  }

  /**
   * Removes the reverse map from storage only
   */
  void removeReverseMap(final String kind, final String name, 
      final String uid) {
    // clean up meta data too
    final byte[][] qualifiers = new byte[2][];
    qualifiers[0] = CliUtils.toBytes(kind); 
    if (Bytes.equals(CliUtils.METRICS, qualifiers[0])) {
      qualifiers[1] = CliUtils.METRICS_META;
    } else if (Bytes.equals(CliUtils.TAGK, qualifiers[0])) {
      qualifiers[1] = CliUtils.TAGK_META;
    } else if (Bytes.equals(CliUtils.TAGV, qualifiers[0])) {
      qualifiers[1] = CliUtils.TAGV_META;
    }

    final DeleteRequest delete = new DeleteRequest(table, 
        UniqueId.stringToUid(uid), CliUtils.NAME_FAMILY, qualifiers);
    client.delete(delete);
    // can't remove from the id2name map as this will be called while looping
    log.info("FIX: Removed " + kind + " reverse mapping: " + uid + " -> "
        + name);
  }

  /**
   * Return mapping from kind (metric/tagk/tagv) to its Uids.
   */
  static HashMap<String, Uids> loadUids(final HBaseClient client,
                                        final byte[] table,
                                        final Logger log,
                                        final boolean fix,
                                        final boolean fix_unknowns) {
    final long start_time = System.nanoTime();
    final HashMap<String, Uids> name2uids = new HashMap<String, Uids>();
    final Scanner scanner = client.newScanner(table);
    scanner.setMaxNumRows(1024);
    int kvcount = 0;
    try {
      ArrayList<ArrayList<KeyValue>> rows;
      while ((rows = scanner.nextRows().joinUninterruptibly()) != null) {
        for (final ArrayList<KeyValue> row : rows) {
          for (final KeyValue kv : row) {
            kvcount++;
            final byte[] qualifier = kv.qualifier();
            // TODO - validate meta data in the future, for now skip it
            if (Bytes.equals(qualifier, TSMeta.META_QUALIFIER()) ||
                Bytes.equals(qualifier, TSMeta.COUNTER_QUALIFIER()) ||
                Bytes.equals(qualifier, CliUtils.METRICS_META) ||
                Bytes.equals(qualifier, CliUtils.TAGK_META) ||
                Bytes.equals(qualifier, CliUtils.TAGV_META)) {
              continue;
            }

            if (!Bytes.equals(qualifier, CliUtils.METRICS) &&
                !Bytes.equals(qualifier, CliUtils.TAGK) &&
                !Bytes.equals(qualifier, CliUtils.TAGV)) {
              log.warn("Unknown qualifier " + UniqueId.uidToString(qualifier) 
                  + " in row " + UniqueId.uidToString(kv.key()));
              if (fix && fix_unknowns) {
                final DeleteRequest delete = new DeleteRequest(table, kv.key(), 
                    kv.family(), qualifier);
                client.delete(delete);
                log.info("FIX: Removed unknown qualifier " 
                  + UniqueId.uidToString(qualifier) 
                  + " in row " + UniqueId.uidToString(kv.key()));
              }
              continue;
            }

            final String kind = CliUtils.fromBytes(kv.qualifier());
            Uids uids = name2uids.get(kind);
            if (uids == null) {
              uids = new Uids(client, table, log);
              name2uids.put(kind, uids);
            }
            final byte[] key = kv.key();
            final byte[] family = kv.family();
            final byte[] value = kv.value();
            if (Bytes.equals(key, CliUtils.MAXID_ROW)) {
              if (value.length != 8) {
                uids.error(kv, "Invalid maximum ID for " + kind
                           + ": should be on 8 bytes: ");
                // TODO - a fix would be to find the max used ID for the type 
                // and store that in the max row.
              } else {
                uids.maxid = Bytes.getLong(value);
                log.info("Maximum ID for " + kind + ": " + uids.maxid);
              }
            } else {
              short idwidth = 0;
              if (Bytes.equals(family, CliUtils.ID_FAMILY)) {
                idwidth = (short) value.length;
                final String skey = CliUtils.fromBytes(key);
                final String svalue = UniqueId.uidToString(value);
                final long max_found_id;
                if (Bytes.equals(qualifier, CliUtils.METRICS)) {
                  max_found_id = UniqueId.uidToLong(value, TSDB.metrics_width());
                } else if (Bytes.equals(qualifier, CliUtils.TAGK)) {
                  max_found_id = UniqueId.uidToLong(value, TSDB.tagk_width());
                } else {
                  max_found_id = UniqueId.uidToLong(value, TSDB.tagv_width());
                }
                if (uids.max_found_id < max_found_id) {
                  uids.max_found_id = max_found_id;
                }
                final String id = uids.name2id.put(skey, svalue);
                if (id != null) {
                  uids.error(kv, "Duplicate forward " + kind + " mapping: "
                             + skey + " -> " + id
                             + " and " + skey + " -> " + svalue);
                }
              } else if (Bytes.equals(family, CliUtils.NAME_FAMILY)) {
                final String skey = UniqueId.uidToString(key);
                final String svalue = CliUtils.fromBytes(value);
                idwidth = (short) key.length;
                final String name = uids.id2name.put(skey, svalue);
                if (name != null) {
                  uids.error(kv, "Duplicate reverse " + kind + "  mapping: "
                             + svalue + " -> " + name
                             + " and " + svalue + " -> " + skey);
                }
              }
              if (uids.width == 0) {
                uids.width = idwidth;
              } else if (uids.width != idwidth) {
                uids.error(kv, "Invalid " + kind + " ID of length " + idwidth
                           + " (expected: " + uids.width + ')');
              }
            }
          }
        }
      }
    } catch (HBaseException e) {
      log.error("Error while scanning HBase, scanner=" + scanner, e);
      throw e;
    } catch (Exception e) {
      log.error("WTF?  Unexpected exception type, scanner=" + scanner, e);
      throw new AssertionError("Should never happen");
    }
    return name2uids;
  }

}

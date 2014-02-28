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
package net.opentsdb.tools;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.hbase.async.Bytes;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.HBaseException;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;

import net.opentsdb.core.TSDB;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.Config;

/**
 * Command line tool to manipulate UIDs.
 * Can be used to find or assign UIDs.
 */
final class UidManager {

  private static final Logger LOG = LoggerFactory.getLogger(UidManager.class);

  /** Function used to convert a String to a byte[]. */
  private static final Method toBytes;
  /** Function used to convert a byte[] to a String. */
  private static final Method fromBytes;
  /** Charset used to convert Strings to byte arrays and back. */
  private static final Charset CHARSET;
  /** The single column family used by this class. */
  private static final byte[] ID_FAMILY;
  /** The single column family used by this class. */
  private static final byte[] NAME_FAMILY;
  /** Row key of the special row used to track the max ID already assigned. */
  private static final byte[] MAXID_ROW;
  static {
    final Class<UniqueId> uidclass = UniqueId.class;
    try {
      // Those are all implementation details so they're not part of the
      // interface.  We access them anyway using reflection.  I think this
      // is better than marking those public and adding a javadoc comment
      // "THIS IS INTERNAL DO NOT USE".  If only Java had C++'s "friend" or
      // a less stupid notion of a package.
      Field f;
      f = uidclass.getDeclaredField("CHARSET");
      f.setAccessible(true);
      CHARSET = (Charset) f.get(null);
      f = uidclass.getDeclaredField("ID_FAMILY");
      f.setAccessible(true);
      ID_FAMILY = (byte[]) f.get(null);
      f = uidclass.getDeclaredField("NAME_FAMILY");
      f.setAccessible(true);
      NAME_FAMILY = (byte[]) f.get(null);
      f = uidclass.getDeclaredField("MAXID_ROW");
      f.setAccessible(true);
      MAXID_ROW = (byte[]) f.get(null);
      toBytes = uidclass.getDeclaredMethod("toBytes", String.class);
      toBytes.setAccessible(true);
      fromBytes = uidclass.getDeclaredMethod("fromBytes", byte[].class);
      fromBytes.setAccessible(true);
    } catch (Exception e) {
      throw new RuntimeException("static initializer failed", e);
    }
  }

  /** Prints usage. */
  static void usage(final String errmsg) {
    usage(null, errmsg);
  }

  /** Prints usage. */
  static void usage(final ArgP argp, final String errmsg) {
    System.err.println(errmsg);
    System.err.println("Usage: uid <subcommand> args\n"
        + "Sub commands:\n"
        + "  grep [kind] <RE>: Finds matching IDs.\n"
        + "  assign <kind> <name> [names]:"
        + " Assign an ID for the given name(s).\n"
        + "  rename <kind> <name> <newname>: Renames this UID.\n"
        + "  delete <kind> <name>: Deletes this UID.\n"
        + "  fsck: Checks the consistency of UIDs.\n"
        + "  [kind] <name>: Lookup the ID of this name.\n"
        + "  [kind] <ID>: Lookup the name of this ID.\n"
        + "  metasync: Generates missing TSUID and UID meta entries, updates\n"
        + "            created timestamps\n"
        + "  metapurge: Removes meta data entries from the UID table\n"
        + "  treesync: Process all timeseries meta objects through tree rules\n"
        + "  treepurge <id> [definition]: Purge a tree and/or the branches\n"
        + "            from storage. Provide an integer Tree ID and optionally\n"
        + "            add \"true\" to delete the tree definition\n\n"
        + "Example values for [kind]:"
        + " metrics, tagk (tag name), tagv (tag value).");
    if (argp != null) {
      System.err.print(argp.usage());
    }
  }

  public static void main(String[] args) throws Exception {
    ArgP argp = new ArgP();
    CliOptions.addCommon(argp);
    CliOptions.addVerbose(argp);
    argp.addOption("--idwidth", "N",
                   "Number of bytes on which the UniqueId is encoded.");
    argp.addOption("--ignore-case",
                   "Ignore case distinctions when matching a regexp.");
    argp.addOption("-i", "Short for --ignore-case.");
    args = CliOptions.parse(argp, args);
    if (args == null) {
      usage(argp, "Invalid usage");
      System.exit(2);
    } else if (args.length < 1) {
      usage(argp, "Not enough arguments");
      System.exit(2);
    }   
    final short idwidth = (argp.has("--idwidth")
                           ? Short.parseShort(argp.get("--idwidth"))
                           : 3);
    if (idwidth <= 0) {
      usage(argp, "Negative or 0 --idwidth");
      System.exit(3);
    }
    final boolean ignorecase = argp.has("--ignore-case") || argp.has("-i");
    
    // get a config object
    Config config = CliOptions.getConfig(argp);
    final byte[] table = config.getString("tsd.storage.hbase.uid_table")
      .getBytes();
    
    final TSDB tsdb = new TSDB(config);
    tsdb.getClient().ensureTableExists(
        config.getString("tsd.storage.hbase.uid_table")).joinUninterruptibly();
    argp = null;
    int rc;
    try {
      rc = runCommand(tsdb, table, idwidth, ignorecase, args);
    } finally {
      try {
        tsdb.getClient().shutdown().joinUninterruptibly();
        LOG.info("Gracefully shutdown the TSD");
      } catch (Exception e) {
        LOG.error("Unexpected exception while shutting down", e);
        rc = 42;
      }
    }
    System.exit(rc);
  }

  private static int runCommand(final TSDB tsdb,
                                final byte[] table,
                                final short idwidth,
                                final boolean ignorecase,
                                final String[] args) {
    final int nargs = args.length;
    if (args[0].equals("grep")) {
      if (2 <= nargs && nargs <= 3) {
        try {
          return grep(tsdb.getClient(), table, ignorecase, args);
        } catch (HBaseException e) {
          return 3;
        }
      } else {
        usage("Wrong number of arguments");
        return 2;
      }
    } else if (args[0].equals("assign")) {
      if (nargs < 3) {
        usage("Wrong number of arguments");
        return 2;
      }
      return assign(tsdb.getClient(), table, idwidth, args);
    } else if (args[0].equals("rename")) {
      if (nargs != 4) {
        usage("Wrong number of arguments");
        return 2;
      }
      return rename(tsdb.getClient(), table, idwidth, args);
    } else if (args[0].equals("delete")) {
      if (nargs != 3) {
        usage("Wrong number of arguments");
        return 2;
      }

      try {
        tsdb.getClient()
            .ensureTableExists(
                tsdb.getConfig().getString("tsd.storage.hbase.data_table"))
            .joinUninterruptibly();

        return delete(tsdb, table, idwidth, args);
      } catch (Exception e) {
        LOG.error("Unexpected exception", e);
        return 4;
      }
    } else if (args[0].equals("fsck")) {
      return fsck(tsdb.getClient(), table);
    } else if (args[0].equals("metasync")) {
      // check for the data table existence and initialize our plugins 
      // so that update meta data can be pushed to search engines
      try {
        tsdb.getClient().ensureTableExists(
            tsdb.getConfig().getString(
                "tsd.storage.hbase.data_table")).joinUninterruptibly();
        tsdb.initializePlugins(false);
        return metaSync(tsdb);
      } catch (Exception e) {
        LOG.error("Unexpected exception", e);
        return 3;
      }      
    } else if (args[0].equals("metapurge")) {
      // check for the data table existence and initialize our plugins 
      // so that update meta data can be pushed to search engines
      try {
        tsdb.getClient().ensureTableExists(
            tsdb.getConfig().getString(
                "tsd.storage.hbase.uid_table")).joinUninterruptibly();
        return metaPurge(tsdb);
      } catch (Exception e) {
        LOG.error("Unexpected exception", e);
        return 3;
      }      
    } else if (args[0].equals("treesync")) {
      // check for the UID table existence
      try {
        tsdb.getClient().ensureTableExists(
            tsdb.getConfig().getString(
                "tsd.storage.hbase.uid_table")).joinUninterruptibly();
        if (!tsdb.getConfig().enable_tree_processing()) {
          LOG.warn("Tree processing is disabled");
          return 0;
        }
        return treeSync(tsdb);
      } catch (Exception e) {
        LOG.error("Unexpected exception", e);
        return 3;
      }      
    } else if (args[0].equals("treepurge")) {
      if (nargs < 2) {
        usage("Wrong number of arguments");
        return 2;
      }
      try {
        tsdb.getClient().ensureTableExists(
            tsdb.getConfig().getString(
                "tsd.storage.hbase.uid_table")).joinUninterruptibly();
        final int tree_id = Integer.parseInt(args[1]);
        final boolean delete_definitions;
        if (nargs < 3) {
          delete_definitions = false;
        } else {
          final String delete_all = args[2];
          if (delete_all.toLowerCase().equals("true")) {
            delete_definitions = true;
          } else {
            delete_definitions = false;
          }
        }
        return purgeTree(tsdb, tree_id, delete_definitions);
      } catch (Exception e) {
        LOG.error("Unexpected exception", e);
        return 3;
      }      
    } else {
      if (1 <= nargs && nargs <= 2) {
        final String kind = nargs == 2 ? args[0] : null;
        try {
          final long id = Long.parseLong(args[nargs - 1]);
          return lookupId(tsdb.getClient(), table, idwidth, id, kind);
        } catch (NumberFormatException e) {
          return lookupName(tsdb.getClient(), table, idwidth, 
              args[nargs - 1], kind);
        }
      } else {
        usage("Wrong number of arguments");
        return 2;
      }
    }
  }

  /**
   * Implements the {@code grep} subcommand.
   * @param client The HBase client to use.
   * @param table The name of the HBase table to use.
   * @param ignorecase Whether or not to ignore the case while grepping.
   * @param args Command line arguments ({@code [kind] RE}).
   * @return The exit status of the command (0 means at least 1 match).
   */
  private static int grep(final HBaseClient client,
                          final byte[] table,
                          final boolean ignorecase,
                          final String[] args) {
    final Scanner scanner = client.newScanner(table);
    scanner.setMaxNumRows(1024);
    String regexp;
    scanner.setFamily(ID_FAMILY);
    if (args.length == 3) {
      scanner.setQualifier(toBytes(args[1]));
      regexp = args[2];
    } else {
      regexp = args[1];
    }
    if (ignorecase) {
      regexp = "(?i)" + regexp;
    }
    scanner.setKeyRegexp(regexp, CHARSET);
    boolean found = false;
    try {
      ArrayList<ArrayList<KeyValue>> rows;
      while ((rows = scanner.nextRows().joinUninterruptibly()) != null) {
        for (final ArrayList<KeyValue> row : rows) {
          found |= printResult(row, ID_FAMILY, true);
        }
      }
    } catch (HBaseException e) {
      LOG.error("Error while scanning HBase, scanner=" + scanner, e);
      throw e;
    } catch (Exception e) {
      LOG.error("WTF?  Unexpected exception type, scanner=" + scanner, e);
      throw new AssertionError("Should never happen");
    }
    return found ? 0 : 1;
  }

  /**
   * Helper to print the cells in a given family for a given row, if any.
   * @param row The row to print.
   * @param family Only cells in this family (if any) will be printed.
   * @param formard If true, this row contains a forward mapping (name to ID).
   * Otherwise the row is assumed to contain a reverse mapping (ID to name).
   * @return {@code true} if at least one cell was printed.
   */
  private static boolean printResult(final ArrayList<KeyValue> row,
                                     final byte[] family,
                                     final boolean formard) {
    final byte[] key = row.get(0).key();
    String name = formard ? fromBytes(key) : null;
    String id = formard ? null : Arrays.toString(key);
    boolean printed = false;
    for (final KeyValue kv : row) {
      if (!Bytes.equals(kv.family(), family)) {
        continue;
      }
      printed = true;
      if (formard) {
        id = Arrays.toString(kv.value());
      } else {
        name = fromBytes(kv.value());
      }
      System.out.println(fromBytes(kv.qualifier()) + ' ' + name + ": " + id);
    }
    return printed;
  }

  /**
   * Implements the {@code assign} subcommand.
   * @param client The HBase client to use.
   * @param table The name of the HBase table to use.
   * @param idwidth Number of bytes on which the UIDs should be.
   * @param args Command line arguments ({@code assign name [names]}).
   * @return The exit status of the command (0 means success).
   */
  private static int assign(final HBaseClient client,
                            final byte[] table,
                            final short idwidth,
                            final String[] args) {
    final UniqueId uid = new UniqueId(client, table, args[1], (int) idwidth);
    for (int i = 2; i < args.length; i++) {
      try {
        uid.getOrCreateId(args[i]);
        // Lookup again the ID we've just created and print it.
        extactLookupName(client, table, idwidth, args[1], args[i]);
      } catch (HBaseException e) {
        LOG.error("error while processing " + args[i], e);
        return 3;
      }
    }
    return 0;
  }

  /**
   * Implements the {@code rename} subcommand.
   * @param client The HBase client to use.
   * @param table The name of the HBase table to use.
   * @param idwidth Number of bytes on which the UIDs should be.
   * @param args Command line arguments ({@code assign name [names]}).
   * @return The exit status of the command (0 means success).
   */
  private static int rename(final HBaseClient client,
                            final byte[] table,
                            final short idwidth,
                            final String[] args) {
    final String kind = args[1];
    final String oldname = args[2];
    final String newname = args[3];
    final UniqueId uid = new UniqueId(client, table, kind, (int) idwidth);
    try {
      uid.rename(oldname, newname);
    } catch (HBaseException e) {
      LOG.error("error while processing renaming " + oldname
                + " to " + newname, e);
      return 3;
    } catch (NoSuchUniqueName e) {
      LOG.error(e.getMessage());
      return 1;
    }
    System.out.println(kind + ' ' + oldname + " -> " + newname);
    return 0;
  }
  
  /**
   * Implements the {@code rename} subcommand.
   * @param client The HBase client to use.
   * @param table The name of the HBase table to use.
   * @param idwidth Number of bytes on which the UIDs should be.
   * @param args Command line arguments ({@code assign name [names]}).
   * @return The exit status of the command (0 means success).
   */
  private static int delete(final TSDB tsdb, final byte[] table,
      final short idwidth, final String[] args) throws Exception {
    final String kind = args[1];
    final String name = args[2];
    final UniqueId uid = new UniqueId(tsdb.getClient(), table, kind,
        (int) idwidth);
    uid.setTSDB(tsdb);
    try {
      uid.delete(name);
    } catch (HBaseException e) {
      LOG.error("error while processing delete " + name, e);
      return 3;
    } catch (NoSuchUniqueName e) {
      LOG.error(e.getMessage());
      return 1;
    }
    System.out.println(kind + ' ' + name + " deleted.");
    return 0;
  }

  /**
   * Implements the {@code fsck} subcommand.
   * @param client The HBase client to use.
   * @param table The name of the HBase table to use.
   * @return The exit status of the command (0 means success).
   */
  private static int fsck(final HBaseClient client, final byte[] table) {

    final class Uids {
      int errors;
      long maxid;
      short width;
      final HashMap<String, String> id2name = new HashMap<String, String>();
      final HashMap<String, String> name2id = new HashMap<String, String>();

      void error(final KeyValue kv, final String msg) {
        error(msg + ".  kv=" + kv);
      }

      void error(final String msg) {
        LOG.error(msg);
        errors++;
      }
    }

    final byte[] METRICS_META = "metric_meta".getBytes(CHARSET);
    final byte[] TAGK_META = "tagk_meta".getBytes(CHARSET);
    final byte[] TAGV_META = "tagv_meta".getBytes(CHARSET);
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
            
            // TODO - validate meta data in the future, for now skip it
            if (Bytes.equals(kv.qualifier(), TSMeta.META_QUALIFIER()) ||
                Bytes.equals(kv.qualifier(), TSMeta.COUNTER_QUALIFIER()) ||
                Bytes.equals(kv.qualifier(), METRICS_META) ||
                Bytes.equals(kv.qualifier(), TAGK_META) ||
                Bytes.equals(kv.qualifier(), TAGV_META)) {
              continue;
            }
            
            final String kind = fromBytes(kv.qualifier());
            Uids uids = name2uids.get(kind);
            if (uids == null) {
              uids = new Uids();
              name2uids.put(kind, uids);
            }
            final byte[] key = kv.key();
            final byte[] family = kv.family();
            final byte[] value = kv.value();
            if (Bytes.equals(key, MAXID_ROW)) {
              if (value.length != 8) {
                uids.error(kv, "Invalid maximum ID for " + kind
                           + ": should be on 8 bytes: ");
              } else {
                uids.maxid = Bytes.getLong(value);
                LOG.info("Maximum ID for " + kind + ": " + uids.maxid);
              }
            } else {
              short idwidth = 0;
              if (Bytes.equals(family, ID_FAMILY)) {
                idwidth = (short) value.length;
                final String skey = fromBytes(key);
                final String svalue = Arrays.toString(value);
                final String id = uids.name2id.put(skey, svalue);
                if (id != null) {
                  uids.error(kv, "Duplicate forward " + kind + " mapping: "
                             + skey + " -> " + id
                             + " and " + skey + " -> " + svalue);
                }
              } else if (Bytes.equals(family, NAME_FAMILY)) {
                final String skey = Arrays.toString(key);
                final String svalue = fromBytes(value);
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
      LOG.error("Error while scanning HBase, scanner=" + scanner, e);
      throw e;
    } catch (Exception e) {
      LOG.error("WTF?  Unexpected exception type, scanner=" + scanner, e);
      throw new AssertionError("Should never happen");
    }

    // Match up all forward mappings with their reverse mappings and vice
    // versa and make sure they agree.
    int errors = 0;
    for (final Map.Entry<String, Uids> entry : name2uids.entrySet()) {
      final String kind = entry.getKey();
      final Uids uids = entry.getValue();

      // Look for forward mappings without the corresponding reverse mappings.
      // These are harmful and shouldn't exist.
      for (final Map.Entry<String, String> nameid : uids.name2id.entrySet()) {
        final String name = nameid.getKey();
        final String id = nameid.getValue();
        final String found = uids.id2name.get(id);
        if (found == null) {
          uids.error("Forward " + kind + " mapping is missing reverse"
                     + " mapping: " + name + " -> " + id);
        } else if (!found.equals(name)) {
          uids.error("Forward " + kind + " mapping " + name + " -> " + id
                     + " is different than reverse mapping: "
                     + id + " -> " + found);
          final String id2 = uids.name2id.get(found);
          if (id2 != null) {
            uids.error("Inconsistent forward " + kind + " mapping "
                       + name + " -> " + id
                       + " vs " + name + " -> " + found
                       + " / " + found + " -> " + id2);
          } else {
            uids.error("Duplicate forward " + kind + " mapping "
                       + name + " -> " + id
                       + " and " + id2 + " -> " + found);
          }
        }
      }

      // Look for reverse mappings without the corresponding forward mappings.
      // These are harmless but shouldn't frequently occur.
      for (final Map.Entry<String, String> idname : uids.id2name.entrySet()) {
        final String name = idname.getValue();
        final String id = idname.getKey();
        final String found = uids.name2id.get(name);
        if (found == null) {
          LOG.warn("Reverse " + kind + " mapping is missing forward"
                   + " mapping: " + name + " -> " + id);
        } else if (!found.equals(id)) {
          final String name2 = uids.id2name.get(found);
          if (name2 != null) {
            uids.error("Inconsistent reverse " + kind + " mapping "
                       + id + " -> " + name
                       + " vs " + found + " -> " + name
                       + " / " + name2 + " -> " + found);
          } else {
            uids.error("Duplicate reverse " + kind + " mapping "
                       + id + " -> " + name
                       + " and " + found + " -> " + name2);
          }
        }
      }

      final int maxsize = Math.max(uids.id2name.size(), uids.name2id.size());
      if (uids.maxid > maxsize) {
        LOG.warn("Max ID for " + kind + " is " + uids.maxid + " but only "
                 + maxsize + " entries were found.  Maybe "
                 + (uids.maxid - maxsize) + " IDs were deleted?");
      } else if (uids.maxid < maxsize) {
        uids.error("We found " + maxsize + ' ' + kind + " but the max ID is"
                   + " only " + uids.maxid + "!  Future IDs may be"
                   + " double-assigned!");
      }

      if (uids.errors > 0) {
        LOG.error(kind + ": Found " + uids.errors + " errors.");
        errors += uids.errors;
      }
    }
    final long timing = (System.nanoTime() - start_time) / 1000000;
    System.out.println(kvcount + " KVs analyzed in " + timing + "ms (~"
                       + (kvcount * 1000 / timing) + " KV/s)");
    if (errors == 0) {
      System.out.println("No errors found.");
      return 0;
    }
    System.err.println(errors + " errors found.");
    return 1;
  }

  /**
   * Looks up an ID and finds the corresponding name(s), if any.
   * @param client The HBase client to use.
   * @param table The name of the HBase table to use.
   * @param idwidth Number of bytes on which the UIDs should be.
   * @param lid The ID to look for.
   * @param kind The 'kind' of the ID (can be {@code null}).
   * @return The exit status of the command (0 means at least 1 found).
   */
  private static int lookupId(final HBaseClient client,
                              final byte[] table,
                              final short idwidth,
                              final long lid,
                              final String kind) {
    final byte[] id = idInBytes(idwidth, lid);
    if (id == null) {
      return 1;
    } else if (kind != null) {
      return extactLookupId(client, table, idwidth, kind, id);
    }
    return findAndPrintRow(client, table, id, NAME_FAMILY, false);
  }

  /**
   * Gets a given row in HBase and prints it on standard output.
   * @param client The HBase client to use.
   * @param table The name of the HBase table to use.
   * @param key The row key to attempt to get from HBase.
   * @param family The family in which we're interested.
   * @return 0 if at least one cell was found and printed, 1 otherwise.
   */
  private static int findAndPrintRow(final HBaseClient client,
                                     final byte[] table,
                                     final byte[] key,
                                     final byte[] family,
                                     boolean formard) {
    final GetRequest get = new GetRequest(table, key);
    get.family(family);
    ArrayList<KeyValue> row;
    try {
      row = client.get(get).joinUninterruptibly();
    } catch (HBaseException e) {
      LOG.error("Get failed: " + get, e);
      return 1;
    } catch (Exception e) {
      LOG.error("WTF?  Unexpected exception type, get=" + get, e);
      return 42;
    }
    return printResult(row, family, formard) ? 0 : 1;
  }

  /**
   * Looks up an ID for a given kind, and prints it if found.
   * @param client The HBase client to use.
   * @param table The name of the HBase table to use.
   * @param idwidth Number of bytes on which the UIDs should be.
   * @param kind The 'kind' of the ID (must not be {@code null}).
   * @param id The ID to look for.
   * @return 0 if the ID for this kind was found, 1 otherwise.
   */
  private static int extactLookupId(final HBaseClient client,
                                    final byte[] table,
                                    final short idwidth,
                                    final String kind,
                                    final byte[] id) {
    final UniqueId uid = new UniqueId(client, table, kind, (int) idwidth);
    try {
      final String name = uid.getName(id);
      System.out.println(kind + ' ' + name + ": " + Arrays.toString(id));
      return 0;
    } catch (NoSuchUniqueId e) {
      LOG.error(e.getMessage());
      return 1;
    }
  }

  /**
   * Transforms an ID into the corresponding byte array.
   * @param idwidth Number of bytes on which the UIDs should be.
   * @param lid The ID to transform.
   * @return The ID represented in {@code idwidth} bytes, or
   * {@code null} if {@code lid} couldn't fit in {@code idwidth} bytes.
   */
  private static byte[] idInBytes(final short idwidth, final long lid) {
    if (idwidth <= 0) {
      throw new AssertionError("negative idwidth: " + idwidth);
    }
    final byte[] id = Bytes.fromLong(lid);
    for (int i = 0; i < id.length - idwidth; i++) {
      if (id[i] != 0) {
        System.err.println(lid + " is too large to fit on " + idwidth
            + " bytes.  Maybe you forgot to adjust --idwidth?");
        return null;
      }
    }
    return Arrays.copyOfRange(id, id.length - idwidth, id.length);
  }

  /**
   * Looks up a name and finds the corresponding UID(s), if any.
   * @param client The HBase client to use.
   * @param table The name of the HBase table to use.
   * @param idwidth Number of bytes on which the UIDs should be.
   * @param name The name to look for.
   * @param kind The 'kind' of the ID (can be {@code null}).
   * @return The exit status of the command (0 means at least 1 found).
   */
  private static int lookupName(final HBaseClient client,
                                final byte[] table,
                                final short idwidth,
                                final String name,
                                final String kind) {
    if (kind != null) {
      return extactLookupName(client, table, idwidth, kind, name);
    }
    return findAndPrintRow(client, table, toBytes(name), ID_FAMILY, true);
  }

  /**
   * Looks up a name for a given kind, and prints it if found.
   * @param client The HBase client to use.
   * @param idwidth Number of bytes on which the UIDs should be.
   * @param kind The 'kind' of the ID (must not be {@code null}).
   * @param name The name to look for.
   * @return 0 if the name for this kind was found, 1 otherwise.
   */
  private static int extactLookupName(final HBaseClient client,
                                      final byte[] table,
                                      final short idwidth,
                                      final String kind,
                                      final String name) {
    final UniqueId uid = new UniqueId(client, table, kind, (int) idwidth);
    try {
      final byte[] id = uid.getId(name);
      System.out.println(kind + ' ' + name + ": " + Arrays.toString(id));
      return 0;
    } catch (NoSuchUniqueName e) {
      LOG.error(e.getMessage());
      return 1;
    }
  }

  /**
   * Runs through the entire data table and creates TSMeta objects for unique
   * timeseries and/or updates {@code created} timestamps
   * The process is as follows:
   * <ul><li>Fetch the max number of Metric UIDs as we'll use those to match
   * on the data rows</li>
   * <li>Split the # of UIDs amongst worker threads</li>
   * <li>Setup a scanner in each thread for the range it will be working on and
   * start iterating</li>
   * <li>Fetch the TSUID from the row key</li>
   * <li>For each unprocessed TSUID:
   * <ul><li>Check if the metric UID mapping is present, if not, log an error
   * and continue</li>
   * <li>See if the meta for the metric UID exists, if not, create it</li>
   * <li>See if the row timestamp is less than the metric UID meta's created
   * time. This means we have a record of the UID being used earlier than the
   * meta data indicates. Update it.</li>
   * <li>Repeat the previous three steps for each of the TAGK and TAGV tags</li>
   * <li>Check to see if meta data exists for the timeseries</li>
   * <li>If not, create the counter column if it's missing, and create the meta
   * column</li>
   * <li>If it did exist, check the {@code created} timestamp and if the row's 
   * time is less, update the meta data</li></ul></li>
   * <li>Continue on to the next unprocessed timeseries data row</li></ul>
   * <b>Note:</b> Updates or new entries will also be sent to the search plugin
   * if configured.
   * @param tsdb The tsdb to use for processing, including a search plugin
   * @return 0 if completed successfully, something else if it dies
   */
  private static int metaSync(final TSDB tsdb) throws Exception {
    final long start_time = System.currentTimeMillis() / 1000;
    final long max_id = getMaxMetricID(tsdb);
    
    // now figure out how many IDs to divy up between the workers
    final int workers = Runtime.getRuntime().availableProcessors() * 2;
    final double quotient = (double)max_id / (double)workers;
    final Set<Integer> processed_tsuids = 
      Collections.synchronizedSet(new HashSet<Integer>());
    final ConcurrentHashMap<String, Long> metric_uids = 
      new ConcurrentHashMap<String, Long>();
    final ConcurrentHashMap<String, Long> tagk_uids = 
      new ConcurrentHashMap<String, Long>();
    final ConcurrentHashMap<String, Long> tagv_uids = 
      new ConcurrentHashMap<String, Long>();
    
    long index = 1;
    
    LOG.info("Max metric ID is [" + max_id + "]");
    LOG.info("Spooling up [" + workers + "] worker threads");
    final Thread[] threads = new Thread[workers];
    for (int i = 0; i < workers; i++) {
      threads[i] = new MetaSync(tsdb, index, quotient, processed_tsuids, 
          metric_uids, tagk_uids, tagv_uids, i);
      threads[i].setName("MetaSync # " + i);
      threads[i].start();
      index += quotient;
      if (index < max_id) {
        index++;
      }
    }
    
    // wait till we're all done
    for (int i = 0; i < workers; i++) {
      threads[i].join();
      LOG.info("[" + i + "] Finished");
    }
    
    // make sure buffered data is flushed to storage before exiting
    tsdb.flush().joinUninterruptibly();
    
    final long duration = (System.currentTimeMillis() / 1000) - start_time;
    LOG.info("Completed meta data synchronization in [" + 
        duration + "] seconds");
    return 0;
  }
  
  /**
   * Runs through the tsdb-uid table and removes TSMeta, UIDMeta and TSUID 
   * counter entries from the table
   * The process is as follows:
   * <ul><li>Fetch the max number of Metric UIDs</li>
   * <li>Split the # of UIDs amongst worker threads</li>
   * <li>Create a delete request with the qualifiers of any matching meta data
   * columns</li></ul>
   * <li>Continue on to the next unprocessed timeseries data row</li></ul>
   * @param tsdb The tsdb to use for processing, including a search plugin
   * @return 0 if completed successfully, something else if it dies
   */
  private static int metaPurge(final TSDB tsdb) throws Exception {
    final long start_time = System.currentTimeMillis() / 1000;
    final long max_id = getMaxMetricID(tsdb);
    
    // now figure out how many IDs to divy up between the workers
    final int workers = Runtime.getRuntime().availableProcessors() * 2;
    final double quotient = (double)max_id / (double)workers;
    
    long index = 1;
    
    LOG.info("Max metric ID is [" + max_id + "]");
    LOG.info("Spooling up [" + workers + "] worker threads");
    final Thread[] threads = new Thread[workers];
    for (int i = 0; i < workers; i++) {
      threads[i] = new MetaPurge(tsdb, index, quotient, i);
      threads[i].setName("MetaSync # " + i);
      threads[i].start();
      index += quotient;
      if (index < max_id) {
        index++;
      }
    }
    
    // wait till we're all done
    for (int i = 0; i < workers; i++) {
      threads[i].join();
      LOG.info("[" + i + "] Finished");
    }
    
    // make sure buffered data is flushed to storage before exiting
    tsdb.flush().joinUninterruptibly();
    
    final long duration = (System.currentTimeMillis() / 1000) - start_time;
    LOG.info("Completed meta data synchronization in [" + 
        duration + "] seconds");
    return 0;
  }
  
  /**
   * Runs through all TSMeta objects in the UID table and passes them through
   * each of the Trees configured in the system.
   * First, the method loads all trees in the system, compiles them into 
   * TreeBuilders, then scans the UID table, passing each TSMeta through each
   * of the TreeBuilder objects.
   * @param tsdb The TSDB to use for access
   * @return 0 if completed successfully, something else if an error occurred
   */
  private static int treeSync(final TSDB tsdb) throws Exception {
    final long start_time = System.currentTimeMillis() / 1000;
    final long max_id = getMaxMetricID(tsdb);
    
 // now figure out how many IDs to divy up between the workers
    final int workers = Runtime.getRuntime().availableProcessors() * 2;
    final double quotient = (double)max_id / (double)workers;
    
    long index = 1;
    
    LOG.info("Max metric ID is [" + max_id + "]");
    LOG.info("Spooling up [" + workers + "] worker threads");
    final Thread[] threads = new Thread[workers];
    for (int i = 0; i < workers; i++) {
      threads[i] = new TreeSync(tsdb, index, quotient, i);
      threads[i].setName("TreeSync # " + i);
      threads[i].start();
      index += quotient;
      if (index < max_id) {
        index++;
      }
    }
    
    // wait till we're all done
    for (int i = 0; i < workers; i++) {
      threads[i].join();
      LOG.info("[" + i + "] Finished");
    }
    
    // make sure buffered data is flushed to storage before exiting
    tsdb.flush().joinUninterruptibly();
    
    final long duration = (System.currentTimeMillis() / 1000) - start_time;
    LOG.info("Completed meta data synchronization in [" + 
        duration + "] seconds");
    return 0;
  }
  
  /**
   * Attempts to delete the branches, leaves, collisions and not-matched entries
   * for a given tree. Optionally removes the tree definition itself
   * @param tsdb The TSDB to use for access
   * @param tree_id ID of the tree to delete
   * @param delete_definition Whether or not to delete the tree definition as
   * well 
   * @return 0 if completed successfully, something else if an error occurred
   */
  private static int purgeTree(final TSDB tsdb, final int tree_id, 
      final boolean delete_definition) throws Exception {
    final TreeSync sync = new TreeSync(tsdb, 0, 1, 0);
    return sync.purgeTree(tree_id, delete_definition);
  }
  
  /**
   * Returns the max metric ID from the UID table
   * @param tsdb The TSDB to use for data access
   * @return The max metric ID as an integer value
   */
  private static long getMaxMetricID(final TSDB tsdb) {
    // first up, we need the max metric ID so we can split up the data table
    // amongst threads.
    final GetRequest get = new GetRequest(tsdb.uidTable(), new byte[] { 0 });
    get.family("id".getBytes(CHARSET));
    get.qualifier("metrics".getBytes(CHARSET));
    ArrayList<KeyValue> row;
    try {
      row = tsdb.getClient().get(get).joinUninterruptibly();
      if (row == null || row.isEmpty()) {
        throw new IllegalStateException("No data in the metric max UID cell");
      }
      final byte[] id_bytes = row.get(0).value();
      if (id_bytes.length != 8) {
        throw new IllegalStateException("Invalid metric max UID, wrong # of bytes");
      }
      return Bytes.getLong(id_bytes);
    } catch (Exception e) {
      throw new RuntimeException("Shouldn't be here", e);
    }
  }
  
  private static byte[] toBytes(final String s) {
    try {
      return (byte[]) toBytes.invoke(null, s);
    } catch (Exception e) {
      throw new RuntimeException("toBytes=" + toBytes, e);
    }
  }

  private static String fromBytes(final byte[] b) {
    try {
      return (String) fromBytes.invoke(null, b);
    } catch (Exception e) {
      throw new RuntimeException("fromBytes=" + fromBytes, e);
    }
  }
}

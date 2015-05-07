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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.Bytes;
import org.hbase.async.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.HBaseException;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
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
        + "  fsck: [fix] [delete_unknown] Checks the consistency of UIDs.\n"
        + "        fix            - Fix errors. By default errors are logged.\n"
        + "        delete_unknown - Remove columns with unknown qualifiers.\n"
        + "                         The \"fix\" flag must be supplied as well.\n"
        + "\n"
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
    } else if (args[0].equals("fsck")) {
      boolean fix = false;
      boolean fix_unknowns = false;
      if (args.length > 1) {
        for (String arg : args) {
          if (arg.equals("fix")) {
            fix = true;
          } else if (arg.equals("delete_unknown")) {
            fix_unknowns = true;
          }
        }
      }
      return fsck(tsdb.getClient(), table, fix, fix_unknowns);
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
    scanner.setFamily(CliUtils.ID_FAMILY);
    if (args.length == 3) {
      scanner.setQualifier(CliUtils.toBytes(args[1]));
      regexp = args[2];
    } else {
      regexp = args[1];
    }
    if (ignorecase) {
      regexp = "(?i)" + regexp;
    }
    scanner.setKeyRegexp(regexp, CliUtils.CHARSET);
    boolean found = false;
    try {
      ArrayList<ArrayList<KeyValue>> rows;
      while ((rows = scanner.nextRows().joinUninterruptibly()) != null) {
        for (final ArrayList<KeyValue> row : rows) {
          found |= printResult(row, CliUtils.ID_FAMILY, true);
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
    String name = formard ? CliUtils.fromBytes(key) : null;
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
        name = CliUtils.fromBytes(kv.value());
      }
      System.out.println(CliUtils.fromBytes(kv.qualifier()) + ' ' + name + ": " + id);
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
   * Implements the {@code fsck} subcommand.
   * @param client The HBase client to use.
   * @param table The name of the HBase table to use.
   * @return The exit status of the command (0 means success).
   */
  private static int fsck(final HBaseClient client, final byte[] table, 
      final boolean fix, final boolean fix_unknowns) {

    if (fix) {
      LOG.info("----------------------------------");
      LOG.info("-    Running fsck in FIX mode    -");
      LOG.info("-      Remove Unknowns: " + fix_unknowns + "     -");
      LOG.info("----------------------------------");
    } else {
      LOG.info("Running in log only mode");
    }
    
    final class Uids {
      int errors;
      long maxid;
      long max_found_id;
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
      
      /*
       * Replaces or creates the reverse map in storage and in the local map
       */
      void restoreReverseMap(final String kind, final String name, 
          final String uid) {
        final PutRequest put = new PutRequest(table, 
            UniqueId.stringToUid(uid), CliUtils.NAME_FAMILY, CliUtils.toBytes(kind), 
            CliUtils.toBytes(name));
        client.put(put);
        id2name.put(uid, name);
        LOG.info("FIX: Restoring " + kind + " reverse mapping: " 
            + uid + " -> " + name);
      }
      
      /*
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
        LOG.info("FIX: Removed " + kind + " reverse mapping: " + uid + " -> "
            + name);
      }
    }

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
              LOG.warn("Unknown qualifier " + UniqueId.uidToString(qualifier) 
                  + " in row " + UniqueId.uidToString(kv.key()));
              if (fix && fix_unknowns) {
                final DeleteRequest delete = new DeleteRequest(table, kv.key(), 
                    kv.family(), qualifier);
                client.delete(delete);
                LOG.info("FIX: Removed unknown qualifier " 
                  + UniqueId.uidToString(qualifier) 
                  + " in row " + UniqueId.uidToString(kv.key()));
              }
              continue;
            }

            final String kind = CliUtils.fromBytes(kv.qualifier());
            Uids uids = name2uids.get(kind);
            if (uids == null) {
              uids = new Uids();
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
                LOG.info("Maximum ID for " + kind + ": " + uids.maxid);
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

      // This will be used in the event that we run into an inconsistent forward
      // mapping that could mean a single UID was assigned to different names.
      // It SHOULD NEVER HAPPEN, but it could.
      HashMap<String, TreeSet<String>> uid_collisions = null;
      
      // Look for forward mappings without the corresponding reverse mappings.
      // These are harmful and shouldn't exist.
      for (final Map.Entry<String, String> nameid : uids.name2id.entrySet()) {
        final String name = nameid.getKey();
        final String id = nameid.getValue();
        final String found = uids.id2name.get(id);
        if (found == null) {
          uids.error("Forward " + kind + " mapping is missing reverse"
                     + " mapping: " + name + " -> " + id);
          
          if (fix) {
            uids.restoreReverseMap(kind, name, id);
          }
          
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
            
            // This shouldn't happen but there are two possible states:
            // 1) The reverse map name is wrong
            // 2) Multiple names are mapped to the same UID, which is REALLY
            //    as we're sending data from different sources to the same time
            //    series.
            if (fix) {
              // using the name2id map, build an id -> set of names map. Do it 
              // once, as needed, since it's expensive.
              if (uid_collisions == null) {
                uid_collisions = 
                  new HashMap<String, TreeSet<String>>(uids.name2id.size());
                for (final Map.Entry<String, String> row : uids.name2id.entrySet()) {
                  TreeSet<String> names = uid_collisions.get(row.getValue());
                  if (names == null) {
                    names = new TreeSet<String>();
                    uid_collisions.put(row.getValue(), names);
                  }
                  names.add(row.getKey());
                }
              }
              
              // if there was only one name to UID map found, then the time 
              // series *should* be OK and we can just fix the reverse map.
              if (uid_collisions.containsKey(id) && 
                  uid_collisions.get(id).size() <= 1) {
                uids.restoreReverseMap(kind, name, id);
              }
            }
          } else {
            uids.error("Duplicate forward " + kind + " mapping "
                       + name + " -> " + id
                       + " and " + id2 + " -> " + found);
            
            if (fix) {
              uids.restoreReverseMap(kind, name, id);
            }
          }
        }
      }
      
      // Scan through the UID collisions map and fix the screw ups
      if (uid_collisions != null) {
        for (Map.Entry<String, TreeSet<String>> collision 
            : uid_collisions.entrySet()) {
          if (collision.getValue().size() <= 1) {
            continue;
          }
          
          // The data in any time series with the errant UID is 
          // a mashup of with all of the names. The best thing to do is
          // start over. We'll rename the old time series so the user can
          // still see it if they want to, but delete the forward mappings
          // so that UIDs can be reassigned and clean series started.
          // - concatenate all of the names into 
          //   "fsck.<name1>.<name2>[...<nameN>]"
          // - delete the forward mappings for all of the names
          // - create a mapping with the fsck'd name pointing to the id
          final StringBuilder fsck_builder = new StringBuilder("fsck");
          final String id = collision.getKey();
          
          // compile the new fsck'd name and remove each of the duplicate keys
          for (String name : collision.getValue()) {
            fsck_builder.append(".")
                     .append(name);
            
            final DeleteRequest delete = new DeleteRequest(table, 
                CliUtils.toBytes(name), CliUtils.ID_FAMILY, CliUtils.toBytes(kind));
            client.delete(delete);
            uids.name2id.remove(name);
            LOG.info("FIX: Removed forward " + kind + " mapping for " + name + " -> " 
                + id);
          }
          
          // write the new forward map
          final String fsck_name = fsck_builder.toString();
          final PutRequest put = new PutRequest(table, CliUtils.toBytes(fsck_name), 
              CliUtils.ID_FAMILY, CliUtils.toBytes(kind), UniqueId.stringToUid(id));
          client.put(put);
          LOG.info("FIX: Created forward " + kind + " mapping for fsck'd UID " + 
              fsck_name + " -> " + collision.getKey());
          
          // we still need to fix the uids map for the reverse run through below
          uids.name2id.put(fsck_name, collision.getKey());
          uids.restoreReverseMap(kind, fsck_name, id);
          
          LOG.error("----------------------------------");
          LOG.error("-     UID COLLISION DETECTED     -");
          LOG.error("Corrupted UID [" + collision.getKey() + "] renamed to [" 
              + fsck_name +"]");
          LOG.error("----------------------------------");
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
          
          if (fix) {
            uids.removeReverseMap(kind, name, id);
          }
        } else if (!found.equals(id)) {
          final String name2 = uids.id2name.get(found);
          if (name2 != null) {
            uids.error("Inconsistent reverse " + kind + " mapping "
                       + id + " -> " + name
                       + " vs " + found + " -> " + name
                       + " / " + name2 + " -> " + found);
            
            if (fix) {
              uids.removeReverseMap(kind, name, id);
            }
          } else {
            uids.error("Duplicate reverse " + kind + " mapping "
                       + id + " -> " + name
                       + " and " + found + " -> " + name2);
            
            if (fix) {
              uids.removeReverseMap(kind, name, id);
            }
          }
        }
      }

      final int maxsize = Math.max(uids.id2name.size(), uids.name2id.size());
      if (uids.maxid > maxsize) {
        LOG.warn("Max ID for " + kind + " is " + uids.maxid + " but only "
                 + maxsize + " entries were found.  Maybe "
                 + (uids.maxid - maxsize) + " IDs were deleted?");
      } else if (uids.maxid < uids.max_found_id) {
        uids.error("We found an ID of " + uids.max_found_id + " for " + kind 
                   + " but the max ID is only " + uids.maxid 
                   + "!  Future IDs may be double-assigned!");
        
        if (fix) {
          // increment the max UID by the difference. It could be that a TSD may
          // increment the id immediately before our call, and if so, that would
          // put us over a little, but that's OK as it's better to waste a few
          // IDs than to under-run.
          if (uids.max_found_id == Long.MAX_VALUE) {
            LOG.error("Ran out of UIDs for " + kind + ". Unable to fix max ID");
          } else {
            final long diff = uids.max_found_id - uids.maxid;
            final AtomicIncrementRequest air = new AtomicIncrementRequest(table, 
                CliUtils.MAXID_ROW, CliUtils.ID_FAMILY, CliUtils.toBytes(kind), diff);
            client.atomicIncrement(air);
            LOG.info("FIX: Updated max ID for " + kind + " to " + uids.max_found_id);
          }          
        }
      }

      if (uids.errors > 0) {
        LOG.error(kind + ": Found " + uids.errors + " errors.");
        errors += uids.errors;
      }
    }
    final long timing = (System.nanoTime() - start_time) / 1000000;
    LOG.info(kvcount + " KVs analyzed in " + timing + "ms (~"
                       + (kvcount * 1000 / timing) + " KV/s)");
    if (errors == 0) {
      LOG.info("No errors found.");
      return 0;
    }
    LOG.warn(errors + " errors found.");
    return errors;
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
    return findAndPrintRow(client, table, id, CliUtils.NAME_FAMILY, false);
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
    return findAndPrintRow(client, table, CliUtils.toBytes(name), 
        CliUtils.ID_FAMILY, true);
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

    // now figure out how many IDs to divy up between the workers
    final int workers = Runtime.getRuntime().availableProcessors() * 2;
    final Set<Integer> processed_tsuids = 
      Collections.synchronizedSet(new HashSet<Integer>());
    final ConcurrentHashMap<String, Long> metric_uids = 
      new ConcurrentHashMap<String, Long>();
    final ConcurrentHashMap<String, Long> tagk_uids = 
      new ConcurrentHashMap<String, Long>();
    final ConcurrentHashMap<String, Long> tagv_uids = 
      new ConcurrentHashMap<String, Long>();

    final List<Scanner> scanners = CliUtils.getDataTableScanners(tsdb, workers);
    LOG.info("Spooling up [" + scanners.size() + "] worker threads");
    final List<Thread> threads = new ArrayList<Thread>(scanners.size());
    int i = 0;
    for (final Scanner scanner : scanners) {
      final MetaSync worker = new MetaSync(tsdb, scanner, processed_tsuids, 
          metric_uids, tagk_uids, tagv_uids, i++);
      worker.setName("Sync #" + i);
      worker.start();
      threads.add(worker);
    }

    for (final Thread thread : threads) {
      thread.join();
      LOG.info("Thread [" + thread + "] Finished");
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
    final long max_id = CliUtils.getMaxMetricID(tsdb);
    
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
    final long max_id = CliUtils.getMaxMetricID(tsdb);
    
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

}

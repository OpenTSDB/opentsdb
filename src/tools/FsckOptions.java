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

import net.opentsdb.utils.Config;

/**
 * Various options to use during fsck over OpenTSDB tables
 */
final class FsckOptions {
  private boolean fix;
  private boolean compact;
  private boolean resolve_dupes;
  private boolean last_write_wins;
  private boolean delete_orphans;
  private boolean delete_unknown_columns;
  private boolean delete_bad_values;
  private boolean delete_bad_rows;
  private boolean delete_bad_compacts;
  private int threads;
  
  /**
   * Default Ctor that sets the options based on command line flags and config
   * object
   * @param argp Command line arguments post parsing
   * @param config The configuration object loaded from a file 
   * @throws IllegalArgumentException if a required value is missing or something
   * was incorrect
   */
  public FsckOptions(final ArgP argp, final Config config) {
    fix = argp.has("--fix") || argp.has("--fix-all");
    compact = argp.has("--compact") || argp.has("--fix-all");
    resolve_dupes = argp.has("--resolve-duplicates") || argp.has("--fix-all");
    last_write_wins = argp.has("--last-write-wins") || 
        config.getBoolean("tsd.storage.fix_duplicates");
    delete_orphans = argp.has("--delete-orphans") || argp.has("--fix-all");
    delete_unknown_columns = argp.has("--delete-unknown-columns") || 
        argp.has("--fix-all");
    delete_bad_values = argp.has("--delete-bad-values") || 
        argp.has("--fix-all");
    delete_bad_rows = argp.has("--delete-bad-rows") || 
        argp.has("--fix-all");
    delete_bad_compacts = argp.has("--delete-bad-compacts") || 
        argp.has("--fix-all");
    if (argp.has("--threads")) {
      threads = Integer.parseInt(argp.get("--threads"));
      if (threads < 1) {
        throw new IllegalArgumentException("Must have at least one thread");
      }
      if (threads > Runtime.getRuntime().availableProcessors() * 4) {
        throw new IllegalArgumentException(
            "Not allowed to run more than 4 threads per core");
      }
    } else {
      threads = 0;
    }
  }
  
  /**
   * Add data table fsck options to the command line parser
   * @param argp The parser to add options to
   */
  public static void addDataOptions(final ArgP argp) {
    argp.addOption("--full-scan", "Scan the entire data table.");
    argp.addOption("--fix", "Fix errors as they're found. Use in combination with"
        + " other flags.");
    argp.addOption("--fix-all", "Set all flags and fix errors as they're found.");
    argp.addOption("--compact", "Compacts rows after parsing.");
    argp.addOption("--resolve-duplicates", 
        "Keeps the oldest (default) or newest duplicates. See --last-write-wins");
    argp.addOption("--last-write-wins", 
        "Last data point written will be kept when fixing duplicates.\n" +
        "            May be set via config file and the 'tsd.storage.fix_duplicates' option.");
    argp.addOption("--delete-orphans", 
        "Delete any time series rows where one or more UIDs fail resolution.");
    argp.addOption("--delete-unknown-columns", 
        "Delete any unrecognized column that doesn't belong to OpenTSDB.");
    argp.addOption("--delete-bad-values", 
        "Delete single column datapoints with bad values.");
    argp.addOption("--delete-bad-rows", "Delete rows with invalid keys.");
    argp.addOption("--delete-bad-compacts", 
        "Delete compacted columns that cannot be parsed.");
    argp.addOption("--threads", "NUMBER",
        "Number of threads to use when executing a full table scan.");
  }
  
  /** @return Whether or not to fix errors while processing. Does not affect 
   * compacting */
  public boolean fix() {
    return fix;
  }
  
  /** @return Whether or not to compact rows while processing. Can cause 
   * compaction without the --fix flag. Will skip rows with duplicate data 
   * points unless --last-write-wins is also specified or set in the config 
   * file */
  public boolean compact() {
    return compact;
  }
  
  /** @return Whether or not to fix duplicates */
  public boolean resolveDupes() {
    return resolve_dupes;
  }
  
  /** @return Accept data points with the most recent timestamp when duplicates 
   * are found */
  public boolean lastWriteWins() {
    return last_write_wins;
  }
  
  /** @return Whether or not to delete rows where the UIDs failed to resolve 
   * to a name */
  public boolean deleteOrphans() {
    return delete_orphans;
  }
  
  /** @return Delete columns that aren't recognized */
  public boolean deleteUnknownColumns() {
    return delete_unknown_columns;
  }
  
  /** @return Remove data points with bad values */
  public boolean deleteBadValues() {
    return delete_bad_values;
  }

  
  /** @return Remove rows with invalid keys */
  public boolean deleteBadRows() {
    return delete_bad_rows;
  }
  
  /** @return Remove compacted columns that can't be parsed */
  public boolean deleteBadCompacts() {
    return delete_bad_compacts;
  }

  /** @return The number of threads to run. If 0, default to cores * 2 */
  public int threads() {
    return threads;
  }
  
  /** @param fix Whether or not to fix errors while processing. Does not affect 
   * compacting */
  public void setFix(final boolean fix) {
    this.fix = fix;
  }
  

  /** @param compact Whether or not to compact rows while processing. Can cause 
   * compaction without the --fix flag. Will skip rows with duplicate data 
   * points unless --last-write-wins is also specified or set in the config 
   * file */
  public void setCompact(final boolean compact) {
    this.compact = compact;
  }
  
  /** @param fix_dupes Whether or not to fix duplicate data points */
  public void setResolveDupes(final boolean fix_dupes) {
    this.resolve_dupes = fix_dupes;
  }
  

  /** @param last_write_wins Accept data points with the most recent timestamp when duplicates 
   * are found */
  public void setLastWriteWins(final boolean last_write_wins) {
    this.last_write_wins = last_write_wins;
  }
  

  /** @param delete_orphans Whether or not to delete rows where the UIDs failed to resolve 
   * to a name */
  public void setDeleteOrphans(final boolean delete_orphans) {
    this.delete_orphans = delete_orphans;
  }
  

  /** @param delete_unknown_columns Delete columns that aren't recognized */
  public void setDeleteUnknownColumns(final boolean delete_unknown_columns) {
    this.delete_unknown_columns = delete_unknown_columns;
  }
  

  /** @param delete_bad_values Remove data points with bad values */
  public void setDeleteBadValues(final boolean delete_bad_values) {
    this.delete_bad_values = delete_bad_values;
  }

  /** @param delete_bad_rows Remove data points with invalid keys */
  public void setDeleteBadRows(final boolean delete_bad_rows) {
    this.delete_bad_rows = delete_bad_rows;
  }

  /** @param delete_bad_compacts Remove compated columns that can't be parsed */
  public void setDeleteBadCompacts(final boolean delete_bad_compacts) {
    this.delete_bad_compacts = delete_bad_compacts;
  }

  /** @param threads The number of threads to run
   * @throws IllegalArgumentException if < 1 threads or more than cores * 4 
   * threads are specified
   */
  public void setThreads(final int threads) {
    if (threads < 1) {
      throw new IllegalArgumentException("Must have at least one thread");
    }
    if (threads > Runtime.getRuntime().availableProcessors() * 4) {
      throw new IllegalArgumentException(
          "Not allowed to run more than 4 threads per core");
    }
    this.threads = threads;
  }
}

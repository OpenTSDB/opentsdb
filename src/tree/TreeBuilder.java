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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;

import net.opentsdb.core.TSDB;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.tree.TreeRule.TreeRuleType;
import net.opentsdb.uid.UniqueId.UniqueIdType;

import org.hbase.async.HBaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

/**
 * Contains the logic and methods for building a branch from a tree definition
 * and a TSMeta object. Use the class by loading a tree, passing it to the 
 * builder constructor, and call {@link #processTimeseriesMeta} with a TSMeta
 * object.
 * <p>
 * When processing, the builder runs the meta data through each of the rules in
 * the rule set and recursively builds a tree. After running through all of the
 * rules, if valid results were obtained, each branch is saved to storage if 
 * they haven't been processed before (in the {@link #processed_branches} map).
 * If a leaf was found, it will be saved. If any collisions or not-matched
 * reports occurred, they will be saved to storage.
 * <p>
 * If {@link #processTimeseriesMeta} is called with the testing flag, the 
 * tree will be built but none of the branches will be stored. This is used for
 * RPC calls to display the results to a user and {@link #test_messages} will
 * contain a detailed description of the processing results.
 * <p>
 * <b>Warning:</b> This class is not thread safe. It should only be used by a
 * single thread to process a TSMeta at a time. If processing multiple TSMetas
 * you can create the builder and run all of the meta objects through the 
 * process methods.
 * @since 2.0
 */
public final class TreeBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(TreeBuilder.class);
  
  /** List of trees to use when processing real-time TSMeta  entries */
  private static final List<Tree> trees = new ArrayList<Tree>();
  
  /** List of roots so we don't have to fetch them every time we process a ts */
  private static final ConcurrentHashMap<Integer, Branch> tree_roots = 
    new ConcurrentHashMap<Integer, Branch>();
  
  /** Timestamp when we last reloaded all of the trees */
  private static long last_tree_load;
  
  /** Lock used to synchronize loading of the tree list */
  private static final Lock trees_lock = new ReentrantLock();
  
  /** The TSDB to use for fetching/writing data */
  private final TSDB tsdb;
  
  /** Stores merged branches for testing */
  private Branch root;
  
  /** 
   * Used when parsing data to determine the max rule ID, necessary when users
   * skip a level on accident
   */
  private int max_rule_level;

  /** Filled with messages when the user has asked for a test run */
  private ArrayList<String> test_messages;
  
  /** The tree to work with */
  private Tree tree;
  
  /** The meta data we're parsing */
  private TSMeta meta;
  
  /** Current array of splits, may be null */
  private String[] splits;
  
  /** Current rule index */
  private int rule_idx;
  
  /** Current split index */
  private int split_idx;

  /** The current branch we're working with */
  private Branch current_branch;
  
  /** Current rule */
  private TreeRule rule;
  
  /** Whether or not the TS failed to match a rule, used for 
   * {@code strict_match} */
  private String not_matched;
  
  /** 
   * Map used to keep track of branches that have already been processed by
   * this particular builder. This is useful for the tree sync CLI utility or
   * for future caching so that we don't send useless CAS calls to storage
   */
  private final HashMap<String, Boolean> processed_branches = 
    new HashMap<String, Boolean>();

  /**
   * Constructor to initialize the builder. Also calculates the 
   * {@link #max_rule_level} based on the tree's rules
   * @param tsdb The TSDB to use for access
   * @param tree A tree with rules configured and ready for parsing
   */
  public TreeBuilder(final TSDB tsdb, final Tree tree) {
    this.tsdb = tsdb;
    this.tree = tree;
    calculateMaxLevel();
  }
  
  /**
   * Convenience overload of {@link #processTimeseriesMeta(TSMeta, boolean)} that
   * sets the testing flag to false. Any changes processed from this method will
   * be saved to storage
   * @param meta The timeseries meta object to process
   * @return A list of deferreds to wait on for storage completion
   * @throws IllegalArgumentException if the tree has not been set or is invalid
   */
  public Deferred<ArrayList<Boolean>> processTimeseriesMeta(final TSMeta meta) {
    if (tree == null || tree.getTreeId() < 1) {
      throw new IllegalArgumentException(
          "The tree has not been set or is invalid");
    }
    return processTimeseriesMeta(meta, false);
  }
  
  /**
   * Runs the TSMeta object through the {@link Tree}s rule set, optionally 
   * storing the resulting branches, leaves and meta data.
   * If the testing flag is set, no results will be saved but the caller can
   * fetch the root branch from this object as it will contain the tree that
   * would result from the processing. Also, the {@link #test_messages} list
   * will contain details about the process for debugging purposes.
   * @param meta The timeseries meta object to process
   * @param is_testing Whether or not changes should be written to storage. If 
   * false, resulting branches and leaves will be saved. If true, results will
   * not be flushed to storage.
   * @return A list of deferreds to wait on for storage completion
   * @throws IllegalArgumentException if the tree has not been set or is invalid
   * @throws HBaseException if a storage exception occurred
   */
  public Deferred<ArrayList<Boolean>> processTimeseriesMeta(final TSMeta meta, 
      final boolean is_testing) {
    if (tree == null || tree.getTreeId() < 1) {
      throw new IllegalArgumentException(
          "The tree has not been set or is invalid");
    }
    if (meta == null || meta.getTSUID() == null || meta.getTSUID().isEmpty()) {
      throw new IllegalArgumentException("Missing TSUID");
    }
    
    // reset the state in case the caller is reusing this object
    resetState();
    this.meta = meta;
    
    // setup a list of deferreds to return to the caller so they can wait for
    // storage calls to complete
    final ArrayList<Deferred<Boolean>> storage_calls = 
      new ArrayList<Deferred<Boolean>>();
    
    /**
     * Runs the local TSMeta object through the tree's rule set after the root
     * branch has been set. This can be called after loading or creating the
     * root or if the root is set, it's called directly from this method. The
     * response is the deferred group for the caller to wait on.
     */
    final class ProcessCB implements Callback<Deferred<ArrayList<Boolean>>, 
      Branch> {

      /**
       * Process the TSMeta using the provided branch as the root.
       * @param branch The root branch to use
       * @return A group of deferreds to wait on for storage call completion
       */
      @Override
      public Deferred<ArrayList<Boolean>> call(final Branch branch) 
        throws Exception {
        
        // start processing with the depth set to 1 since we'll start adding 
        // branches to the root
        processRuleset(branch, 1);
        
        if (not_matched != null && !not_matched.isEmpty() && 
            tree.getStrictMatch()) {
          
          // if the tree has strict matching enabled and one or more levels
          // failed to match, then we don't want to store the resulting branches,
          // only the TSUID that failed to match
          testMessage(
              "TSUID failed to match one or more rule levels, will not add: " + 
              meta);
          if (!is_testing && tree.getNotMatched() != null && 
              !tree.getNotMatched().isEmpty()) {
            tree.addNotMatched(meta.getTSUID(), not_matched);
            storage_calls.add(tree.flushNotMatched(tsdb));
          }
          
        } else if (current_branch == null) {
          
          // something was wrong with the rule set that resulted in an empty
          // branch. Since this is likely a user error, log it instead of
          // throwing an exception
          LOG.warn("Processed TSUID [" + meta + 
              "] resulted in a null branch on tree: " + tree.getTreeId());
          
        } else if (!is_testing) {
          
          // iterate through the generated tree store the tree and leaves,
          // adding the parent path as we go
          Branch cb = current_branch;
          Map<Integer, String> path = branch.getPath();
          cb.prependParentPath(path);
          while (cb != null) {
            if (cb.getLeaves() != null || 
                !processed_branches.containsKey(cb.getBranchId())) {
              LOG.debug("Flushing branch to storage: " + cb);

              /**
               * Since we need to return a deferred group and we can't just
               * group the branch storage deferreds with the not-matched and 
               * collisions, we need to implement a callback that will wait for
               * the results of the branch stores and group that with the rest.
               * This CB will return false if ANY of the branches failed to 
               * be written.
               */
              final class BranchCB implements Callback<Deferred<Boolean>, 
                ArrayList<Boolean>> {

                @Override
                public Deferred<Boolean> call(final ArrayList<Boolean> deferreds)
                    throws Exception {
                  
                  for (Boolean success : deferreds) {
                    if (!success) {
                      return Deferred.fromResult(false);
                    }
                  }
                  return Deferred.fromResult(true);
                }
                
              }
              final Deferred<Boolean> deferred = cb.storeBranch(tsdb, tree, true)
                .addCallbackDeferring(new BranchCB());
              storage_calls.add(deferred);
              processed_branches.put(cb.getBranchId(), true);
            }
            
            // move to the next branch in the tree
            if (cb.getBranches() == null) {
              cb = null;
            } else {
              path = cb.getPath();
              // we should only have one child if we're building a tree, so we 
              // only need to grab the first one
              cb = cb.getBranches().first();
              cb.prependParentPath(path);
            }
          }
          
          // if we have collisions, flush em
          if (tree.getCollisions() != null && !tree.getCollisions().isEmpty()) {
            storage_calls.add(tree.flushCollisions(tsdb));
          }
          
        } else {
          
          // we are testing, so compile the branch paths so that the caller can
          // fetch the root branch object and return it from an RPC call
          Branch cb = current_branch;
          branch.addChild(cb);
          Map<Integer, String> path = branch.getPath();
          cb.prependParentPath(path);
          while (cb != null) {
            if (cb.getBranches() == null) {
              cb = null;
            } else {
              path = cb.getPath();
              // we should only have one child if we're building
              cb = cb.getBranches().first();
              cb.prependParentPath(path);
            }
          }
        }
        
        LOG.debug("Completed processing meta [" + meta + "] through tree: " + tree.getTreeId());
        return Deferred.group(storage_calls);
      }
    
    }

    /**
     * Called after loading or initializing the root and continues the chain
     * by passing the root onto the ProcessCB
     */
    final class LoadRootCB implements Callback<Deferred<ArrayList<Boolean>>, 
      Branch> {

      @Override
      public Deferred<ArrayList<Boolean>> call(final Branch root) 
        throws Exception {
        TreeBuilder.this.root = root;
        return new ProcessCB().call(root);
      }
      
    }
    
    LOG.debug("Processing meta [" + meta + "] through tree: " + tree.getTreeId());
    if (root == null) {
      // if this is a new object or the root has been reset, we need to fetch
      // it from storage or initialize it
      LOG.debug("Fetching root branch for tree: " + tree.getTreeId());
      return loadOrInitializeRoot(tsdb, tree.getTreeId(), is_testing)
        .addCallbackDeferring(new LoadRootCB());
    } else {
      // the root has been set, so just reuse it
      try {
        return new ProcessCB().call(root);
      } catch (Exception e) {
        throw new RuntimeException("Failed to initiate processing", e);
      }
    }
  }

  /**
   * Attempts to retrieve or initialize the root branch for the configured tree.
   * If the is_testing flag is false, the root will be saved if it has to be
   * created. The new or existing root branch will be stored to the local root
   * object.
   * <b>Note:</b> This will also cache the root in the local store since we 
   * don't want to keep loading on every TSMeta during real-time processing
   * @param tsdb The tsdb to use for storage calls
   * @param tree_id ID of the tree the root should be fetched/initialized for
   * @param is_testing Whether or not the root should be written to storage if
   * initialized.
   * @return True if loading or initialization was successful.
   */
  public static Deferred<Branch> loadOrInitializeRoot(final TSDB tsdb, 
      final int tree_id, final boolean is_testing) {

    /**
     * Final callback executed after the storage put completed. It also caches
     * the root branch so we don't keep calling and re-calling it, returning a
     * copy for the local TreeBuilder to use
     */
    final class NewRootCB implements Callback<Deferred<Branch>, 
    ArrayList<Boolean>> {

      final Branch root;
      
      public NewRootCB(final Branch root) {
        this.root = root;
      }
      
      @Override
      public Deferred<Branch> call(final ArrayList<Boolean> storage_call) 
        throws Exception {
        LOG.info("Initialized root branch for tree: " + tree_id);
        tree_roots.put(tree_id, root);
        return Deferred.fromResult(new Branch(root));
      }
      
    }
    
    /**
     * Called after attempting to fetch the branch. If the branch didn't exist
     * then we'll create a new one and save it if told to
     */
    final class RootCB implements Callback<Deferred<Branch>, Branch> {

      @Override
      public Deferred<Branch> call(final Branch branch) throws Exception {
        if (branch == null) {
          LOG.info("Couldn't find the root branch, initializing");
          final Branch root = new Branch(tree_id);
          root.setDisplayName("ROOT");
          final TreeMap<Integer, String> root_path = 
            new TreeMap<Integer, String>();
          root_path.put(0, "ROOT");
          root.prependParentPath(root_path);
          if (is_testing) {
            return Deferred.fromResult(root);
          } else {
            return root.storeBranch(tsdb, null, true).addCallbackDeferring(
                new NewRootCB(root));
          }
        } else {
          return Deferred.fromResult(branch);
        }
      }
      
    }
    
    // if the root is already in cache, return it
    final Branch cached = tree_roots.get(tree_id);
    if (cached != null) {
      LOG.debug("Loaded cached root for tree: " + tree_id);
      return Deferred.fromResult(new Branch(cached));
    }
    
    LOG.debug("Loading or initializing root for tree: " + tree_id);
    return Branch.fetchBranchOnly(tsdb, Tree.idToBytes(tree_id))
      .addCallbackDeferring(new RootCB());
  }
  
  /**
   * Attempts to run the given TSMeta object through all of the trees in the
   * system.
   * @param tsdb The TSDB to use for access
   * @param meta The timeseries meta object to process
   * @return A meaningless deferred to wait on for all trees to process the 
   * meta object
   * @throws IllegalArgumentException if the tree has not been set or is invalid
   * @throws HBaseException if a storage exception occurred
   */
  public static Deferred<Boolean> processAllTrees(final TSDB tsdb, 
      final TSMeta meta) {

    /**
     * Simple final callback that waits on all of the processing calls before
     * returning
     */
    final class FinalCB implements Callback<Boolean, 
      ArrayList<ArrayList<Boolean>>> {
      @Override
      public Boolean call(final ArrayList<ArrayList<Boolean>> groups) 
        throws Exception {
        return true;
      }
    }

    /**
     * Callback that loops through the local list of trees, processing the
     * TSMeta through each
     */
    final class ProcessTreesCB implements Callback<Deferred<Boolean>, 
      List<Tree>> {
      
      // stores the tree deferred calls for later joining. Lazily initialized
      ArrayList<Deferred<ArrayList<Boolean>>> processed_trees;
      
      @Override
      public Deferred<Boolean> call(List<Tree> trees) throws Exception {
        if (trees == null || trees.isEmpty()) {
          LOG.debug("No trees found to process meta through");
          return Deferred.fromResult(false);
        } else {
          LOG.debug("Loaded [" + trees.size() + "] trees");
        }
        
        processed_trees = 
          new ArrayList<Deferred<ArrayList<Boolean>>>(trees.size());
        for (Tree tree : trees) {
          if (!tree.getEnabled()) {
            continue;
          }
          final TreeBuilder builder = new TreeBuilder(tsdb, new Tree(tree));
          processed_trees.add(builder.processTimeseriesMeta(meta, false));
        }
        
        return Deferred.group(processed_trees).addCallback(new FinalCB());
      }
      
    }
    
    /**
     * Callback used when loading or re-loading the cached list of trees
     */
    final class FetchedTreesCB implements Callback<List<Tree>, List<Tree>> {

      @Override
      public List<Tree> call(final List<Tree> loaded_trees) 
        throws Exception {
        
        final List<Tree> local_trees;
        synchronized(trees) {
          trees.clear();
          for (final Tree tree : loaded_trees) {
            if (tree.getEnabled()) {
              trees.add(tree);
            }
          }
          
          local_trees = new ArrayList<Tree>(trees.size());
          local_trees.addAll(trees);
        }
        trees_lock.unlock();
        return local_trees;
      }
      
    }

    /**
     * Since we can't use a try/catch/finally to release the lock we need to 
     * setup an ErrBack to catch any exception thrown by the loader and
     * release the lock before returning
     */
    final class ErrorCB implements Callback<Object, Exception> {

      @Override
      public Object call(final Exception e) throws Exception {
        trees_lock.unlock();
        throw e;
      }
      
    }
    
    // lock to load or 
    trees_lock.lock();
    
    // if we haven't loaded our trees in a while or we've just started, load
    if (((System.currentTimeMillis() / 1000) - last_tree_load) > 300) {
      final Deferred<List<Tree>> load_deferred = Tree.fetchAllTrees(tsdb)
        .addCallback(new FetchedTreesCB()).addErrback(new ErrorCB());
      last_tree_load = (System.currentTimeMillis() / 1000);
      return load_deferred.addCallbackDeferring(new ProcessTreesCB());
    }
    
    // copy the tree list so we don't hold up the other threads while we're
    // processing
    final List<Tree> local_trees;
    if (trees.isEmpty()) {
      LOG.debug("No trees were found to process the meta through");
      trees_lock.unlock();
      return Deferred.fromResult(true);
    }
    
    local_trees = new ArrayList<Tree>(trees.size());
    local_trees.addAll(trees);
    
    // unlock so the next thread can get a copy of the trees and start
    // processing
    trees_lock.unlock();
    
    try {
      return new ProcessTreesCB().call(local_trees);
    } catch (Exception e) {
      throw new RuntimeException("Failed to process trees", e);
    }
  }

  /**
   * Recursive method that compiles a set of branches and a leaf from the loaded
   * tree's rule set. The first time this is called the root should be given as
   * the {@code branch} argument.
   * Recursion is complete when all rule levels have been exhausted and, 
   * optionally, all splits have been processed.
   * <p>
   * To process a rule set, you only need to call this method. It acts as a 
   * router, calling the correct "parse..." methods depending on the rule type.
   * <p>
   * Processing a rule set involves the following:
   * <ul><li>Route to a parser method for the proper rule type</li>
   * <li>Parser method attempts to find the proper value and returns immediately
   * if it didn't match and we move on to the next rule</li>
   * <li>Parser passes the parsed value on to {@link #processParsedValue} that
   * routes to a sub processor such as a handler for regex or split rules</li>
   * <li>If processing for the current rule has finished and was successful, 
   * {@link #setCurrentName} is called to set the branch display name</li>
   * <li>If more rules exist, we recurse</li>
   * <li>If we've completed recursion, we determine if the branch is a leaf, or
   * if it's a null and we need to skip it, etc.</li></ul>
   * @param parent_branch The previously processed branch
   * @param depth The current branch depth. The first call should set this to 1
   * @return True if processing has completed, i.e. we've finished all rules, 
   * false if there is further processing to perform.
   * @throws IllegalStateException if one of the rule processors failed due to
   * a bad configuration.
   */
  private boolean processRuleset(final Branch parent_branch, int depth) {

    // when we've passed the final rule, just return to stop the recursion
    if (rule_idx > max_rule_level) {
      return true;
    }
    
    // setup the branch for this iteration and set the "current_branch" 
    // reference. It's not final as we'll be copying references back and forth
    final Branch previous_branch = current_branch;
    current_branch = new Branch(tree.getTreeId());
    
    // fetch the current rule level or try to find the next one
    TreeMap<Integer, TreeRule> rule_level = fetchRuleLevel();
    if (rule_level == null) {
      return true;
    }
    
    // loop through each rule in the level, processing as we go
    for (Map.Entry<Integer, TreeRule> entry : rule_level.entrySet()) {
      // set the local rule
      rule = entry.getValue();
      testMessage("Processing rule: " + rule);
      
      // route to the proper handler based on the rule type
      if (rule.getType() == TreeRuleType.METRIC) {
        parseMetricRule();
        // local_branch = current_branch; //do we need this???
      } else if (rule.getType() == TreeRuleType.TAGK) {
        parseTagkRule();
      } else if (rule.getType() == TreeRuleType.METRIC_CUSTOM) {
        parseMetricCustomRule();
      } else if (rule.getType() == TreeRuleType.TAGK_CUSTOM) {
        parseTagkCustomRule();
      } else if (rule.getType() == TreeRuleType.TAGV_CUSTOM) {
        parseTagvRule();
      } else {
        throw new IllegalArgumentException("Unkown rule type: " + 
            rule.getType());
      }
      
      // rules on a given level are ORd so the first one that matches, we bail
      if (current_branch.getDisplayName() != null && 
          !current_branch.getDisplayName().isEmpty()) {
        break;
      }
    }
    
    // if no match was found on the level, then we need to set no match
    if (current_branch.getDisplayName() == null || 
        current_branch.getDisplayName().isEmpty()) {
      if (not_matched == null) {
        not_matched = new String(rule.toString());
      } else {
        not_matched += " " + rule;
      }
    }
    
    // determine if we need to continue processing splits, are done with splits
    // or need to increment to the next rule level
    if (splits != null && split_idx >= splits.length) {
      // finished split processing
      splits = null;
      split_idx = 0;
      rule_idx++;
    } else if (splits != null) {
      // we're still processing splits, so continue
    } else {
      // didn't have any splits so continue on to the next level
      rule_idx++;
    }
    
    // call ourselves recursively until we hit a leaf or run out of rules
    final boolean complete = processRuleset(current_branch, ++depth);
    
    // if the recursion loop is complete, we either have a leaf or need to roll
    // back
    if (complete) {
      // if the current branch is null or empty, we didn't match, so roll back
      // to the previous branch and tell it to be the leaf
      if (current_branch == null || current_branch.getDisplayName() == null || 
          current_branch.getDisplayName().isEmpty()) {
        LOG.trace("Got to a null branch");
        current_branch = previous_branch;
        return true;
      }
      
      // if the parent has an empty ID, we need to roll back till we find one
      if (parent_branch.getDisplayName() == null || 
          parent_branch.getDisplayName().isEmpty()) {
        testMessage("Depth [" + depth + 
            "] Parent branch was empty, rolling back");
        return true;
      }
      
      // add the leaf to the parent and roll back
      final Leaf leaf = new Leaf(current_branch.getDisplayName(), 
          meta.getTSUID());
      parent_branch.addLeaf(leaf, tree);
      testMessage("Depth [" + depth + "] Adding leaf [" + leaf + 
          "] to parent branch [" + parent_branch + "]");
      current_branch = previous_branch;
      return false;
    }
    
    // if a rule level failed to match, we just skip the result swap
    if ((previous_branch == null || previous_branch.getDisplayName().isEmpty()) 
        && !current_branch.getDisplayName().isEmpty()) {
      if (depth > 2) {
        testMessage("Depth [" + depth + 
            "] Skipping a non-matched branch, returning: " + current_branch);
      }
      return false;
    }

    // if the current branch is empty, skip it
    if (current_branch.getDisplayName() == null || 
        current_branch.getDisplayName().isEmpty()) {
      testMessage("Depth [" + depth + "] Branch was empty");
      current_branch = previous_branch;
      return false;
    }
    
    // if the previous and current branch are the same, we just discard the 
    // previous, since the current may have a leaf
    if (current_branch.getDisplayName().equals(previous_branch.getDisplayName())){
      testMessage("Depth [" + depth + "] Current was the same as previous");
      return false;
    }
    
    // we've found a new branch, so add it
    parent_branch.addChild(current_branch);
    testMessage("Depth [" + depth + "] Adding branch: " + current_branch + 
        " to parent: " + parent_branch);
    current_branch = previous_branch;
    return false;
  }

  /**
   * Processes the metric from a TSMeta. Routes to the 
   * {@link #processParsedValue} method after processing
   * @throws IllegalStateException if the metric UIDMeta was null or the metric
   * name was empty 
   */
  private void parseMetricRule() {
    if (meta.getMetric() == null) {
      throw new IllegalStateException(
          "Timeseries metric UID object was null");
    }
    
    final String metric = meta.getMetric().getName();
    if (metric == null || metric.isEmpty()) {
      throw new IllegalStateException(
          "Timeseries metric name was null or empty");
    }
    
    processParsedValue(metric);
  }

  /**
   * Processes the tag value paired with a tag name. Routes to the 
   * {@link #processParsedValue} method after processing if successful
   * @throws IllegalStateException if the tag UIDMetas have not be set
   */
  private void parseTagkRule() {
    final List<UIDMeta> tags = meta.getTags();
    if (tags == null || tags.isEmpty()) {
      throw new IllegalStateException(
        "Tags for the timeseries meta were null");
    }
    
    String tag_name = "";
    boolean found = false;
    
    // loop through each tag pair. If the tagk matches the requested field name
    // then we flag it as "found" and on the next pass, grab the tagv name. This
    // assumes we have a list of [tagk, tagv, tagk, tagv...] pairs. If not, 
    // we're screwed
    for (UIDMeta uidmeta : tags) {
      if (uidmeta.getType() == UniqueIdType.TAGK && 
          uidmeta.getName().equals(rule.getField())) {
        found = true;
      } else if (uidmeta.getType() == UniqueIdType.TAGV && found) {
        tag_name = uidmeta.getName();
        break;
      }
    }
    
    // if we didn't find a match, return
    if (!found || tag_name.isEmpty()) {
      testMessage("No match on tagk [" + rule.getField() + "] for rule: " + 
          rule);
      return;
    }
    
    // matched!
    testMessage("Matched tagk [" + rule.getField() + "] for rule: " + rule);
    processParsedValue(tag_name);    
  }
  
  /**
   * Processes the custom tag value paired with a custom tag name. Routes to the 
   * {@link #processParsedValue} method after processing if successful.
   * If the custom tag group is null or empty for the metric, we just return.
   * @throws IllegalStateException if the metric UIDMeta has not been set
   */
  private void parseMetricCustomRule() {
    if (meta.getMetric() == null) {
      throw new IllegalStateException(
          "Timeseries metric UID object was null");
    }
    
    Map<String, String> custom = meta.getMetric().getCustom();
    if (custom != null && custom.containsKey(rule.getCustomField())) {
      if (custom.get(rule.getCustomField()) == null) {
        throw new IllegalStateException(
            "Value for custom metric field [" + rule.getCustomField() + 
            "] was null");
      }
      processParsedValue(custom.get(rule.getCustomField()));
      testMessage("Matched custom tag [" + rule.getCustomField() 
          + "] for rule: " + rule);
    } else {
      // no match
      testMessage("No match on custom tag [" + rule.getCustomField() 
          + "] for rule: " + rule);
    }
  }
  
  /**
   * Processes the custom tag value paired with a custom tag name. Routes to the 
   * {@link #processParsedValue} method after processing if successful.
   * If the custom tag group is null or empty for the tagk, or the tagk couldn't
   * be found, we just return.
   * @throws IllegalStateException if the tags UIDMeta array has not been set
   */
  private void parseTagkCustomRule() {
    if (meta.getTags() == null || meta.getTags().isEmpty()) {
      throw new IllegalStateException(
        "Timeseries meta data was missing tags");
    }
    
    // first, find the tagk UIDMeta we're matching against
    UIDMeta tagk = null;
    for (UIDMeta tag: meta.getTags()) {
      if (tag.getType() == UniqueIdType.TAGK && 
          tag.getName().equals(rule.getField())) {
        tagk = tag;
        break;
      }
    }
    
    if (tagk == null) {
      testMessage("No match on tagk [" + rule.getField() + "] for rule: " + 
          rule);
      return;
    }
    
    // now scan the custom tags for a matching tag name and it's value
    testMessage("Matched tagk [" + rule.getField() + "] for rule: " + 
        rule);
    final Map<String, String> custom = tagk.getCustom();
    if (custom != null && custom.containsKey(rule.getCustomField())) {
      if (custom.get(rule.getCustomField()) == null) {
        throw new IllegalStateException(
            "Value for custom tagk field [" + rule.getCustomField() + 
            "] was null");
      }
      processParsedValue(custom.get(rule.getCustomField()));
      testMessage("Matched custom tag [" + rule.getCustomField() + 
          "] for rule: " + rule);
    } else {
      testMessage("No match on custom tag [" + rule.getCustomField() + 
          "] for rule: " + rule);
      return;
    }
  }
  
  /**
   * Processes the custom tag value paired with a custom tag name. Routes to the 
   * {@link #processParsedValue} method after processing if successful.
   * If the custom tag group is null or empty for the tagv, or the tagv couldn't
   * be found, we just return.
   * @throws IllegalStateException if the tags UIDMeta array has not been set
   */
  private void parseTagvRule() {
    if (meta.getTags() == null || meta.getTags().isEmpty()) {
      throw new IllegalStateException(
        "Timeseries meta data was missing tags");
    }
    
    // first, find the tagv UIDMeta we're matching against
    UIDMeta tagv = null;
    for (UIDMeta tag: meta.getTags()) {
      if (tag.getType() == UniqueIdType.TAGV && 
          tag.getName().equals(rule.getField())) {
        tagv = tag;
        break;
      }
    }
    
    if (tagv == null) {
      testMessage("No match on tagv [" + rule.getField() + "] for rule: " + 
          rule);
      return;
    }
    
    // now scan the custom tags for a matching tag name and it's value
    testMessage("Matched tagv [" + rule.getField() + "] for rule: " + 
        rule);
    final Map<String, String> custom = tagv.getCustom();
    if (custom != null && custom.containsKey(rule.getCustomField())) {
      if (custom.get(rule.getCustomField()) == null) {
        throw new IllegalStateException(
            "Value for custom tagv field [" + rule.getCustomField() + 
            "] was null");
      }
      processParsedValue(custom.get(rule.getCustomField()));
      testMessage("Matched custom tag [" + rule.getCustomField() + 
          "] for rule: " + rule);
    } else {
      testMessage("No match on custom tag [" + rule.getCustomField() + 
          "] for rule: " + rule);
      return;
    }
  }  

  /**
   * Routes the parsed value to the proper processing method for altering the
   * display name depending on the current rule. This can route to the regex
   * handler or the split processor. Or if neither splits or regex are specified
   * for the rule, the parsed value is set as the branch name.
   * @param parsed_value The value parsed from the calling parser method
   * @throws IllegalStateException if a valid processor couldn't be found. This
   * should never happen but you never know.
   */
  private void processParsedValue(final String parsed_value) {
    if (rule.getCompiledRegex() == null && 
        (rule.getSeparator() == null || rule.getSeparator().isEmpty())) {
      // we don't have a regex and we don't need to separate, so just use the
      // name of the timseries
      setCurrentName(parsed_value, parsed_value);
    } else if (rule.getCompiledRegex() != null) {
      // we have a regex rule, so deal with it
      processRegexRule(parsed_value);
    } else if (rule.getSeparator() != null && !rule.getSeparator().isEmpty()) {
      // we have a split rule, so deal with it
      processSplit(parsed_value);
    } else {
      throw new IllegalStateException("Unable to find a processor for rule: " + 
          rule);
    }
  }
  
  /**
   * Performs a split operation on the parsed value using the character set
   * in the rule's {@code separator} field. When splitting a value, the 
   * {@link #splits} and {@link #split_idx} fields are used to track state and 
   * determine where in the split we currently are. {@link #processRuleset} will
   * handle incrementing the rule index after we have finished our split. If
   * the split separator character wasn't found in the parsed string, then we
   * just return the entire string and move on to the next rule.
   * @param parsed_value The value parsed from the calling parser method
   * @throws IllegalStateException if the value was empty or the separator was
   * empty
   */
  private void processSplit(final String parsed_value) {
    if (splits == null) {
      // then this is the first time we're processing the value, so we need to
      // execute the split if there's a separator, after some validation
      if (parsed_value == null || parsed_value.isEmpty()) {
        throw new IllegalArgumentException("Value was empty for rule: " + 
            rule);
      }
      if (rule.getSeparator() == null || rule.getSeparator().isEmpty()) {
        throw new IllegalArgumentException("Separator was empty for rule: " + 
            rule);
      }      
      
      // split it
      splits = parsed_value.split(rule.getSeparator());
      if (splits.length < 1) { 
        testMessage("Separator did not match, created an empty list on rule: " + 
            rule);
        // set the index to 1 so the next time through it thinks we're done and
        // moves on to the next rule
        split_idx = 1;
        return;
      }
      split_idx = 0;
      setCurrentName(parsed_value, splits[split_idx]);
      split_idx++;
    } else {
      // otherwise we have split values and we just need to grab the next one
      setCurrentName(parsed_value, splits[split_idx]);
      split_idx++;
    }
  }
  
  /**
   * Runs the parsed string through a regex and attempts to extract a value from
   * the specified group index. Group indexes start at 0. If the regex was not
   * matched, or an extracted value for the requested group did not exist, then
   * the processor returns and the rule will be considered a no-match.
   * @param parsed_value The value parsed from the calling parser method
   * @throws IllegalStateException if the rule regex was null
   */
  private void processRegexRule(final String parsed_value) {
    if (rule.getCompiledRegex() == null) {
      throw new IllegalArgumentException("Regex was null for rule: " + 
          rule);
    }

    final Matcher matcher = rule.getCompiledRegex().matcher(parsed_value);
    if (matcher.find()) {
      // The first group is always the full string, so we need to increment
      // by one to fetch the proper group
      if (matcher.groupCount() >= rule.getRegexGroupIdx() + 1) {
        final String extracted = 
          matcher.group(rule.getRegexGroupIdx() + 1);
        if (extracted == null || extracted.isEmpty()) {
          // can't use empty values as a branch/leaf name
          testMessage("Extracted value for rule " + 
              rule + " was null or empty");
        } else {
          // found a branch or leaf!
          setCurrentName(parsed_value, extracted);
        }
      } else {
        // the group index was out of range
        testMessage("Regex group index [" + 
            rule.getRegexGroupIdx() + "] for rule " + 
            rule + " was out of bounds [" +
            matcher.groupCount() + "]");
      }
    }
  }

  /**
   * Processes the original and extracted values through the 
   * {@code display_format} of the rule to determine a display name for the
   * branch or leaf. 
   * @param original_value The original, raw value processed by the calling rule
   * @param extracted_value The post-processed value after the rule worked on it
   */
  private void setCurrentName(final String original_value, 
      final String extracted_value) {

    // now parse and set the display name. If the formatter is empty, we just 
    // set it to the parsed value and exit
    String format = rule.getDisplayFormat();
    if (format == null || format.isEmpty()) {
      current_branch.setDisplayName(extracted_value);
      return;
    }
    
    if (format.contains("{ovalue}")) {
      format = format.replace("{ovalue}", original_value);
    }
    if (format.contains("{value}")) {
      format = format.replace("{value}", extracted_value);
    }
    if (format.contains("{tsuid}")) {
      format = format.replace("{tsuid}", meta.getTSUID());
    }
    if (format.contains("{tag_name}")) {
      final TreeRuleType type = rule.getType();
      if (type == TreeRuleType.TAGK) {
        format = format.replace("{tag_name}", rule.getField());
      } else if (type == TreeRuleType.METRIC_CUSTOM ||
          type == TreeRuleType.TAGK_CUSTOM ||
          type == TreeRuleType.TAGV_CUSTOM) {
        format = format.replace("{tag_name}", rule.getCustomField());
      } else {
        // we can't match the {tag_name} token since the rule type is invalid
        // so we'll just blank it
        format = format.replace("{tag_name}", "");
        LOG.warn("Display rule " + rule + 
          " was of the wrong type to match on {tag_name}");
        if (test_messages != null) {
          test_messages.add("Display rule " + rule + 
              " was of the wrong type to match on {tag_name}");
        }
      }
    }
    current_branch.setDisplayName(format);
  }
  
  /**
   * Helper method that iterates through the first dimension of the rules map
   * to determine the highest level (or key) and stores it to 
   * {@code max_rule_level}
   */
  private void calculateMaxLevel() {
    if (tree.getRules() == null) {
      LOG.debug("No rules set for this tree");
      return;
    }
    
    for (Integer level : tree.getRules().keySet()) {
      if (level > max_rule_level) {
        max_rule_level = level;
      }
    }
  }
  
  /**
   * Adds the given message to the local {@link #test_messages} array if it has
   * been configured. Also logs each message to TRACE for debugging purposes.
   * @param message The message to log
   */
  private void testMessage(final String message) {
    if (test_messages != null) {
      test_messages.add(message);
    }
    LOG.trace(message);
  }
  
  /**
   * A helper that fetches the next level in the rule set. If a user removes
   * an entire rule level, we want to be able to skip it gracefully without
   * throwing an exception. This will loop until we hit {@link #max_rule_level}
   * or we find a valid rule.
   * @return The rules on the current {@link #rule_idx} level or the next valid
   * level if {@link #rule_idx} is invalid. Returns null if we've run out of
   * rules.
   */
  private TreeMap<Integer, TreeRule> fetchRuleLevel() {
    TreeMap<Integer, TreeRule> current_level = null;
    
    // iterate until we find some rules on a level or we run out
    while (current_level == null && rule_idx <= max_rule_level) {
      current_level = tree.getRules().get(rule_idx);
      if (current_level != null) {
        return current_level;
      } else {
        rule_idx++;
      }
    }
    
    // no more levels
    return null;
  }

  /**
   * Resets local state variables to their defaults
   */
  private void resetState() {
    meta = null;
    splits = null;
    rule_idx = 0;
    split_idx = 0;
    current_branch = null;
    rule = null;
    not_matched = null;
    if (root != null) {
      if (root.getBranches() != null) {
        root.getBranches().clear();
      }
      if (root.getLeaves() != null) {
        root.getLeaves().clear();
      }
    }
    test_messages = new ArrayList<String>();
  }

  // GETTERS AND SETTERS --------------------------------
  
  /** @return the local tree object */
  public Tree getTree() {
    return tree;
  }
  
  /** @return the root object */
  public Branch getRootBranch() {
    return root;
  }

  /** @return the list of test message results */
  public ArrayList<String> getTestMessage() {
    return test_messages;
  }
  
  /** @param tree The tree to store locally */
  public void setTree(final Tree tree) {
    this.tree = tree;
    calculateMaxLevel();
    root = null;
  }
}

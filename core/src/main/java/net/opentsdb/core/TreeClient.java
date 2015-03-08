package net.opentsdb.core;

import com.stumbleupon.async.Deferred;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.tree.Branch;
import net.opentsdb.tree.Leaf;
import net.opentsdb.tree.Tree;
import net.opentsdb.tree.TreeRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

@Singleton
public class TreeClient {
  private static final Logger LOG = LoggerFactory.getLogger(TreeClient.class);

  private final TsdbStore store;

  @Inject
  public TreeClient(final TsdbStore store) {
    this.store = checkNotNull(store);
  }

  /**
   * Attempts to store the local tree in a new row, automatically assigning a
   * new tree ID and returning the value.
   * This method will scan the UID table for the maximum tree ID, increment it,
   * store the new tree, and return the new ID. If no trees have been created,
   * the returned ID will be "1". If we have reached the limit of trees for the
   * system, as determined by {@link net.opentsdb.core.Const#MAX_TREE_ID_INCLUSIVE}, we will throw
   * an exception.
   *
   * @param tree The Tree to store
   * @return A positive ID, greater than 0 if successful, 0 if there was
   * an error
   */
  public Deferred<Integer> createNewTree(final Tree tree) {
    if (tree.getTreeId() > 0) {
      throw new IllegalArgumentException("Tree ID has already been set");
    }
    if (tree.getName() == null || tree.getName().isEmpty()) {
      throw new IllegalArgumentException("Tree was missing the name");
    }
    return store.createNewTree(tree);
  }

  /**
   * Attempts to delete all rules belonging to the given tree.
   * @param tree_id ID of the tree the rules belongs to
   * @return A deferred to wait on for completion. The value has no meaning and
   * may be null.
   * @throws IllegalArgumentException if the one of the required parameters was
   * missing
   */
  public Deferred<Object> deleteAllTreeRules(final int tree_id) {

    Tree.validateTreeID(tree_id);

    return store.deleteAllTreeRule(tree_id);
  }

  /**
   * Attempts to delete all branches, leaves, collisions and not-matched entries
   * for the given tree. Optionally can delete the tree definition and rules as
   * well.
   * <b>Warning:</b> This call can take a long time to complete so it should
   * only be done from a command line or issues once via RPC and allowed to
   * process. Multiple deletes running at the same time on the same tree
   * shouldn't be an issue but it's a waste of resources.
   * @param tree_id ID of the tree to delete
   * @param delete_definition Whether or not the tree definition and rule set
   * should be deleted as well
   * @return True if the deletion completed successfully, false if there was an
   * issue.
   * @throws IllegalArgumentException if the tree ID was invalid
   */
  public Deferred<Boolean> deleteTree(final int tree_id,
                                      final boolean delete_definition) {

    Tree.validateTreeID(tree_id);

    return store.deleteTree(tree_id, delete_definition);
  }

  /**
   * Attempts to delete the specified rule from storage
   * @param tree_id ID of the tree the rule belongs to
   * @param level Level where the rule resides
   * @param order Order where the rule resides
   * @return A deferred without meaning. The response may be null and should
   * only be used to track completion.
   * @throws IllegalArgumentException if the one of the required parameters was
   * missing
   */
  public Deferred<Object> deleteTreeRule(final int tree_id,
                                                final int level,
                                                final int order) {
    TreeRule.validateTreeRule(tree_id, level, order);

    return store.deleteTreeRule(tree_id, level, order);
  }

  /**
   * Attempts to fetch the branch, it's leaves and all child branches.
   * The UID names for each leaf may also be loaded if configured.
   * @param branch_id ID of the branch to retrieve
   * @param load_leaf_uids Whether or not to load UID names for each leaf
   * @param tsdb
   * @return A branch if found, null if it did not exist
   * @throws net.opentsdb.utils.JSONException if the object could not be deserialized
   */
  public Deferred<Branch> fetchBranch(final byte[] branch_id,
                                      final boolean load_leaf_uids, final TSDB tsdb) {
    return store.fetchBranch(branch_id, load_leaf_uids, tsdb);
  }

  /**
   * Attempts to fetch only the branch definition object from storage. This is
   * much faster than scanning many rows for child branches as per the
   * {@link #fetchBranch} call. Useful when building trees, particularly to
   * fetch the root branch.
   * @param branch_id ID of the branch to retrieve
   * @return A branch if found, null if it did not exist
   * @throws net.opentsdb.utils.JSONException if the object could not be deserialized
   */
  public Deferred<Branch> fetchBranchOnly(final byte[] branch_id) {
    return store.fetchBranchOnly(branch_id);
  }

  /**
   * Returns the collision set from storage for the given tree, optionally for
   * only the list of TSUIDs provided.
   * <b>Note:</b> This can potentially be a large list if the rule set was
   * written poorly and there were many timeseries so only call this
   * without a list of TSUIDs if you feel confident the number is small.
   *
   * @param tree_id ID of the tree to fetch collisions for
   * @param tsuids An optional list of TSUIDs to fetch collisions for. This may
   * be empty or null, in which case all collisions for the tree will be
   * returned.
   * @return A list of collisions or null if nothing was found
   * @throws IllegalArgumentException if the tree ID was invalid
   */
  public Deferred<Map<String, String>> fetchCollisions(
          final int tree_id,
          final List<String> tsuids) {
    Tree.validateTreeID(tree_id);
    return store.fetchCollisions(tree_id, tsuids);
  }

  /**
   * Returns the not-matched set from storage for the given tree, optionally for
   * only the list of TSUIDs provided.
   * <b>Note:</b> This can potentially be a large list if the rule set was
   * written poorly and there were many timeseries so only call this
   * without a list of TSUIDs if you feel confident the number is small.
   *
   * @param tree_id ID of the tree to fetch non matches for
   * @param tsuids An optional list of TSUIDs to fetch non-matches for. This may
   * be empty or null, in which case all non-matches for the tree will be
   * returned.
   * @return A list of not-matched mappings or null if nothing was found
   * @throws IllegalArgumentException if the tree ID was invalid
   */
  public Deferred<Map<String, String>> fetchNotMatched(final int tree_id,
                                                       final List<String> tsuids) {
    Tree.validateTreeID(tree_id);
    return store.fetchNotMatched(tree_id, tsuids);
  }

  /**
   * Attempts to fetch the given tree from storage, loading the rule set at
   * the same time.
   * @param tree_id The Tree to fetch
   * @return A tree object if found, null if the tree did not exist
   * @throws IllegalArgumentException if the tree ID was invalid
   * @throws org.hbase.async.HBaseException if a storage exception occurred
   * @throws net.opentsdb.utils.JSONException if the object could not be deserialized
  */
  public Deferred<Tree> fetchTree(final int tree_id) {
    Tree.validateTreeID(tree_id);

    return store.fetchTree(tree_id);
  }

  /**
   * Attempts to retrieve the specified tree rule from storage.
   * @param tree_id ID of the tree the rule belongs to
   * @param level Level where the rule resides
   * @param order Order where the rule resides
   * @return A TreeRule object if found, null if it does not exist
   * @throws IllegalArgumentException if the one of the required parameters was
   * missing
   * @throws net.opentsdb.utils.JSONException if the object could not be serialized
   */
  public Deferred<TreeRule> fetchTreeRule(final int tree_id, final int level,
                                          final int order) {

    TreeRule.validateTreeRule(tree_id, level, order);

    return store.fetchTreeRule(tree_id, level, order);
  }

  /**
   * Attempts to flush the collisions to storage. The storage call is a PUT so
   * it will overwrite any existing columns, but since each column is the TSUID
   * it should only exist once and the data shouldn't change.
   * <b>Note:</b> This will also clear the {@link net.opentsdb.tree.Tree#collisions} map
   *
   * @param tree The Tree to flush to storage.
   *
   * @return A meaningless deferred (will always be true since we need to group
   * it with tree store calls) for the caller to wait on
   */
  public Deferred<Boolean> flushTreeCollisions(final Tree tree) {
    if (!tree.getStoreFailures()) {
      tree.getCollisions().clear();
      return Deferred.fromResult(true);
    }

    return store.flushTreeCollisions(tree);
  }

  /**
   * Attempts to flush the non-matches to storage. The storage call is a PUT so
   * it will overwrite any existing columns, but since each column is the TSUID
   * it should only exist once and the data shouldn't change.
   * <b>Note:</b> This will also clear the local {@link net.opentsdb.tree.Tree#not_matched} map
   * @param tree The Tree to flush to storage.
   * @return A meaningless deferred (will always be true since we need to group
   * it with tree store calls) for the caller to wait on
   */
  public Deferred<Boolean> flushTreeNotMatched(final Tree tree) {
    if (!tree.getStoreFailures()) {
      tree.getNotMatched().clear();
      return Deferred.fromResult(true);
    }
    return store.flushTreeNotMatched(tree);
  }

  /**
   * Attempts to write the branch definition and optionally child leaves to
   * storage via CompareAndSets.
   * Each returned deferred will be a boolean regarding whether the CAS call
   * was successful or not. This will be a mix of the branch call and leaves.
   * Some of these may be false, which is OK, because if the branch
   * definition already exists, we don't need to re-write it. Leaves will
   * return false if there was a collision.
   * @param tree The tree to record collisions to
   * @param branch The branch to be stored
   * @param store_leaves Whether or not child leaves should be written to
   * storage
   * @return A list of deferreds to wait on for completion.
   * @throws IllegalArgumentException if the tree ID was missing or data was
   * missing
   */
  public Deferred<ArrayList<Boolean>> storeBranch(final Tree tree,
                                                  final Branch branch,
                                                  final boolean store_leaves) {
    Tree.validateTreeID(branch.getTreeId());

    return store.storeBranch(tree, branch, store_leaves);
  }

  /**
   * Attempts to write the leaf to storage using a CompareAndSet call. We expect
   * the stored value to be null. If it's not, we fetched the stored leaf. If
   * the stored value is the TSUID as the local leaf, we return true since the
   * caller is probably reprocessing a timeseries. If the stored TSUID is
   * different, we store a collision in the tree and return false.
   * <b>Note:</b> You MUST write the tree to storage after calling this as there
   * may be a new collision. Check the tree's collision set.
   * @param leaf The Leaf to put to storage
   * @param branch The branch this leaf belongs to
   * @param tree Tree the leaf and branch belong to
   * @return True if the leaf was stored successful or already existed, false
   * if there was a collision
   * @throws org.hbase.async.HBaseException if there was an issue
   * @throws net.opentsdb.utils.JSONException if the object could not be serialized
   */
  public Deferred<Boolean> storeLeaf(final Leaf leaf, final Branch branch,
                                     final Tree tree) {
    return store.storeLeaf(leaf, branch, tree);
  }

  /**
   * Attempts to store the tree definition via a CompareAndSet call.
   *
   * @param tree The Tree to be stored.
   * @param overwrite Whether or not tree data should be overwritten
   * @return True if the write was successful, false if an error occurred
   * @throws IllegalArgumentException if the tree ID is missing or invalid
   */
  public Deferred<Boolean> storeTree(final Tree tree, final boolean overwrite) {
    Tree.validateTreeID(tree.getTreeId());
    // if there aren't any changes, save time and bandwidth by not writing to
    // storage
    if (!tree.hasChanged()) {
      LOG.debug("{} does not have changes, skipping sync to storage", tree);
      throw new IllegalStateException("No changes detected in the tree");
    }
    return store.storeTree(tree, overwrite);
  }

  /**
   * Attempts to write the rule to storage via CompareAndSet, merging changes
   * with an existing rule.
   * <b>Note:</b> If the local object didn't have any fields set by the caller
   * or there weren't any changes, then the data will not be written and an
   * exception will be thrown.
   * <b>Note:</b> This method also validates the rule, making sure that proper
   * combinations of data exist before writing to storage.
   * @param rule The TreeRule to be stored
   * @param overwrite When the RPC method is PUT, will overwrite all user
   * accessible fields
   * @return True if the CAS call succeeded, false if the stored data was
   * modified in flight. This should be retried if that happens.
   * @throws IllegalArgumentException if parsing failed or the tree ID was
   * invalid or validation failed
   * @throws IllegalStateException if the data hasn't changed. This is OK!
   * @throws net.opentsdb.utils.JSONException if the object could not be serialized
   */
  public Deferred<Boolean> syncTreeRuleToStorage(final TreeRule rule,
                                                 final boolean overwrite) {
    Tree.validateTreeID(rule.getTreeId());

    return store.syncTreeRuleToStorage(rule, overwrite);
  }
}

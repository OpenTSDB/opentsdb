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
package net.opentsdb.tsd;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.PatternSyntaxException;

import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import com.fasterxml.jackson.core.type.TypeReference;
import com.stumbleupon.async.DeferredGroupException;

import net.opentsdb.core.TSDB;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.tree.Branch;
import net.opentsdb.tree.Tree;
import net.opentsdb.tree.TreeBuilder;
import net.opentsdb.tree.TreeRule;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.utils.JSON;

/**
 * Handles API calls for trees such as fetching, editing or deleting trees, 
 * branches and rules.
 * @since 2.0
 */
final class TreeRpc implements HttpRpc {
  /** Type reference for common string/string maps */
  private static TypeReference<HashMap<String, String>> TR_HASH_MAP = 
    new TypeReference<HashMap<String, String>>() {};
    
  /**
   * Routes the request to the proper handler
   * @param tsdb The TSDB to which we belong
   * @param query The HTTP query to use for parsing and responding
   */
  @Override
  public void execute(TSDB tsdb, HttpQuery query) throws IOException {
    // the uri will be /api/vX/tree/? or /api/tree/?
    final String[] uri = query.explodeAPIPath();
    final String endpoint = uri.length > 1 ? uri[1] : ""; 
    
    try {
      if (endpoint.isEmpty()) {
        handleTree(tsdb, query);
      } else if (endpoint.toLowerCase().equals("branch")) {
        handleBranch(tsdb, query);
      } else if (endpoint.toLowerCase().equals("rule")) {
        handleRule(tsdb, query);
      } else if (endpoint.toLowerCase().equals("rules")) {
        handleRules(tsdb, query);
      } else if (endpoint.toLowerCase().equals("test")) {
        handleTest(tsdb, query);
      } else if (endpoint.toLowerCase().equals("collisions")) {
        handleCollisionNotMatched(tsdb, query, true);
      } else if (endpoint.toLowerCase().equals("notmatched")) {
        handleCollisionNotMatched(tsdb, query, false);
      } else {
        throw new BadRequestException(HttpResponseStatus.NOT_FOUND, 
            "This endpoint is not supported");
      }
    } catch (BadRequestException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Handles the plain /tree endpoint CRUD. If a POST or PUT is requested and
   * no tree ID is provided, we'll assume the user wanted to create a new tree.
   * @param tsdb The TSDB to which we belong
   * @param query The HTTP query to work with
   * @throws BadRequestException if the request was invalid.
   */
  private void handleTree(TSDB tsdb, HttpQuery query) {
    final Tree tree;
    if (query.hasContent()) {
      tree = query.serializer().parseTreeV1();
    } else {
      tree = parseTree(query);
    }
    
    try {
      // if get, then we're just returning one or more trees
      if (query.getAPIMethod() == HttpMethod.GET) {
  
        if (tree.getTreeId() == 0) {
          query.sendReply(query.serializer().formatTreesV1(
              Tree.fetchAllTrees(tsdb).joinUninterruptibly()));
        } else {
          final Tree single_tree = Tree.fetchTree(tsdb, tree.getTreeId())
            .joinUninterruptibly();
          if (single_tree == null) {
            throw new BadRequestException(HttpResponseStatus.NOT_FOUND, 
              "Unable to locate tree: " + tree.getTreeId());
          }
          query.sendReply(query.serializer().formatTreeV1(single_tree));
        }
  
      } else if (query.getAPIMethod() == HttpMethod.POST || query.getAPIMethod() == HttpMethod.PUT) {
        // For post or put, we're either editing a tree or creating a new one. 
        // If the tree ID is missing, we need to create a new one, otherwise we 
        // edit an existing tree.
        
        // if the tree ID is set, fetch, copy, save
        if (tree.getTreeId() > 0) {
          if (Tree.fetchTree(tsdb, tree.getTreeId())
              .joinUninterruptibly() == null) {
            throw new BadRequestException(HttpResponseStatus.NOT_FOUND, 
                "Unable to locate tree: " + tree.getTreeId());
          } else {
            if (tree.storeTree(tsdb, (query.getAPIMethod() == HttpMethod.PUT))
                .joinUninterruptibly() != null) {
              final Tree stored_tree = Tree.fetchTree(tsdb, tree.getTreeId())
                .joinUninterruptibly();
              query.sendReply(query.serializer().formatTreeV1(stored_tree));
            } else {
              throw new BadRequestException(
                  HttpResponseStatus.INTERNAL_SERVER_ERROR, 
                  "Unable to save changes to tre tree: " + tree.getTreeId(),
                  "Plesae try again at a later time");
            }
          }
        } else {
          // create a new tree
          final int tree_id = tree.createNewTree(tsdb).joinUninterruptibly(); 
          if (tree_id > 0) {
            final Tree stored_tree = Tree.fetchTree(tsdb, tree_id)
              .joinUninterruptibly();
            query.sendReply(query.serializer().formatTreeV1(stored_tree));
          } else {
            throw new BadRequestException(
                HttpResponseStatus.INTERNAL_SERVER_ERROR, 
                "Unable to save changes to tree: " + tree.getTreeId(),
                "Plesae try again at a later time");
          }
        }
        
      // handle DELETE requests
      } else if (query.getAPIMethod() == HttpMethod.DELETE) {
        boolean delete_definition = false;
        
        if (query.hasContent()) {
          // since we don't want to complicate the Tree class with a "delete 
          // description" flag, we can just double parse the hash map in delete
          // calls
          final String json = query.getContent();
          final HashMap<String, String> properties = 
            JSON.parseToObject(json, TR_HASH_MAP);
          final String delete_all = properties.get("definition");
          if (delete_all != null && delete_all.toLowerCase().equals("true")) {
              delete_definition = true;
          }
        } else {
          final String delete_all = query.getQueryStringParam("definition");
          if (delete_all != null && delete_all.toLowerCase().equals("true")) {
            delete_definition = true;
          }
        }
        
        if (Tree.fetchTree(tsdb, tree.getTreeId()).joinUninterruptibly() == 
          null) {
          throw new BadRequestException(HttpResponseStatus.NOT_FOUND, 
              "Unable to locate tree: " + tree.getTreeId());
        }
        Tree.deleteTree(tsdb, tree.getTreeId(), delete_definition)
          .joinUninterruptibly(); 
        query.sendStatusOnly(HttpResponseStatus.NO_CONTENT);
        
      } else {
        throw new BadRequestException(HttpResponseStatus.BAD_REQUEST, 
          "Unsupported HTTP request method");
      }
      
    } catch (BadRequestException e) {
      throw e;
    } catch (IllegalStateException e) {
      query.sendStatusOnly(HttpResponseStatus.NOT_MODIFIED);
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(e);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  /**
   * Attempts to retrieve a single branch and return it to the user. If the 
   * requested branch doesn't exist, it returns a 404.
   * @param tsdb The TSDB to which we belong
   * @param query The HTTP query to work with
   * @throws BadRequestException if the request was invalid.
   */
  private void handleBranch(TSDB tsdb, HttpQuery query) {
    if (query.getAPIMethod() != HttpMethod.GET) {
      throw new BadRequestException(HttpResponseStatus.BAD_REQUEST, 
        "Unsupported HTTP request method");
    }
    
    try {
      final int tree_id = parseTreeId(query, false);
      final String branch_hex =
          query.getQueryStringParam("branch");
      
      // compile the branch ID. If the user did NOT supply a branch address, 
      // that would include the tree ID, then we fall back to the tree ID and
      // the root for that tree
      final byte[] branch_id;
      if (branch_hex == null || branch_hex.isEmpty()) {
        if (tree_id < 1) {
          throw new BadRequestException(
              "Missing or invalid branch and tree IDs");
        }
        branch_id = Tree.idToBytes(tree_id);
      } else {
        branch_id = Branch.stringToId(branch_hex);
      }
      
      // fetch it
      final Branch branch = Branch.fetchBranch(tsdb, branch_id, true)
        .joinUninterruptibly();
      if (branch == null) {
        throw new BadRequestException(HttpResponseStatus.NOT_FOUND, 
            "Unable to locate branch '" + Branch.idToString(branch_id) + 
            "' for tree '" + Tree.bytesToId(branch_id) + "'");
      }
      query.sendReply(query.serializer().formatBranchV1(branch));
      
    } catch (BadRequestException e) {
      throw e;
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(e);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  /**
   * Handles the CRUD calls for a single rule, enabling adding, editing or 
   * deleting the rule
   * @param tsdb The TSDB to which we belong
   * @param query The HTTP query to work with
   * @throws BadRequestException if the request was invalid.
   */
  private void handleRule(TSDB tsdb, HttpQuery query) {
    final TreeRule rule;
    if (query.hasContent()) {
      rule = query.serializer().parseTreeRuleV1();
    } else {
      rule = parseRule(query);
    }
    
    try {
      
      // no matter what, we'll need a tree to work with, so make sure it exists
      Tree tree = null;
        tree = Tree.fetchTree(tsdb, rule.getTreeId())
          .joinUninterruptibly();
  
      if (tree == null) {
        throw new BadRequestException(HttpResponseStatus.NOT_FOUND, 
            "Unable to locate tree: " + rule.getTreeId());
      }
      
      // if get, then we're just returning a rule from a tree
      if (query.getAPIMethod() == HttpMethod.GET) {
        
        final TreeRule tree_rule = tree.getRule(rule.getLevel(), 
            rule.getOrder());
        if (tree_rule == null) {
          throw new BadRequestException(HttpResponseStatus.NOT_FOUND, 
              "Unable to locate rule: " + rule);
        }
        query.sendReply(query.serializer().formatTreeRuleV1(tree_rule));
        
      } else if (query.getAPIMethod() == HttpMethod.POST || query.getAPIMethod() == HttpMethod.PUT) {
  
        if (rule.syncToStorage(tsdb, (query.getAPIMethod() == HttpMethod.PUT))
            .joinUninterruptibly()) {
          final TreeRule stored_rule = TreeRule.fetchRule(tsdb, 
              rule.getTreeId(), rule.getLevel(), rule.getOrder())
              .joinUninterruptibly();
          query.sendReply(query.serializer().formatTreeRuleV1(stored_rule));
        } else {
          throw new RuntimeException("Unable to save rule " + rule + 
              " to storage");
        }
  
      } else if (query.getAPIMethod() == HttpMethod.DELETE) {
  
        if (tree.getRule(rule.getLevel(), rule.getOrder()) == null) {
          throw new BadRequestException(HttpResponseStatus.NOT_FOUND, 
              "Unable to locate rule: " + rule);
        }
        TreeRule.deleteRule(tsdb, tree.getTreeId(), rule.getLevel(), 
            rule.getOrder()).joinUninterruptibly(); 
        query.sendStatusOnly(HttpResponseStatus.NO_CONTENT);
  
      } else {
        throw new BadRequestException(HttpResponseStatus.BAD_REQUEST, 
          "Unsupported HTTP request method");
      }
    
    } catch (BadRequestException e) {
      throw e;
    } catch (IllegalStateException e) {
      query.sendStatusOnly(HttpResponseStatus.NOT_MODIFIED);
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(e);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  /**
   * Handles requests to replace or delete all of the rules in the given tree.
   * It's an efficiency helper for cases where folks don't want to make a single
   * call per rule when updating many rules at once.
   * @param tsdb The TSDB to which we belong
   * @param query The HTTP query to work with
   * @throws BadRequestException if the request was invalid.
   */
  private void handleRules(TSDB tsdb, HttpQuery query) {
    int tree_id = 0;
    List<TreeRule> rules = null;
    if (query.hasContent()) {
      rules = query.serializer().parseTreeRulesV1();
      if (rules == null || rules.isEmpty()) {
        throw new BadRequestException("Missing tree rules");
      }
      
      // validate that they all belong to the same tree
      tree_id = rules.get(0).getTreeId();
      for (TreeRule rule : rules) {
        if (rule.getTreeId() != tree_id) {
          throw new BadRequestException(
              "All rules must belong to the same tree");
        }
      }
    } else {
      tree_id = parseTreeId(query, false);
    }
    
    // make sure the tree exists
    try {
      if (Tree.fetchTree(tsdb, tree_id).joinUninterruptibly() == null) {
        throw new BadRequestException(HttpResponseStatus.NOT_FOUND, 
            "Unable to locate tree: " + tree_id);
      }
    
      if (query.getAPIMethod() == HttpMethod.POST || query.getAPIMethod() == HttpMethod.PUT) {
        if (rules == null || rules.isEmpty()) {
          if (rules == null || rules.isEmpty()) {
            throw new BadRequestException("Missing tree rules");
          }
        }
        
        // purge the existing tree rules if we're told to PUT
        if (query.getAPIMethod() == HttpMethod.PUT) {
          TreeRule.deleteAllRules(tsdb, tree_id).joinUninterruptibly();
        }
        for (TreeRule rule : rules) {
          rule.syncToStorage(tsdb, query.getAPIMethod() == HttpMethod.PUT)
            .joinUninterruptibly();
        }
        query.sendStatusOnly(HttpResponseStatus.NO_CONTENT);
  
      } else if (query.getAPIMethod() == HttpMethod.DELETE) {
  
        TreeRule.deleteAllRules(tsdb, tree_id).joinUninterruptibly();
        query.sendStatusOnly(HttpResponseStatus.NO_CONTENT);
  
      } else {
        throw new BadRequestException(HttpResponseStatus.BAD_REQUEST, 
          "Unsupported HTTP request method");
      }
    
    } catch (BadRequestException e) {
      throw e;
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(e);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  /**
   * Runs the specified TSMeta object through a tree's rule set to determine
   * what the results would be or debug a meta that wasn't added to a tree
   * successfully
   * @param tsdb The TSDB to which we belong
   * @param query The HTTP query to work with
   * @throws BadRequestException if the request was invalid.
   */
  private void handleTest(TSDB tsdb, HttpQuery query) {
    final Map<String, Object> map;
    if (query.hasContent()) {
      map = query.serializer().parseTreeTSUIDsListV1();
    } else {
      map = parseTSUIDsList(query);
    }
    
    final Integer tree_id = (Integer) map.get("treeId");
    if (tree_id == null) {
      throw new BadRequestException("Missing or invalid Tree ID");
    }
    
    // make sure the tree exists
    Tree tree = null;
    try {
      
      tree = Tree.fetchTree(tsdb, tree_id).joinUninterruptibly();
      if (tree == null) {
        throw new BadRequestException(HttpResponseStatus.NOT_FOUND, 
            "Unable to locate tree: " + tree_id);
      }
      
      // ugly, but keeps from having to create a dedicated class just to 
      // convert one field.
      @SuppressWarnings("unchecked")
      final List<String> tsuids = (List<String>)map.get("tsuids");
      if (tsuids == null || tsuids.isEmpty()) {
        throw new BadRequestException("Missing or empty TSUID list");
      }
      
      if (query.getAPIMethod() == HttpMethod.GET || query.getAPIMethod() == HttpMethod.POST ||
          query.getAPIMethod() == HttpMethod.PUT) {
        
        final HashMap<String, HashMap<String, Object>> results = 
          new HashMap<String, HashMap<String, Object>>(tsuids.size());
        final TreeBuilder builder = new TreeBuilder(tsdb, tree);
        for (String tsuid : tsuids) {
          final HashMap<String, Object> tsuid_results = 
            new HashMap<String, Object>();
          
          try {
            final TSMeta meta = TSMeta.getTSMeta(tsdb, tsuid)
              .joinUninterruptibly();
            // if the meta doesn't exist, we can't process, so just log a 
            // message to the results and move on to the next TSUID
            if (meta == null) {
              tsuid_results.put("branch", null);
              tsuid_results.put("meta", null);
              final ArrayList<String> messages = new ArrayList<String>(1);
              messages.add("Unable to locate TSUID meta data");
              tsuid_results.put("messages", messages);
              results.put(tsuid, tsuid_results);
              continue;
            }
            
            builder.processTimeseriesMeta(meta, true).joinUninterruptibly();
            tsuid_results.put("branch", builder.getRootBranch());
            tsuid_results.put("meta", meta);
            tsuid_results.put("messages", builder.getTestMessage());
            
            results.put(tsuid, tsuid_results);
          } catch (DeferredGroupException e) {
            // we want to catch NSU errors and handle them gracefully for
            // TSUIDs where they may have been deleted
            Throwable ex = e;
            while (ex.getClass().equals(DeferredGroupException.class)) {
              ex = ex.getCause();
            }
            
            if (ex.getClass().equals(NoSuchUniqueId.class)) {
              tsuid_results.put("branch", null);
              tsuid_results.put("meta", null);
              final ArrayList<String> messages = new ArrayList<String>(1);
              messages.add("TSUID was missing a UID name: " + ex.getMessage());
              tsuid_results.put("messages", messages);
              results.put(tsuid, tsuid_results);
            }
          }
        }
        
        query.sendReply(query.serializer().formatTreeTestV1(results));
  
      } else {
        throw new BadRequestException(HttpResponseStatus.BAD_REQUEST, 
          "Unsupported HTTP request method");
      }
    
    } catch (BadRequestException e) {
      throw e;
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(e);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  /**
   * Handles requests to fetch collisions or not-matched entries for the given
   * tree. To cut down on code, this method uses a flag to determine if we want
   * collisions or not-matched entries, since they both have the same data types.
   * @param tsdb The TSDB to which we belong
   * @param query The HTTP query to work with
   * @param for_collisions
   */
  private void handleCollisionNotMatched(TSDB tsdb, HttpQuery query, final boolean for_collisions) {
    final Map<String, Object> map;
    if (query.hasContent()) {
      map = query.serializer().parseTreeTSUIDsListV1();
    } else {
      map = parseTSUIDsList(query);
    }
    
    final Integer tree_id = (Integer) map.get("treeId");
    if (tree_id == null) {
      throw new BadRequestException("Missing or invalid Tree ID");
    }
    
    // make sure the tree exists
    try {
      
      if (Tree.fetchTree(tsdb, tree_id).joinUninterruptibly() == null) {
        throw new BadRequestException(HttpResponseStatus.NOT_FOUND, 
            "Unable to locate tree: " + tree_id);
      }
  
      if (query.getAPIMethod() == HttpMethod.GET || query.getAPIMethod() == HttpMethod.POST ||
          query.getAPIMethod() == HttpMethod.PUT) {
  
        // ugly, but keeps from having to create a dedicated class just to 
        // convert one field.
        @SuppressWarnings("unchecked")
        final List<String> tsuids = (List<String>)map.get("tsuids");
        final Map<String, String> results = for_collisions ? 
            Tree.fetchCollisions(tsdb, tree_id, tsuids).joinUninterruptibly() :
              Tree.fetchNotMatched(tsdb, tree_id, tsuids).joinUninterruptibly();
        query.sendReply(query.serializer().formatTreeCollisionNotMatchedV1(
            results, for_collisions));
  
      } else {
        throw new BadRequestException(HttpResponseStatus.BAD_REQUEST, 
        "Unsupported HTTP request method");
      }
    
    } catch (ClassCastException e) {
      throw new BadRequestException(
          "Unable to convert the given data to a list", e);
    } catch (BadRequestException e) {
      throw e;
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(e);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  /**
   * Parses query string parameters into a blank tree object. Used for updating
   * tree meta data.
   * @param query The HTTP query to work with
   * @return A tree object filled in with changes
   * @throws BadRequestException if some of the data was invalid
   */
  private Tree parseTree(HttpQuery query) {
    final Tree tree = new Tree(parseTreeId(query, false));
    if (query.hasQueryStringParam("name")) {
      tree.setName(query.getQueryStringParam("name"));
    }
    if (query.hasQueryStringParam("description")) {
      tree.setDescription(query.getQueryStringParam("description"));
    }
    if (query.hasQueryStringParam("notes")) {
      tree.setNotes(query.getQueryStringParam("notes"));
    }
    if (query.hasQueryStringParam("strict_match")) {
      if (query.getQueryStringParam("strict_match").toLowerCase()
          .equals("true")) {
        tree.setStrictMatch(true);
      } else {
        tree.setStrictMatch(false);
      }
    }
    if (query.hasQueryStringParam("enabled")) {
      final String enabled = query.getQueryStringParam("enabled");
      if (enabled.toLowerCase().equals("true")) {
        tree.setEnabled(true);
      } else {
        tree.setEnabled(false);
      }
    }
    if (query.hasQueryStringParam("store_failures")) {
      if (query.getQueryStringParam("store_failures").toLowerCase()
          .equals("true")) {
        tree.setStoreFailures(true);
      } else {
        tree.setStoreFailures(false);
      }
    }
    return tree;
  }

  /**
   * Parses query string parameters into a blank tree rule object. Used for 
   * updating individual rules
   * @param query The HTTP query to work with
   * @return A rule object filled in with changes
   * @throws BadRequestException if some of the data was invalid
   */
  private TreeRule parseRule(HttpQuery query) {
    final TreeRule rule = new TreeRule(parseTreeId(query, true));
    
    if (query.hasQueryStringParam("type")) {
      try {
        rule.setType(TreeRule.stringToType(query.getQueryStringParam("type")));
      } catch (IllegalArgumentException e) {
        throw new BadRequestException("Unable to parse the 'type' parameter", e);
      }
    }
    if (query.hasQueryStringParam("field")) {
      rule.setField(query.getQueryStringParam("field"));
    }
    if (query.hasQueryStringParam("custom_field")) {
      rule.setCustomField(query.getQueryStringParam("custom_field"));
    }
    if (query.hasQueryStringParam("regex")) {
      try {
        rule.setRegex(query.getQueryStringParam("regex"));
      } catch (PatternSyntaxException e) {
        throw new BadRequestException(
            "Unable to parse the 'regex' parameter", e);
      }
    }
    if (query.hasQueryStringParam("separator")) {
      rule.setSeparator(query.getQueryStringParam("separator"));
    }
    if (query.hasQueryStringParam("description")) {
      rule.setDescription(query.getQueryStringParam("description"));
    }
    if (query.hasQueryStringParam("notes")) {
      rule.setNotes(query.getQueryStringParam("notes"));
    }
    if (query.hasQueryStringParam("regex_group_idx")) {
      try {
        rule.setRegexGroupIdx(Integer.parseInt(
            query.getQueryStringParam("regex_group_idx")));
      } catch (NumberFormatException e) {
        throw new BadRequestException(
            "Unable to parse the 'regex_group_idx' parameter", e);
      }
    }
    if (query.hasQueryStringParam("display_format")) {
      rule.setDisplayFormat(query.getQueryStringParam("display_format"));
    }
    //if (query.hasQueryStringParam("level")) {
      try {
        rule.setLevel(Integer.parseInt(
            query.getRequiredQueryStringParam("level")));
      } catch (NumberFormatException e) {
        throw new BadRequestException(
            "Unable to parse the 'level' parameter", e);
      }
    //}
    //if (query.hasQueryStringParam("order")) {
      try {
        rule.setOrder(Integer.parseInt(
            query.getRequiredQueryStringParam("order")));
      } catch (NumberFormatException e) {
        throw new BadRequestException(
            "Unable to parse the 'order' parameter", e);
      }
    //}
    return rule;
  }
  
  /**
   * Parses the tree ID from a query
   * Used often so it's been broken into it's own method
   * @param query The HTTP query to work with
   * @param required Whether or not the ID is required for the given call
   * @return The tree ID or 0 if not provided
   */
  private int parseTreeId(HttpQuery query, final boolean required) {
    try{
      if (required) {
        return Integer.parseInt(query.getRequiredQueryStringParam("treeid"));
      } else {
        if (query.hasQueryStringParam("treeid")) {
          return Integer.parseInt(query.getQueryStringParam("treeid"));
        } else {
          return 0;
        }
      }
    } catch (NumberFormatException nfe) {
      throw new BadRequestException("Unable to parse 'tree' value", nfe);
    }
  }

  /**
   * Used to parse a list of TSUIDs from the query string for collision or not
   * matched requests. TSUIDs must be comma separated.
   * @param query The HTTP query to work with
   * @return A map with a list of tsuids. If found, the tsuids array will be 
   * under the "tsuid" key. The map is necessary for compatability with POJO 
   * parsing. 
   */
  private Map<String, Object> parseTSUIDsList(HttpQuery query) {
    final HashMap<String, Object> map = new HashMap<String, Object>();
    map.put("treeId", parseTreeId(query, true));
    
    final String tsquery = query.getQueryStringParam("tsuids");
    if (tsquery != null) {
      final String[] tsuids = tsquery.split(",");
      map.put("tsuids", Arrays.asList(tsuids));
    }
    
    return map;
  }
}

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


import java.util.HashMap;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import com.google.common.collect.ImmutableMap;
import net.opentsdb.core.Const;
import net.opentsdb.utils.JSON;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

/**
 * Represents single rule in a set of rules for a given tree. Each rule is
 * uniquely identified by:
 * <ul><li>tree_id - The ID of the tree to which the rule belongs</li>
 * <li>level - Outer processing order where the rule resides. Lower values are
 * processed first. Starts at 0.</li>
 * <li>order - Inner processing order within a given level. Lower values are
 * processed first. Starts at 0.</li></ul>
 * Each rule is stored as an individual column so that they can be modified
 * individually. RPC calls can also bulk replace rule sets.
 * @since 2.0
 */
@JsonIgnoreProperties(ignoreUnknown = true) 
@JsonAutoDetect(fieldVisibility = Visibility.PUBLIC_ONLY)
public final class TreeRule {

  /** Types of tree rules */
  public enum TreeRuleType {
    METRIC,         /** A simple metric rule */
    METRIC_CUSTOM,  /** Matches on UID Meta custom field */
    TAGK,           /** Matches on a tagk name */
    TAGK_CUSTOM,    /** Matches on a UID Meta custom field */
    TAGV_CUSTOM     /** Matches on a UID Meta custom field */
  }

  /** Type of rule */
  @JsonDeserialize(using = JSON.TreeRuleTypeDeserializer.class)
  private TreeRuleType type = null;
  
  /** Name of the field to match on if applicable */
  private String field = "";
  
  /** Name of the custom field to match on, the key */
  private String custom_field = "";
  
  /** User supplied regular expression before parsing */
  private String regex = "";
  
  /** Separation character or string */
  private String separator = "";
  
  /** An optional description of the rule */
  private String description = "";
  
  /** Optional notes about the rule */
  private String notes = "";
  
  /** Optional group index for extracting from regex matches */
  private int regex_group_idx = 0;
  
  /** Optioanl display format override */
  private String display_format = "";
  
  /** Required level where the rule resides */
  private int level = 0;
  
  /** Required order where the rule resides */
  private int order = 0;
  
  /** The tree this rule belongs to */
  private int tree_id = 0;
  
  /** Compiled regex pattern, compiled after processing */
  private Pattern compiled_regex = null;
  
  /** Tracks fields that have changed by the user to avoid overwrites */
  private final HashMap<String, Boolean> changed = 
    new HashMap<String, Boolean>();
  
  /**
   * Default constructor necessary for de/serialization
   */
  public TreeRule() {
    initializeChangedMap();
  }
  
  /**
   * Constructor initializes the tree ID
   * @param tree_id The tree this rule belongs to
   */
  public TreeRule(final int tree_id) {
    this.tree_id = tree_id;
    initializeChangedMap();
  }

  /**
   * Copy constructor that creates a completely independent copy of the original
   * object
   * @param original The original object to copy from
   * @throws PatternSyntaxException if the regex is invalid
   */
  public TreeRule(final TreeRule original) {
    custom_field = original.custom_field;
    description = original.description;
    display_format = original.display_format;
    field = original.field;
    level = original.level;
    notes = original.notes;
    order = original.order;
    regex_group_idx = original.regex_group_idx;
    separator = original.separator;
    tree_id = original.tree_id;
    type = original.type;
    setRegex(original.regex);
    initializeChangedMap();
  }
  
  /**
   * Copies changed fields from the incoming rule to the local rule
   * @param rule The rule to copy from
   * @param overwrite Whether or not to replace all fields in the local object
   * @return True if there were changes, false if everything was identical
   */
  public boolean copyChanges(final TreeRule rule, final boolean overwrite) {
    if (rule == null) {
      throw new IllegalArgumentException("Cannot copy a null rule");
    }
    if (tree_id != rule.tree_id) {
      throw new IllegalArgumentException("Tree IDs do not match");
    }
    if (level != rule.level) {
      throw new IllegalArgumentException("Levels do not match");
    }
    if (order != rule.order) {
      throw new IllegalArgumentException("Orders do not match");
    }
    
    if (overwrite || (rule.changed.get("type") && type != rule.type)) {
      type = rule.type;
      changed.put("type", true);
    }
    if (overwrite || (rule.changed.get("field") && !field.equals(rule.field))) {
      field = rule.field;
      changed.put("field", true);
    }
    if (overwrite || (rule.changed.get("custom_field") && 
        !custom_field.equals(rule.custom_field))) {
      custom_field = rule.custom_field;
      changed.put("custom_field", true);
    }
    if (overwrite || (rule.changed.get("regex") && !regex.equals(rule.regex))) {
      // validate and compile via the setter
      setRegex(rule.regex);
    }
    if (overwrite || (rule.changed.get("separator") && 
        !separator.equals(rule.separator))) {
      separator = rule.separator;
      changed.put("separator", true);
    }
    if (overwrite || (rule.changed.get("description") &&
        !description.equals(rule.description))) {
      description = rule.description;
      changed.put("description", true);
    }
    if (overwrite || (rule.changed.get("notes") && !notes.equals(rule.notes))) {
      notes = rule.notes;
      changed.put("notes", true);
    }
    if (overwrite || (rule.changed.get("regex_group_idx") && 
        regex_group_idx != rule.regex_group_idx)) {
      regex_group_idx = rule.regex_group_idx;
      changed.put("regex_group_idx", true);
    }
    if (overwrite || (rule.changed.get("display_format") && 
        !display_format.equals(rule.display_format))) {
      display_format = rule.display_format;
      changed.put("display_format", true);
    }
    for (boolean has_changes : changed.values()) {
      if (has_changes) {
        return true;
      }
    }
    return false;
  }
  
  /** @return the rule ID as [tree_id:level:order] */
  @Override
  public String toString() {
    return "[" + tree_id + ":" + level + ":" + order + ":" + type + "]";
  }
  
  /**
   * Parses a string into a rule type enumerator
   * @param type The string to parse
   * @return The type enumerator
   * @throws IllegalArgumentException if the type was empty or invalid
   */
  public static TreeRuleType stringToType(final String type) {
    if (type == null || type.isEmpty()) {
      throw new IllegalArgumentException("Rule type was empty");
    } else if (type.toLowerCase().equals("metric")) {
      return TreeRuleType.METRIC;
    } else if (type.toLowerCase().equals("metric_custom")) {
      return TreeRuleType.METRIC_CUSTOM;
    } else if (type.toLowerCase().equals("tagk")) {
      return TreeRuleType.TAGK;
    } else if (type.toLowerCase().equals("tagk_custom")) {
      return TreeRuleType.TAGK_CUSTOM;
    } else if (type.toLowerCase().equals("tagv_custom")) {
      return TreeRuleType.TAGV_CUSTOM;
    } else {
      throw new IllegalArgumentException("Unrecognized rule type");
    }
  }

  /**
   * Completes the column qualifier given a level and order using the configured
   * prefix
   * @param level The level of the rule
   * @param order The order of the rule
   * @return A byte array with the column qualifier
   */
  public static byte[] getQualifier(final int level, final int order) {
    final byte[] suffix = (level + ":" + order).getBytes(Const.CHARSET_ASCII);
    final byte[] qualifier = new byte[Const.TREE_RULE_PREFIX.length + suffix.length];
    System.arraycopy(Const.TREE_RULE_PREFIX, 0, qualifier, 0, Const.TREE_RULE_PREFIX.length);
    System.arraycopy(suffix, 0, qualifier, Const.TREE_RULE_PREFIX.length, suffix.length);
    return qualifier;
  }
  
  /**
   * Sets or resets the changed map flags
   */
  public void initializeChangedMap() {
    // set changed flags
    changed.put("type", false);
    changed.put("field", false);
    changed.put("custom_field", false);
    changed.put("regex", false);
    changed.put("separator", false);
    changed.put("description", false);
    changed.put("notes", false);
    changed.put("regex_group_idx", false);
    changed.put("display_format", false);
    changed.put("level", false);
    changed.put("order", false);
    // tree_id can't change
  }
  
  /**
   * Checks that the local rule has valid data, i.e. that for different types
   * of rules, the proper parameters exist. For example, a {@code TAGV_CUSTOM}
   * rule must have a valid {@code field} parameter set.
   * @throws IllegalArgumentException if an invalid combination of parameters
   * is provided
   */
  public void validateRule() {
    if (type == null) {
      throw new IllegalArgumentException(
        "Missing rule type");
    }
    
    switch (type) {
      case METRIC:
        // nothing to validate
        break;
      case METRIC_CUSTOM:
      case TAGK_CUSTOM:
      case TAGV_CUSTOM:
        if (field == null || field.isEmpty()) {
          throw new IllegalArgumentException(
              "Missing field name required for " + type + " rule");
        }
        if (custom_field == null || custom_field.isEmpty()) {
          throw new IllegalArgumentException(
              "Missing custom field name required for " + type + " rule");
        }
        break;
      case TAGK:
        if (field == null || field.isEmpty()) {
          throw new IllegalArgumentException(
              "Missing field name required for " + type + " rule");
        }
        break;
      default:
        throw new IllegalArgumentException("Invalid rule type");
    }
    
    if ((regex != null || !regex.isEmpty()) && regex_group_idx < 0) {
      throw new IllegalArgumentException(
          "Invalid regex group index. Cannot be less than 0");
    }
  }

  // GETTERS AND SETTERS ----------------------------
  
  /** @return the type of rule*/
  public TreeRuleType getType() {
    return type;
  }

  /** @return the name of the field to match on */
  public String getField() {
    return field;
  }

  /** @return the custom_field if matching */
  public String getCustomField() {
    return custom_field;
  }

  /** @return the user supplied, uncompiled regex */
  public String getRegex() {
    return regex;
  }

  /** @return an optional separator*/
  public String getSeparator() {
    return separator;
  }

  /** @return the description of the rule*/
  public String getDescription() {
    return description;
  }

  /** @return the notes */
  public String getNotes() {
    return notes;
  }

  /**  @return the regex_group_idx if using regex group extraction */
  public int getRegexGroupIdx() {
    return regex_group_idx;
  }

  /** @return the display_format */
  public String getDisplayFormat() {
    return display_format;
  }

  /** @return the level where the rule resides*/
  public int getLevel() {
    return level;
  }

  /** @return the order of rule processing within a level */
  public int getOrder() {
    return order;
  }

  /** @return the tree_id */
  public int getTreeId() {
    return tree_id;
  }

  /** @return the compiled_regex */
  @JsonIgnore
  public Pattern getCompiledRegex() {
    return compiled_regex;
  }

  /** @param type The type of rule */
  public void setType(TreeRuleType type) {
    if (this.type != type) {
      changed.put("type", true);
      this.type = type;      
    }
  }

  /** @param field The field name for matching */
  public void setField(String field) {
    if (!this.field.equals(field)) {
      changed.put("field", true);
      this.field = field;
    }
  }

  /** @param custom_field The custom field name to set if matching */
  public void setCustomField(String custom_field) {
    if (!this.custom_field.equals(custom_field)) {
      changed.put("custom_field", true);
      this.custom_field = custom_field;
    }
  }

  /** 
   * @param regex Stores AND compiles the regex string for use in processing
   * @throws PatternSyntaxException if the regex is invalid
   */
  public void setRegex(String regex) {
    if (!this.regex.equals(regex)) {
      changed.put("regex", true);
      this.regex = regex;
      if (regex != null && !regex.isEmpty()) {
        this.compiled_regex = Pattern.compile(regex);
      } else {
        this.compiled_regex = null;
      }
    }
  }

  /** @param separator A character or string to separate on */
  public void setSeparator(String separator) {
    if (!this.separator.equals(separator)) {
      changed.put("separator", true);
      this.separator = separator;
    }
  }

  /** @param description A brief description of the rule */
  public void setDescription(String description) {
    if (!this.description.equals(description)) {
      changed.put("description", true);
      this.description = description;
    }
  }

  /** @param notes Optional detailed notes about the rule */
  public void setNotes(String notes) {
    if (!this.notes.equals(notes)) {
      changed.put("notes", true);
      this.notes = notes;
    }
  }

  /** @param regex_group_idx An optional index (start at 0) to use for regex 
   * group extraction. Must be a positive value. */
  public void setRegexGroupIdx(int regex_group_idx) {
    if (this.regex_group_idx != regex_group_idx) {
      changed.put("regex_group_idx", true);
      this.regex_group_idx = regex_group_idx;
    }
  }

  /** @param display_format Optional format string to alter the display name */
  public void setDisplayFormat(String display_format) {
    if (!this.display_format.equals(display_format)) {
      changed.put("display_format", true);
      this.display_format = display_format;
    }
  }

  /** @param level The top level processing order. Must be 0 or greater 
   * @throws IllegalArgumentException if the level was negative */
  public void setLevel(int level) {
    if (level < 0) {
      throw new IllegalArgumentException("Negative levels are not allowed");
    }
    if (this.level != level) {
      changed.put("level", true);
      this.level = level;
    }
  }

  /** @param order The order of processing within a level. 
   * Must be 0 or greater 
   * @throws IllegalArgumentException if the order was negative */
  public void setOrder(final int order) {
    if (level < 0) {
      throw new IllegalArgumentException("Negative orders are not allowed");
    }
    if (this.order != order) {
      changed.put("order", true);
      this.order = order;
    }
  }
  public final ImmutableMap<String, Boolean> getChanged(){
    ImmutableMap.Builder<String, Boolean> builder = ImmutableMap.builder();
    builder.putAll(changed);
    return  builder.build();
  }

  /**
   * Validates the TreeRule if the parameters are invalid it throws an
   * IllegalArgumentException. It utilizes the function @see
   * {@link Tree#validateTreeID(int)} to check the tree_id.
   *
   * @param tree_id The tree ID to be validated.
   * @param level The rule level to be validated.
   * @param order The rule order to be validated.
   */
  public static void validateTreeRule(final int tree_id,
                                      final int level,
                                      final int order){
    Tree.validateTreeID(tree_id);
    if (level < 0) {
      throw new IllegalArgumentException("Invalid rule level");
    }
    if (order < 0) {
      throw new IllegalArgumentException("Invalid rule order");
    }
  }

}

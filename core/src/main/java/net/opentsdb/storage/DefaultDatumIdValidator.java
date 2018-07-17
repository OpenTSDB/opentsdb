// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.storage;

import java.util.Map.Entry;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CharMatcher;
import com.stumbleupon.async.Deferred;

import net.opentsdb.common.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSDBPlugin;
import net.opentsdb.data.TimeSeriesDatumId;
import net.opentsdb.data.TimeSeriesDatumStringId;

/**
 * Validates a datum ID, making sure the metric, tag keys and tag values
 * conform to the configured or default specification. Each component (
 * metric, tag key, tag value) can be evaluated for UTF or ASCII encoding 
 * and other characters. Likewise a length can be imposed for all types.
 * <p>
 * Under ASCII, only characters, digits and '_', '-', '.' and '/' are 
 * allowed. An additional list of characters can be supplied at start up
 * for allowances and whitespace can also be allowed optionally. 
 * Control characters are not allowed.
 * <p>
 * Under UTF, all characters are allowed except for control characters
 * and whitespace. Whitespace can be toggled per type.
 * 
 * TODO - namespace
 * 
 * @since 3.0
 */
public class DefaultDatumIdValidator implements DatumIdValidator, TSDBPlugin {

  public static final String METRIC_UTF_KEY = "tsdb.validator.id.metric.utf8";
  public static final String TAGK_UTF_KEY = "tsdb.validator.id.tagk.utf8";
  public static final String TAGV_UTF_KEY = "tsdb.validator.id.tagv.utf8";
  
  public static final String METRIC_WHITESPACE_KEY = 
      "tsdb.validator.id.metric.allow_whitespace";
  public static final String TAGK_WHITESPACE_KEY = 
      "tsdb.validator.id.tagk.allow_whitespace";
  public static final String TAGV_WHITESPACE_KEY = 
      "tsdb.validator.id.tagv.allow_whitespace";
  
  public static final String METRIC_LENGTH_KEY = "tsdb.validator.id.metric.length";
  public static final String TAGK_LENGTH_KEY = "tsdb.validator.id.tagk.length";
  public static final String TAGV_LENGTH_KEY = "tsdb.validator.id.tagv.length";
  
  public static final String METRIC_SPECIAL_KEY = "tsdb.validator.id.metric.special_chars";
  public static final String TAGK_SPECIAL_KEY = "tsdb.validator.id.tagk.special_chars";
  public static final String TAGV_SPECIAL_KEY = "tsdb.validator.id.tagv.special_chars";
  
  public static final String REQUIRE_TAGS_KEY = "tsdb.validator.id.require_tags";
  
  /**
   * An enum used locally to determine the type of string we're validating.
   */
  protected static enum Type {
    METRIC,
    TAGK,
    TAGV
  }
  
  /** Whether or not the type is encoded in UTF8 or extended ASCII. */
  protected boolean metric_utf8;
  protected boolean tagk_utf8;
  protected boolean tagv_utf8;
  
  /** Whethe ror not white spaces are allowed in the given type. */
  protected boolean metric_allow_whitespace;
  protected boolean tagk_allow_whitespace;
  protected boolean tagv_allow_whitespace;
  
  /** The maximum length of each type. */
  protected int metric_length;
  protected int tagk_length;
  protected int tagv_length;
  
  /** Special chars allowed in ASCII mode. */
  protected String metric_special_chars;
  protected String tagk_special_chars;
  protected String tagv_special_chars;
  
  /** Whether or not at least one tag is required for the ID. */
  protected boolean require_at_least_one_tag;
  
  @Override
  public String validate(final TimeSeriesDatumId id) {
    if (id == null) {
      throw new IllegalArgumentException("ID cannot be null.");
    }
    
    if (id.type() != Const.TS_STRING_ID) {
      return "Id must be of the a String type";
    }
    final TimeSeriesDatumStringId string_id = (TimeSeriesDatumStringId) id;
    String error = null;
    if (metric_utf8) {
      error = validateUTFString(Type.METRIC, string_id.metric());
    } else {
      error = validateASCIIString(Type.METRIC, string_id.metric());
    }
    if (error != null) {
      return error;
    }
    if (metric_length > 0 && string_id.metric().length() > metric_length) {
      return "Metric length must be equal to or less than " + metric_length;
    }
    if (string_id.tags() == null || string_id.tags().isEmpty()) {
      if (require_at_least_one_tag) {
        return "At least one tag pair must be present.";
      }
      return null;
    }
    
    for (final Entry<String, String> tags : string_id.tags().entrySet()) {
      if (tagk_utf8) {
        error = validateUTFString(Type.TAGK, tags.getKey());
      } else {
        error = validateASCIIString(Type.TAGK, tags.getKey());
      }
      if (error != null) {
        return error;
      }
      if (tagk_length > 0 && tags.getKey().length() > tagk_length) {
        return "Tag key length (" + tags.getKey() + ") must be equal "
            + "to or less than " + tagk_length;
      }
      
      if (tagv_utf8) {
        error = validateUTFString(Type.TAGV, tags.getValue());
      } else {
        error = validateASCIIString(Type.TAGV, tags.getValue());
      }
      if (error != null) {
        return error;
      }
      if (tagv_length > 0 && tags.getValue().length() > tagv_length) {
        return "Tag value length (" + tags.getValue() + ") must be equal "
            + "to or less than " + tagv_length;
      }
    }
    
    return null;
  }

  @Override
  public String id() {
    return "DefaultIdValidator";
  }

  @Override
  public Deferred<Object> initialize(final TSDB tsdb) {
    registerConfigs(tsdb);
    
    // now set our local vars
    metric_utf8 = tsdb.getConfig().getBoolean(METRIC_UTF_KEY);
    tagk_utf8 = tsdb.getConfig().getBoolean(TAGK_UTF_KEY);
    tagv_utf8 = tsdb.getConfig().getBoolean(TAGV_UTF_KEY);
    
    metric_allow_whitespace = tsdb.getConfig().getBoolean(METRIC_WHITESPACE_KEY);
    tagk_allow_whitespace = tsdb.getConfig().getBoolean(TAGK_WHITESPACE_KEY);
    tagv_allow_whitespace = tsdb.getConfig().getBoolean(TAGV_WHITESPACE_KEY);
    
    metric_length = tsdb.getConfig().getInt(METRIC_LENGTH_KEY);
    tagk_length = tsdb.getConfig().getInt(TAGK_LENGTH_KEY);
    tagv_length = tsdb.getConfig().getInt(TAGV_LENGTH_KEY);
    
    metric_special_chars = tsdb.getConfig().getString(METRIC_SPECIAL_KEY);
    tagk_special_chars = tsdb.getConfig().getString(TAGK_SPECIAL_KEY);
    tagv_special_chars = tsdb.getConfig().getString(TAGV_SPECIAL_KEY);
    
    require_at_least_one_tag = tsdb.getConfig().getBoolean(REQUIRE_TAGS_KEY);
    
    return Deferred.fromResult(null);
  }

  @Override
  public Deferred<Object> shutdown() {
    return Deferred.fromResult(null);
  }

  @Override
  public String version() {
    return "3.0.0";
  }

  /**
   * Validates an ASCII string.
   * @param type The type of string.
   * @param s The string to evaluate.
   * @return Null if the string is good, an error if not.
   */
  @VisibleForTesting
  String validateASCIIString(final Type type, final String s) {
    if (s == null) {
      return "Invalid " + type + ": null";
    } else if ("".equals(s)) {
      return "Invalid " + type + ": empty string";
    }
    if (!CharMatcher.ASCII.matchesAllOf(s)) {
      return "Invalid " + type + ": Contains non-ASCII characters";
    }
    final int n = s.length();
    for (int i = 0; i < n; i++) {
      final char c = s.charAt(i);
      if (!(('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') 
          || ('0' <= c && c <= '9') || c == '-' || c == '_' || c == '.' 
          || c == '/' || Character.isLetter(c) || isAllowdSpecialChars(type, c))) {
        return "Invalid " + type + " (\"" + s + "\"): illegal character: " + c;
      }
    }
    return null;
  }
  
  /**
   * Validates a UTF string.
   * @param type The type of string.
   * @param s The string to evaluate.
   * @return Null if the string is good, an error if not.
   */
  @VisibleForTesting
  String validateUTFString(final Type type, final String s) {
    if (s == null) {
      return "Invalid " + type + ": null";
    } else if ("".equals(s)) {
      return "Invalid " + type + ": empty string";
    }
    for (int i = 0; i < s.length(); i++) {
      int cp = s.codePointAt(i);
      if (!Character.isDefined(cp)) {
        return "Invalid " + type + " (\"" + s + "\"): Undefined Unicode "
            + "character at: " + i;
      } else if (Character.isISOControl(cp)) {
        return "Invalid " + type + " (\"" + s + "\"): Control character "
            + "not allowed at: " + i;
      }

      switch (type) {
      case METRIC:
        if (!metric_allow_whitespace && Character.isWhitespace(cp)) {
          return "Invalid " + type + " (\"" + s + "\"): Whitespace "
              + "not allowed at: " + i;
        }
        break;
      case TAGK:
        if (!tagk_allow_whitespace && Character.isWhitespace(cp)) {
          return "Invalid " + type + " (\"" + s + "\"): Whitespace "
              + "not allowed at: " + i;
        }
        break;
      case TAGV:
        if (!tagv_allow_whitespace && Character.isWhitespace(cp)) {
          return "Invalid " + type + " (\"" + s + "\"): Whitespace "
              + "not allowed at: " + i;
        }
        break;
      default:
        throw new IllegalArgumentException("Unexpected type: " + type);
      }
    }
    
    return null;
  }
  
  /**
   * Returns true if the character can be used in an ASCII name.
   * @param type The non-null type to evaluate.
   * @param character The character to evaluate.
   * @return True if the character is allowed, false if not.
   */
  boolean isAllowdSpecialChars(final Type type, final char character) {
    switch (type) {
    case METRIC:
      if (character == ' ' && metric_allow_whitespace) {
        return true;
      }
      return metric_special_chars != null ? 
          (metric_special_chars.indexOf(character) != -1) : false;
    case TAGK:
      if (character == ' ' && tagk_allow_whitespace) {
        return true;
      }
      return tagk_special_chars != null ? 
          (tagk_special_chars.indexOf(character) != -1) : false;
    case TAGV:
      if (character == ' ' && tagv_allow_whitespace) {
        return true;
      }
      return tagv_special_chars != null ? 
          (tagv_special_chars.indexOf(character) != -1) : false;
    default:
      throw new IllegalArgumentException("Unexpected type: " + type);
    }
  }

  /**
   * Registers the configurations this plugin uses.
   * @param tsdb The non-null TSDB
   */
  @VisibleForTesting
  void registerConfigs(final TSDB tsdb) {
    if (!tsdb.getConfig().hasProperty(METRIC_UTF_KEY)) {
      tsdb.getConfig().register(METRIC_UTF_KEY, false, false, 
          "Whether or not metric names are encoded in UTF8 or extended ASCII.");
    }
    if (!tsdb.getConfig().hasProperty(TAGK_UTF_KEY)) {
      tsdb.getConfig().register(TAGK_UTF_KEY, false, false, 
          "Whether or not tag key names are encoded in UTF8 or extended ASCII.");
    }
    if (!tsdb.getConfig().hasProperty(TAGV_UTF_KEY)) {
      tsdb.getConfig().register(TAGV_UTF_KEY, true, false, 
          "Whether or not tag value names are encoded in UTF8 or extended ASCII.");
    }
    
    if (!tsdb.getConfig().hasProperty(METRIC_WHITESPACE_KEY)) {
      tsdb.getConfig().register(METRIC_WHITESPACE_KEY, false, false, 
          "Whether or not whitespace characters are allowed in metric names.");
    }
    if (!tsdb.getConfig().hasProperty(TAGK_WHITESPACE_KEY)) {
      tsdb.getConfig().register(TAGK_WHITESPACE_KEY, false, false, 
          "Whether or not whitespace characters are allowed in tag key names.");
    }
    if (!tsdb.getConfig().hasProperty(TAGV_WHITESPACE_KEY)) {
      tsdb.getConfig().register(TAGV_WHITESPACE_KEY, true, false, 
          "Whether or not whitespace characters are allowed in tag value names.");
    }
    
    if (!tsdb.getConfig().hasProperty(METRIC_LENGTH_KEY)) {
      tsdb.getConfig().register(METRIC_LENGTH_KEY, 256, false, 
          "The maxmium length of metric names.");
    }
    if (!tsdb.getConfig().hasProperty(TAGK_LENGTH_KEY)) {
      tsdb.getConfig().register(TAGK_LENGTH_KEY, 256, false, 
          "The maxmium length of tag key names.");
    }
    if (!tsdb.getConfig().hasProperty(TAGV_LENGTH_KEY)) {
      tsdb.getConfig().register(TAGV_LENGTH_KEY, 1024, false, 
          "The maxmium length of tag value names.");
    }
    
    if (!tsdb.getConfig().hasProperty(METRIC_SPECIAL_KEY)) {
      tsdb.getConfig().register(METRIC_SPECIAL_KEY, null, false, 
          "A list of ASCII characters allowed in metric names when "
          + "running in ASCII mode.");
    }
    if (!tsdb.getConfig().hasProperty(TAGK_SPECIAL_KEY)) {
      tsdb.getConfig().register(TAGK_SPECIAL_KEY, null, false, 
          "A list of ASCII characters allowed in tag key names when "
          + "running in ASCII mode.");
    }
    if (!tsdb.getConfig().hasProperty(TAGV_SPECIAL_KEY)) {
      tsdb.getConfig().register(TAGV_SPECIAL_KEY, null, false, 
          "A list of ASCII characters allowed in tag value names when "
          + "running in ASCII mode.");
    }
    
    if (!tsdb.getConfig().hasProperty(REQUIRE_TAGS_KEY)) {
      tsdb.getConfig().register(REQUIRE_TAGS_KEY, true, false, 
          "Whether or not at least one pair of tags must be present in an ID.");
    }
    
  }
}

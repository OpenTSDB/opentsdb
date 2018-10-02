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
package net.opentsdb.query.filter;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

/**
 * A filter that expands numeric and pipe ranges. See
 * https://github.com/yahoo/range.
 * 
 * @since 3.0
 */
@JsonInclude(Include.NON_NULL)
@JsonDeserialize(builder = TagValueRangeFilter.Builder.class)
public class TagValueRangeFilter extends TagValueLiteralOrFilter {

  protected TagValueRangeFilter(Builder builder) {
    super(builder);
    literals.clear();
    literals.addAll(parseRangeExpression(filter));
  }

  @Override
  public String getType() {
    return TagValueRangeFilterFactory.TYPE;
  }
  
  /**
   * Parse librange like: fnd{01-06}.tmp.{dc1,dc2}.yahoo.com
   *  
   * @param filter
   * @return
   */
  static Set<String> parseRangeExpression(final String filter) {
    final Set<String> literal_rang_results = new HashSet<String>();
    
    boolean is_valid_range = true;
    String error_message = null;
    final Stack<Integer> bracket_stack = new Stack<Integer>();
    final List<Map.Entry<Integer, Integer>> bracket_indexs = 
        new ArrayList<Map.Entry<Integer, Integer>>();
    for (int i = 0; i < filter.length(); ++i) {
      char c = filter.charAt(i);
      if ('{' == c) {
        bracket_stack.push(i);
      } else if ('}' == c) {
        if (bracket_stack.isEmpty()) {
          // don't has a matched {
          is_valid_range = false;
          error_message = "no matched { found in the filter";
          throw new IllegalArgumentException(error_message);
        }
        
        if (i > bracket_stack.peek().intValue()) {
          bracket_indexs.add(
              new AbstractMap.SimpleEntry<Integer, Integer>(bracket_stack.pop(), i));
        }
      }
    } // end for
    
    if (!is_valid_range) {
      return literal_rang_results;
    }

    // iterate each range expression
    List<StringBuilder> pres_phase_members = new ArrayList<StringBuilder>();
    pres_phase_members.add(new StringBuilder());
    List<StringBuilder> next_phase_members = new ArrayList<StringBuilder>();

    int next_append_index = 0;
    for (Map.Entry<Integer, Integer> bracket : bracket_indexs) {
      if (bracket.getKey().intValue() > next_append_index) {
        for (StringBuilder member : pres_phase_members) {
          member.append(filter.substring(next_append_index, bracket.getKey().intValue()));
        }
      } // end if

      // move after the right bracket
      next_append_index = bracket.getValue().intValue() + 1;

      // parse the range between { and }
      String range_sec = filter.substring(bracket.getKey().intValue() + 1, 
          bracket.getValue().intValue());
      if (range_sec.contains("-")) {
        List<String> range_members = parseNumericRange(range_sec);
        if (null == range_members) {
          is_valid_range = false;
          error_message = "invalid numeric range : " + range_sec;
          break;
        }

        for (String range_member : range_members) {
          for (StringBuilder sb : pres_phase_members) {
            StringBuilder nsb = new StringBuilder();
            nsb.append(sb).append(range_member);
            next_phase_members.add(nsb);
          } // end for
        } // end for
      } else if (range_sec.contains(",")) {
        String[] range_tokens = range_sec.split(",");
        for (String range_member : range_tokens) {
          for (StringBuilder sb : pres_phase_members) {
            StringBuilder nsb = new StringBuilder();
            nsb.append(sb).append(range_member);
            next_phase_members.add(nsb);
          } // end for
        } // end for
      } else if (range_sec.isEmpty()) {
        // skip the {} in filter like aaa{}bbb
        for (StringBuilder sb : pres_phase_members) {
          next_phase_members.add(sb);
        }
      } else {
        is_valid_range = false;
        error_message = "invalid range without - or ,";
        break;
      }

      if (is_valid_range) {
        pres_phase_members = next_phase_members;
        next_phase_members = new ArrayList<StringBuilder>();
      }
    } // end for
    
    if (!is_valid_range) {
      return literal_rang_results;
    }
    
    for (StringBuilder sb : pres_phase_members) {
      StringBuilder nsb = new StringBuilder();
      if (filter.length() > next_append_index) {
        nsb.append(sb).append(filter.substring(next_append_index));
      } else {
        nsb.append(sb);
      }
      
      literal_rang_results.add(nsb.toString());
    } // end for
    
    return literal_rang_results;
  }
  
  static List<String> parseNumericRange(final String exp) {
    List<String> result = new ArrayList<String>();
    String[] tokens = exp.split("-");
    if (2 != tokens.length || 
        tokens[0].isEmpty() || 
        tokens[1].isEmpty()) {
      return null;
    }
    
    int prefix_part_one = lenghOfPrefixZero(tokens[0]);
    int prefix_part_two = lenghOfPrefixZero(tokens[1]);
    
    // if prefix with zero, then the length of these two parts must be the same
    if ((prefix_part_one > 0 || prefix_part_two > 0) 
        && (tokens[0].length() != tokens[1].length())) {
      return null;
    }
    
    try {
      int lower_bound = Integer.parseInt(tokens[0]);
      int upper_bound = Integer.parseInt(tokens[1]);
      for (int i = lower_bound; i <= upper_bound; ++i) {
        String number_str = String.valueOf(i);
        StringBuilder sb = new StringBuilder();
        
        if (prefix_part_one > 0 || prefix_part_two > 0) {
          // with prefix zero
          while (sb.length() + number_str.length() < tokens[0].length()) {
            sb.append('0');
          }
        }

        sb.append(number_str);
        result.add(sb.toString());
      } // end for
    } catch (NumberFormatException  e) {
      return null;
    }
    
    return result;
  }
  
  private static int lenghOfPrefixZero(final String str) {
    int i = 0;
    while (i < str.length() && str.charAt(i) == '0') {
      ++i;
    } // end while
    
    if ((i > 0 && i < str.length())) {
      // 01
      return i;
    } else if (i > 0 && i > 1) {
      // 000
      return i - 1;
    }
    
    return 0;
  }
  
  public static Builder newBuilder() {
    return new Builder();
  }
  
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Builder extends TagValueLiteralOrFilter.Builder {
    
    @Override
    public TagValueRangeFilter build() {
      return new TagValueRangeFilter(this);
    }
  }
}

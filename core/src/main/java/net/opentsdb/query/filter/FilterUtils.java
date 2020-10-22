// This file is part of OpenTSDB.
// Copyright (C) 2018-2020  The OpenTSDB Authors.
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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import net.opentsdb.data.TimeSeriesDatumStringId;

import java.util.Map;
import java.util.Map.Entry;

import java.util.Set;

/**
 * Utilities for working with filters.
 * 
 * @since 3.0
 */
public class FilterUtils {

  /**
   * Private ctor, statics only.
   */
  private FilterUtils() { }

  /**
   * Determines if the filter(s) are satisfied with the tag sets. This
   * utility will handle chained filters based on the AND or OR operand.
   * For non-tag handling filters, this method will return true.
   * @param filter The filter to evaluate.
   * @param tags The non-null (possibly empty) set of tags to evaluate.
   * @param matched A set of tag keys used to determine if we've satisfied
   * an explicit tags filter.
   * @return True if the filter(s) matched, false if not or true if the
   * filter(s) were not tag filters.
   */
  public static boolean matchesTags(final QueryFilter filter,
                                    final Map<String, String> tags,
                                    final Set<String> matched) {
    if (filter == null) {
      throw new IllegalArgumentException("Filter cannot be null.");
    }
    if (tags == null) {
      throw new IllegalArgumentException("Tags cannot be null.");
    }
    
    if (filter instanceof ExplicitTagsFilter) {
      Set<String> matched_tags = matched;
      if (matched == null) {
        matched_tags = Sets.newHashSet();
      }
      final boolean satisfied = matchesTags(
          ((ExplicitTagsFilter) filter).getFilter(), tags, matched_tags);
      
      if (!satisfied) {
        return false;
      }
      if (matched_tags.size() != tags.size()) {
        return false;
      }
      return true;
    } else if (filter instanceof TagValueFilter) {
      if (((TagValueFilter) filter).matches(tags)) {
        if (matched != null) {
          matched.add(((TagValueFilter) filter).getTagKey());
        }
        return true;
      }
      return false;
    } else if (filter instanceof TagKeyFilter) {
      if (((TagValueFilter) filter).matches(tags)) {
        if (matched != null) {
          matched.add(((TagKeyFilter) filter).filter());
        }
        return true;
      }
      return false;
    } else if (filter instanceof AnyFieldRegexFilter) {
      if (((AnyFieldRegexFilter) filter).matches(tags)) {
        if (matched != null) {
          matched.add(((AnyFieldRegexFilter) filter).getFilter());
        }
        return true;
      }
    } else if (filter instanceof ChainFilter) {
      final ChainFilter chain = (ChainFilter) filter;
      switch (chain.getOp()) {
        case AND:
          for (final QueryFilter child : chain.getFilters()) {
            if (!matchesTags(child, tags, matched)) {
              return false;
            }
          }
          return true;
        case OR:
          for (final QueryFilter child : chain.getFilters()) {
            if (matchesTags(child, tags, matched)) {
              return true;
            }
          }
          return false;
        default:
          throw new IllegalStateException("Unsupported chain operator: "
              + chain.getOp());
      }
    } else if (filter instanceof NotFilter) {
      return !matchesTags(((NotFilter) filter).getFilter(), tags, matched);
    }

    // it's a different type of filter so return true.
    return true;
  }
  
  /**
   * Rough utility to test meta queries against a mock data store for 
   * TAG_KEYS_AND_VALUES queries.
   * TODO - other filter types and explicit tags along with testing this thing.
   * @param filter The non-null filter.
   * @param id The non-null ID to test against.
   * @param namespace An optional namespace
   * @param matched A non-null map of tags matched
   * @return True if matched, false if not.
   */
  public static boolean matchesTags(final QueryFilter filter,
                                    final TimeSeriesDatumStringId id,
                                    final String namespace,
                                    final Map<String, String> matched) {
    if (filter == null) {
      throw new IllegalArgumentException("Filter cannot be null.");
    }
    if (id == null) {
      throw new IllegalArgumentException("ID cannot be null.");
    }

    if (filter instanceof ExplicitTagsFilter) {
      final boolean satisfied = matchesTags(
          ((ExplicitTagsFilter) filter).getFilter(), id, namespace, matched);

      if (!satisfied) {
        return false;
      }
      if (matched.size() != id.tags().size()) {
        return false;
      }
      return true;
    } else if (filter instanceof TagValueFilter) {
      Map<String, String> single_map = Maps.newHashMapWithExpectedSize(1);
      boolean found = false;
      for (final Entry<String, String> entry : id.tags().entrySet()) {
        single_map.put(entry.getKey(), entry.getValue());
        if (((TagValueFilter) filter).matches(single_map)) {
          found = true;
          if (matched != null) {
            matched.put(entry.getKey(), entry.getValue());
          }
        }
        single_map.clear();
      }
      return found;
    } else if (filter instanceof TagKeyFilter) {
      Map<String, String> single_map = Maps.newHashMapWithExpectedSize(1);
      boolean found = false;
      for (final Entry<String, String> entry : id.tags().entrySet()) {
        single_map.put(entry.getKey(), entry.getValue());
        if (((TagKeyFilter) filter).matches(single_map)) {
          found = true;
          if (matched != null) {
            matched.put(entry.getKey(), entry.getValue());
          }
        }
        single_map.clear();
      }
      return found;
    } else if (filter instanceof AnyFieldRegexFilter) {
      boolean found = false;
      for (final Entry<String, String> entry : id.tags().entrySet()) {
        if (((AnyFieldRegexFilter) filter).matches(entry.getKey()) ||
          ((AnyFieldRegexFilter) filter).matches(entry.getValue())) {
          found = true;
          if (matched != null) {
            matched.put(entry.getKey(), entry.getValue());
          }
        }
      }
      return found;
    } else if (filter instanceof ChainFilter) {
      final ChainFilter chain = (ChainFilter) filter;
      switch (chain.getOp()) {
        case AND:
          for (final QueryFilter child : chain.getFilters()) {
            if (!matchesTags(child, id, namespace, matched)) {
              return false;
            }
          }
          return true;
        case OR:
          for (final QueryFilter child : chain.getFilters()) {
            if (matchesTags(child, id, namespace, matched)) {
              return true;
            }
          }
          return false;
        default:
          throw new IllegalStateException("Unsupported chain operator: "
              + chain.getOp());
      }
    } else if (filter instanceof MetricFilter) {
      if (!((MetricFilter) filter).matches(id.metric())) {
        if (id.metric().startsWith(namespace)) {
          return ((MetricFilter) filter).matches(id.metric().replace(namespace + ".", ""));
        }
        return false;
      }
      return true;
    } else if (filter instanceof NotFilter) {
      // TODO - properly handle NOT NOT filters
      return !matchesTags(((NotFilter) filter).getFilter(), id, namespace, matched);
    }
    return false;
  }

  /**
   * Rough utility to test meta queries against a mock data store for 
   * METRICS queries.
   * @param filter The non-null filter.
   * @param id The non-null ID to test against.
   * @param namespace An optional namespace
   * @return True if matched, false if not.
   */
  public static boolean matchesMetrics(final QueryFilter filter,
                                       final TimeSeriesDatumStringId id,
                                       final String namespace) {
    if (filter instanceof ChainFilter) {
      final ChainFilter chain = (ChainFilter) filter;
      switch (chain.getOp()) {
        case AND:
          for (final QueryFilter child : chain.getFilters()) {
            if (!matchesMetrics(child, id, namespace)) {
              return false;
            }
          }
          return true;
        case OR:
          for (final QueryFilter child : chain.getFilters()) {
            if (matchesMetrics(child, id, namespace)) {
              return true;
            }
          }
          return false;
        default:
          throw new IllegalStateException("Unsupported chain operator: "
              + chain.getOp());
      }
    } else if (filter instanceof MetricFilter) {
      if (!((MetricFilter) filter).matches(id.metric())) {
        if (id.metric().startsWith(namespace)) {
          return ((MetricFilter) filter).matches(id.metric().replace(namespace + ".", ""));
        }
        return false;
      }
      return true;
    } else if (filter instanceof AnyFieldRegexFilter) {
      return ((AnyFieldRegexFilter) filter).matches(id.metric());
    } else if (filter instanceof TagKeyFilter) {
      return ((TagKeyFilter) filter).matches(id.tags());
    } else if (filter instanceof TagValueFilter) {
      return ((TagValueFilter) filter).matches(id.tags());
    } else if (filter instanceof NotFilter) {
      return !(matchesMetrics(((NotFilter) filter).getFilter(), id, namespace));
    }
    return false;
  }
  
  /**
   * 
   * Rough utility to test meta queries against a mock data store for 
   * TAG_KEYS or TAG_VALUES queries.
   * @param filter The non-null filter.
   * @param id The non-null ID to test against.
   * @param namespace An optional namespace
   * @param matched The non-null set of keys or values to populate.
   * @param match_key Whether we match on keys or values.
   * @return True if matched, false if not.
   */
  public static boolean matchesTagKeysOrValues(final QueryFilter filter,
                                               final TimeSeriesDatumStringId id,
                                               final String namespace,
                                               final Set<String> matched,
                                               final boolean match_key) {
    if (filter instanceof ChainFilter) {
      final ChainFilter chain = (ChainFilter) filter;
      switch (chain.getOp()) {
        case AND:
          Set<String> sub_set = Sets.newHashSet();
          for (final QueryFilter child : chain.getFilters()) {
            if (!matchesTagKeysOrValues(child, id, namespace, sub_set, match_key)) {
              return false;
            }
          }
          matched.addAll(sub_set);
          return true;
        case OR:
          for (final QueryFilter child : chain.getFilters()) {
            if (matchesTagKeysOrValues(child, id, namespace, matched, match_key)) {
              return true;
            }
          }
          return false;
        default:
          throw new IllegalStateException("Unsupported chain operator: "
              + chain.getOp());
      }
    } else if (filter instanceof TagKeyFilter) {
      Map<String, String> single_map = Maps.newHashMapWithExpectedSize(1);
      boolean found = false;
      for (final Entry<String, String> entry : id.tags().entrySet()) {
        single_map.put(entry.getKey(), entry.getValue());
        if (((TagKeyFilter) filter).matches(single_map)) {
          found = true;
          if (matched != null) {
            if (match_key) {
              matched.add(entry.getKey());
            } else {
              // yeah, doesn't make sense but....
              matched.add(entry.getValue());
            }
          }
        }
        single_map.clear();
      }
      return found;
    } else if (filter instanceof TagValueFilter) {
      Map<String, String> single_map = Maps.newHashMapWithExpectedSize(1);
      boolean found = false;
      for (final Entry<String, String> entry : id.tags().entrySet()) {
        single_map.put(entry.getKey(), entry.getValue());
        if (((TagValueFilter) filter).matches(single_map)) {
          found = true;
          if (matched != null) {
            if (match_key) {
              // yeah, may not match.
              matched.add(entry.getKey());
            } else {
              matched.add(entry.getValue());
            }
          }
        }
        single_map.clear();
      }
      return found;
    } else if (filter instanceof AnyFieldRegexFilter) {
      boolean found = false;
      for (final Entry<String, String> entry : id.tags().entrySet()) {
        if (match_key ? ((AnyFieldRegexFilter) filter).matches(entry.getKey()) :
          ((AnyFieldRegexFilter) filter).matches(entry.getValue())) {
          found = true;
          if (matched != null) {
            if (match_key) {
              matched.add(entry.getKey());
            } else {
              // yeah, doesn't make sense but....
              matched.add(entry.getValue());
            }
          }
        }
      }
      return found;
    } else if (filter instanceof MetricFilter) {
      if (!((MetricFilter) filter).matches(id.metric())) {
        if (id.metric().startsWith(namespace)) {
          return ((MetricFilter) filter).matches(id.metric().replace(namespace + ".", ""));
        }
        return false;
      }
      return true;
    } else if (filter instanceof NotFilter) {
      // TODO - properly handle Not Not filters.
      return !matchesTagKeysOrValues(((NotFilter) filter).getFilter(), 
          id, namespace, matched, match_key);
    }
    return false;
  }
  
  /**
   * Rough utility to test meta queries against a mock data store for 
   * TIMESERIES queries.
   * TODO - explicit tags.
   * @param filterThe non-null filter.
   * @param id The non-null ID to test against.
   * @param namespace An optional namespace
   * @return True if matched, false if not.
   */
  public static boolean matches(final QueryFilter filter, 
                                final TimeSeriesDatumStringId id,
                                final String namespace) {
    if (filter instanceof ChainFilter) {
      final ChainFilter chain = (ChainFilter) filter;
      switch (chain.getOp()) {
        case AND:
          for (final QueryFilter child : chain.getFilters()) {
            if (!matches(child, id, namespace)) {
              return false;
            }
          }
          return true;
        case OR:
          for (final QueryFilter child : chain.getFilters()) {
            if (matches(child, id, namespace)) {
              return true;
            }
          }
          return false;
        default:
          throw new IllegalStateException("Unsupported chain operator: "
              + chain.getOp());
      }
    } else if (filter instanceof MetricFilter) {
      if (!((MetricFilter) filter).matches(id.metric())) {
        if (id.metric().startsWith(namespace)) {
          return ((MetricFilter) filter).matches(id.metric().replace(namespace + ".", ""));
        }
        return false;
      }
      return true;
    } else if (filter instanceof TagValueFilter) {
        return ((TagValueFilter) filter).matches(id.tags());
    } else if (filter instanceof TagKeyFilter) {
      return ((TagValueFilter) filter).matches(id.tags());
    } else if (filter instanceof AnyFieldRegexFilter) {
      if (((AnyFieldRegexFilter) filter).matches(id.metric())) {
        return true;
      }
      return ((AnyFieldRegexFilter) filter).matches(id.tags());
    } else if (filter instanceof NotFilter) {
      return !(matches(((NotFilter) filter).getFilter(), id, namespace));
    }
    return false;
  }
  
  /**
   * Walks the filter recursively to figure out which tag keys would be of
   * use in the query, particularly for determining pre-aggregates. If a key
   * is not'ted then we'll omit it.
   * @param filter A non-null filter to start with.
   * @return Null if the filter did not have any useful tag key filters, a 
   * non-empty set if one or more tag keys were found.
   */
  public static Set<String> desiredTagKeys(final QueryFilter filter) {
    return desiredTagKeys(filter, false);
  }
  
  private static Set<String> desiredTagKeys(final QueryFilter filter, 
                                            final boolean not) {
    if (filter instanceof NestedQueryFilter) {
      if (filter instanceof NotFilter) {
        return desiredTagKeys(((NestedQueryFilter) filter).getFilter(), true);
      } else {
        return desiredTagKeys(((NestedQueryFilter) filter).getFilter(), not);
      }
    } else if (filter instanceof ChainFilter) {
      Set<String> tags = null;
      for (final QueryFilter sub_filter : ((ChainFilter) filter).getFilters()) {
        final Set<String> sub_tags = desiredTagKeys(sub_filter, not);
        if (sub_tags != null) {
          if (tags == null) {
            tags = Sets.newHashSet();
          }
          tags.addAll(sub_tags);
        }
      }
      return tags;
    } else if (filter instanceof TagKeyLiteralOrFilter && !not) {
      return Sets.newHashSet(((TagKeyLiteralOrFilter) filter).literals());
    } else if (filter instanceof TagValueRegexFilter) {
      if (not && ((TagValueRegexFilter) filter).matchesAll()) {
        // same as if we had a NOT(TagKeyFilter).
        return null;
      }
      return Sets.newHashSet(((TagValueFilter) filter).getTagKey());
    } else if (filter instanceof TagValueWildcardFilter) {
      if (not && ((TagValueWildcardFilter) filter).matchesAll()) {
        // same as if we had a NOT(TagKeyFilter).
        return null;
      }
      return Sets.newHashSet(((TagValueFilter) filter).getTagKey());
    } else if (filter instanceof TagValueFilter) {
      // we always need to put these in as we don't know if the user specified
      // all tags to be "not"ted.
      return Sets.newHashSet(((TagValueFilter) filter).getTagKey());
    } 
    return null;
  }
}

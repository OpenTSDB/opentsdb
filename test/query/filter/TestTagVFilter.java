// This file is part of OpenTSDB.
// Copyright (C) 2015  The OpenTSDB Authors.
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
package net.opentsdb.query.filter;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.opentsdb.core.BaseTsdbTest;
import net.opentsdb.core.TSDB;
import net.opentsdb.uid.NoSuchUniqueName;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import com.stumbleupon.async.DeferredGroupException;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@PrepareForTest({ TSDB.class })
public class TestTagVFilter extends BaseTsdbTest {

  @Test (expected = IllegalArgumentException.class)
  public void getFilterNullTagk() throws Exception {
    TagVFilter.getFilter(null, "myflter");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getFilterEmptyTagk() throws Exception {
    TagVFilter.getFilter(null, "myflter");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getFilterEmptyFilter() throws Exception {
    TagVFilter.getFilter(TAGK_STRING, "");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getFilterNullFilter() throws Exception {
    TagVFilter.getFilter(TAGK_STRING, null);
  }
  
  @Test
  public void getFilterGroupBy() throws Exception {
    assertNull(TagVFilter.getFilter(TAGK_STRING, "*"));
  }
  
  @Test
  public void getFilterLiteral() throws Exception {
    assertNull(TagVFilter.getFilter(TAGK_STRING, TAGV_STRING));
  }
  
  @Test
  public void getFilterGroupByPiped() throws Exception {
    assertNull(TagVFilter.getFilter(TAGK_STRING, "web01|web02"));
  }
  
  @Test
  public void getFilterWildcard() throws Exception {
    final TagVFilter filter = TagVFilter.getFilter(TAGK_STRING, 
        TagVWildcardFilter.FILTER_NAME + "(*bonk.com)");
    assertEquals(TAGK_STRING, filter.getTagk());
    assertTrue(filter instanceof TagVWildcardFilter);
    assertFalse(((TagVWildcardFilter)filter).isCaseInsensitive());
  }

  @Test
  public void getFilterWildcardInsensitive() throws Exception {
    final TagVFilter filter = TagVFilter.getFilter(TAGK_STRING, 
        TagVWildcardFilter.TagVIWildcardFilter.FILTER_NAME + "(*bonk.com)");
    assertEquals(TAGK_STRING, filter.getTagk());
    assertTrue(filter instanceof TagVWildcardFilter);
    assertTrue(((TagVWildcardFilter)filter).isCaseInsensitive());
  }
  
  @Test
  public void getFilterWildcardFatfinger() throws Exception {
    // falls through to the shortcut
    final TagVFilter filter = TagVFilter.getFilter(TAGK_STRING,
        "wil@*sugarbean");
    assertEquals(TAGK_STRING, filter.getTagk());
    assertTrue(filter instanceof TagVWildcardFilter);
    assertTrue(((TagVWildcardFilter)filter).isCaseInsensitive());
  }
  
  @Test
  public void getFilterWildcardImplicit() throws Exception {
    final TagVFilter filter = TagVFilter.getFilter(TAGK_STRING, "*bonk.com");
    assertEquals(TAGK_STRING, filter.getTagk());
    assertTrue(filter instanceof TagVWildcardFilter);
    assertTrue(((TagVWildcardFilter)filter).isCaseInsensitive());
  }
  
  @Test
  public void getFilterPipe() throws Exception {
    final TagVFilter filter = TagVFilter.getFilter(TAGK_STRING, 
        TagVLiteralOrFilter.FILTER_NAME + "(quirm|bonk)");
    assertEquals(TAGK_STRING, filter.getTagk());
    assertTrue(filter instanceof TagVLiteralOrFilter);
    assertFalse(((TagVLiteralOrFilter)filter).isCaseInsensitive());
  }
  
  @Test
  public void getFilterPipeInsensitive() throws Exception {
    final TagVFilter filter = TagVFilter.getFilter(TAGK_STRING, 
        TagVLiteralOrFilter.TagVILiteralOrFilter.FILTER_NAME + "(quirm|bonk)");
    assertEquals(TAGK_STRING, filter.getTagk());
    assertTrue(filter instanceof TagVLiteralOrFilter);
    assertTrue(((TagVLiteralOrFilter)filter).isCaseInsensitive());
  }
  
  @Test
  public void getFilterPipeFatfinger() throws Exception {
    assertNull(TagVFilter.getFilter(TAGK_STRING, "lite@sugarbean|granny"));
  }
  
  @Test
  public void getFilterRegex() throws Exception {
    final TagVFilter filter = TagVFilter.getFilter(TAGK_STRING, 
        TagVRegexFilter.FILTER_NAME + "(.*sugarbean)");
    assertEquals(TAGK_STRING, filter.getTagk());
    assertTrue(filter instanceof TagVRegexFilter);
  }
  
  @Test
  public void getFilterRegexFatFinger() throws Exception {
    // falls through to the implicity
    final TagVFilter filter = TagVFilter.getFilter(TAGK_STRING, "rexp@.*sugarbean");
    assertEquals(TAGK_STRING, filter.getTagk());
    assertTrue(filter instanceof TagVWildcardFilter);
    assertTrue(((TagVWildcardFilter)filter).isCaseInsensitive());
  }
  
  @Test
  public void getFilterRegexCase() throws Exception {
    final TagVFilter filter = TagVFilter.getFilter(TAGK_STRING, 
        TagVRegexFilter.FILTER_NAME.toUpperCase() + "(.*sugarbean)");
    assertEquals(TAGK_STRING, filter.getTagk());
    assertTrue(filter instanceof TagVRegexFilter);
  }

  @Test (expected = IllegalArgumentException.class)
  public void getFilterMissingClosingParens() throws Exception {
    TagVFilter.getFilter(TAGK_STRING, TagVRegexFilter.FILTER_NAME + "(.*sugarbean");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getFilterEmptyParens() throws Exception {
    TagVFilter.getFilter(TAGK_STRING, TagVRegexFilter.FILTER_NAME + "()");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getFilterUnknownType() throws Exception {
    TagVFilter.getFilter(TAGK_STRING, "dummyfilter(nothere)");
  }
  
  @Test
  public void resolveName() throws Exception {
    final TagVFilter filter = new TagVWildcardFilter(TAGK_STRING, "*omnia");
    filter.resolveTagkName(tsdb).join();
    assertArrayEquals(TAGK_BYTES, filter.getTagkBytes());
    assertTrue(filter.getTagVUids().isEmpty());
  }
  
  @Test
  public void resolveNameLiteral() throws Exception {
    final TagVFilter filter = new TagVLiteralOrFilter(TAGK_STRING, TAGV_STRING);
    filter.resolveTagkName(tsdb).join();
    assertArrayEquals(TAGK_BYTES, filter.getTagkBytes());
    assertEquals(1, filter.getTagVUids().size());    
    assertArrayEquals(TAGV_BYTES, filter.getTagVUids().get(0));
  }
  
  @Test
  public void resolveNameLiterals() throws Exception {
    final TagVFilter filter = new TagVLiteralOrFilter(TAGK_STRING, "web01|web02");
    filter.resolveTagkName(tsdb).join();
    assertArrayEquals(TAGK_BYTES, filter.getTagkBytes());
    assertEquals(2, filter.getTagVUids().size());    
    assertArrayEquals(TAGV_BYTES, filter.getTagVUids().get(0));
    assertArrayEquals(TAGV_B_BYTES, filter.getTagVUids().get(1));
  }
  
  @Test (expected = DeferredGroupException.class)
  public void resolveNameLiteralsNSUNTagV() throws Exception {
    final TagVFilter filter = new TagVLiteralOrFilter(TAGK_STRING, "web01|web03");
    filter.resolveTagkName(tsdb).join();
  }
  
  @Test
  public void resolveNameLiteralsNSUNTagvSkipped() throws Exception {
    config.overrideConfig("tsd.query.skip_unresolved_tagvs", "true");
    final TagVFilter filter = new TagVLiteralOrFilter(TAGK_STRING, "web01|web03");
    filter.resolveTagkName(tsdb).join();
    assertArrayEquals(TAGK_BYTES, filter.getTagkBytes());
    assertEquals(1, filter.getTagVUids().size());    
    assertArrayEquals(TAGV_BYTES, filter.getTagVUids().get(0));
  }
  
  @Test
  public void resolveNameLiteralsTooMany() throws Exception {
    config.overrideConfig("tsd.query.filter.expansion_limit", "1");
    final TagVFilter filter = new TagVLiteralOrFilter(TAGK_STRING, "web01|web02");
    filter.resolveTagkName(tsdb).join();
    assertArrayEquals(TAGK_BYTES, filter.getTagkBytes());
    assertTrue(filter.getTagVUids().isEmpty());    
  }
  
  @Test
  public void resolveNameLiteralsCaseInsensitive() throws Exception {
    final TagVFilter filter = new TagVLiteralOrFilter(TAGK_STRING, "web01|web02", 
        true);
    filter.resolveTagkName(tsdb).join();
    assertArrayEquals(TAGK_BYTES, filter.getTagkBytes());
    assertTrue(filter.getTagVUids().isEmpty());    
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void resolveNameNSUN() throws Exception {
    final TagVFilter filter = new TagVWildcardFilter(NSUN_TAGK, "*omnia");
    filter.resolveTagkName(tsdb).join();
  }
  
  @Test (expected = NullPointerException.class)
  public void resolveNameNullTSDB() throws Exception {
    new TagVWildcardFilter("host", "*omnia").resolveTagkName(null);
  }
  
  @Test
  public void comparableTest() throws Exception {
    final TagVFilter filter_a = new TagVWildcardFilter("host", "*omnia");
    Whitebox.setInternalState(filter_a, "tagk_bytes", new byte[] { 0, 0, 0, 1 });
    final TagVFilter filter_b = new TagVRegexFilter("dc", ".*katch");
    Whitebox.setInternalState(filter_b, "tagk_bytes", new byte[] { 0, 0, 0, 2 });
    
    assertEquals(0, filter_a.compareTo(filter_a));
    assertEquals(-1, filter_a.compareTo(filter_b));
    assertEquals(1, filter_b.compareTo(filter_a));
    
    Whitebox.setInternalState(filter_a, "tagk_bytes", (byte[])null);
    assertEquals(0, filter_a.compareTo(filter_a));
    assertEquals(-1, filter_a.compareTo(filter_b));
    assertEquals(1, filter_b.compareTo(filter_a));
    
    Whitebox.setInternalState(filter_b, "tagk_bytes", (byte[])null);
    assertEquals(0, filter_a.compareTo(filter_a));
    assertEquals(0, filter_a.compareTo(filter_b));
    assertEquals(0, filter_b.compareTo(filter_a));
    
  }

  @Test
  public void stripParentheses() throws Exception {
    assertEquals(".*sugarbean", TagVFilter.stripParentheses(
        TagVRegexFilter.FILTER_NAME + "(.*sugarbean)"));
  }
  
  @Test
  public void stripParenthesesEmptyParentheses() throws Exception {
    // let the filter's ctor handle this case
    assertEquals("", TagVFilter.stripParentheses(
        TagVRegexFilter.FILTER_NAME + "()"));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void stripParenthesesMissingClosing() throws Exception {
    TagVFilter.stripParentheses(TagVRegexFilter.FILTER_NAME + "(.*sugarbean");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void stripParenthesesMissingOpening() throws Exception {
    TagVFilter.stripParentheses("regexp.*sugarbean)");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void stripParenthesesNull() throws Exception {
    TagVFilter.stripParentheses(null);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void stripParenthesesEmpty() throws Exception {
    TagVFilter.stripParentheses("");
  }

  @SuppressWarnings("unchecked")
  @Test
  public void tagsToFiltersOldGroupBy() throws Exception {
    final Map<String, String> tags = new HashMap<String, String>(3);
    tags.put("host", "quirm");  // literal
    tags.put("owner", "vimes|vetinary"); // pipe
    tags.put("colo", "*"); // group by all
    final List<TagVFilter> filters = new ArrayList<TagVFilter>(3);
    TagVFilter.tagsToFilters(tags, filters);

    assertEquals(3, filters.size());
    for (final TagVFilter filter : filters) {
      if (filter.getTagk().equals("host")) {
        assertTrue(filter instanceof TagVLiteralOrFilter);
        assertFalse(((TagVLiteralOrFilter)filter).isCaseInsensitive());
        assertEquals(1, ((Set<String>)Whitebox
            .getInternalState(filter, "literals")).size());
      } else if (filter.getTagk().equals("owner")) {
        assertTrue(filter instanceof TagVLiteralOrFilter);
        assertFalse(((TagVLiteralOrFilter)filter).isCaseInsensitive());
        assertEquals(2, ((Set<String>)Whitebox
            .getInternalState(filter, "literals")).size());
      } else if (filter.getTagk().equals("colo")) {
        assertTrue(filter instanceof TagVWildcardFilter);
        assertTrue(((TagVWildcardFilter)filter).isCaseInsensitive());
      } else {
        fail("Unexpected filter type: " + filter);
      }
      assertTrue(filter.isGroupBy());
    }
  }
  
  @Test
  public void tagsToFiltersNewFunctions() throws Exception {
    final Map<String, String> tags = new HashMap<String, String>(4);
    tags.put("host", "*beybi");
    tags.put("owner", "wildcard(*snapcase*)");
    tags.put("colo", "regexp(.*opolis)");
    tags.put("geo", "literal_or(tsort|chalk)");
    final List<TagVFilter> filters = new ArrayList<TagVFilter>(3);
    TagVFilter.tagsToFilters(tags, filters);
    
    assertEquals(4, filters.size());
    for (final TagVFilter filter : filters) {
      if (filter.getTagk().equals("host")) {
        assertTrue(filter instanceof TagVWildcardFilter);
        assertTrue(((TagVWildcardFilter)filter).isCaseInsensitive());
      } else if (filter.getTagk().equals("owner")) {
        assertTrue(filter instanceof TagVWildcardFilter);
        assertFalse(((TagVWildcardFilter)filter).isCaseInsensitive());
      } else if (filter.getTagk().equals("colo")) {
        assertTrue(filter instanceof TagVRegexFilter);
      } else if (filter.getTagk().equals("geo")) {
        assertTrue(filter instanceof TagVLiteralOrFilter);
        assertFalse(((TagVLiteralOrFilter)filter).isCaseInsensitive());
      } else {
        fail("Unexpected filter type: " + filter);
      }
      assertTrue(filter.isGroupBy());
    }
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void tagsToFiltersNoSuchFunction() throws Exception {
    final Map<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "doesnotexist(*beybi)");
    final List<TagVFilter> filters = new ArrayList<TagVFilter>(1);
    TagVFilter.tagsToFilters(tags, filters);
  }
  
  @Test
  public void tagsToFiltersDuplicate() throws Exception {
    final Map<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "*beybi");
    final List<TagVFilter> filters = new ArrayList<TagVFilter>(1);
    filters.add(new TagVWildcardFilter("host", "*beybi", true));
    assertFalse(filters.get(0).isGroupBy());
    TagVFilter.tagsToFilters(tags, filters);
    assertEquals(1, filters.size());
    assertTrue(filters.get(0).isGroupBy());
  }
  
  @Test
  public void tagsToFiltersSameTagDiffValues() throws Exception {
    final Map<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "*beybi");
    final List<TagVFilter> filters = new ArrayList<TagVFilter>(1);
    filters.add(new TagVWildcardFilter("host", "*helit", true));
    assertFalse(filters.get(0).isGroupBy());
    TagVFilter.tagsToFilters(tags, filters);
    assertEquals(2, filters.size());
  }
  
  // TODO - test the plugin loader similar to the other plugins
}

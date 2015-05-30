package net.opentsdb.query.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.PatternSyntaxException;

import org.junit.Before;
import org.junit.Test;

public class TestTagVRegexFilter {
  private static final String TAGK = "host";
  private Map<String, String> tags;
  
  @Before
  public void before() throws Exception {
    tags = new HashMap<String, String>(1);
    tags.put(TAGK, "ogg-01.ops.ankh.morpork.com");
  }
  
  @Test
  public void matchExact() throws Exception {
    TagVFilter filter = new TagVRegexFilter(TAGK, "ogg-01.ops.ankh.morpork.com");
    assertTrue(filter.match(tags));
  }
  
  @Test
  public void matchPostfix() throws Exception {
    TagVFilter filter = new TagVRegexFilter(TAGK, ".*.ops.ankh.morpork.com");
    assertTrue(filter.match(tags));
  }
  
  @Test
  public void matchPrefix() throws Exception {
    TagVFilter filter = new TagVRegexFilter(TAGK, "ogg-01.ops.ankh.*");
    assertTrue(filter.match(tags));
  }
  
  @Test
  public void matchAnything() throws Exception {
    TagVFilter filter = new TagVRegexFilter(TAGK, ".*");
    assertTrue(filter.match(tags));
  }
  
  @Test
  public void matchFailed() throws Exception {
    TagVFilter filter = new TagVRegexFilter(TAGK, "ogg-01.ops.qurim.*");
    assertFalse(filter.match(tags));
  }
  
  @Test
  public void matchGrouping() throws Exception {
    TagVFilter filter = new TagVRegexFilter(TAGK, 
        "ogg-01.ops.(ankh|quirm|tsort).morpork.com");
    assertTrue(filter.match(tags));
  }
  
  @Test
  public void matchNumbers() throws Exception {
    TagVFilter filter = new TagVRegexFilter(TAGK, 
        "ogg-\\d+.ops.ankh.morpork.com");
    assertTrue(filter.match(tags));
  }
  
  @Test
  public void matchNotEnoughNumbers() throws Exception {
    TagVFilter filter = new TagVRegexFilter(TAGK, 
        "ogg-\\d(3).ops.ankh.morpork.com");
    assertFalse(filter.match(tags));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorNullTagk() throws Exception {
    new TagVRegexFilter(null, "ogg-01.ops.qurim.*");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorEmptyTagk() throws Exception {
    new TagVRegexFilter("", "ogg-01.ops.qurim.*");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorNullFilter() throws Exception {
    new TagVRegexFilter(TAGK, null);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorEmptyFilter() throws Exception {
    new TagVRegexFilter(TAGK, "");
  }
  
  @Test (expected = PatternSyntaxException.class)
  public void ctorBadRegex() throws Exception {
    new TagVRegexFilter(TAGK, "ogg-\\d(3.ops.ankh.morpork.com");
  }

  @Test
  public void toStringTest() throws Exception {
    TagVFilter filter = new TagVRegexFilter(TAGK, 
        "ogg-\\d+.ops.ankh.morpork.com");
    assertTrue(filter.toString().contains("regex"));
  }
  
  @Test
  public void hashCodeAndEqualsTest() throws Exception {
    TagVFilter filter_a = new TagVRegexFilter(TAGK, 
        "ogg-\\d+.ops.ankh.morpork.com");
    TagVFilter filter_b = new TagVRegexFilter(TAGK, 
        "ogg-\\d+.ops.ankh.morpork.com");
    TagVFilter filter_c = new TagVRegexFilter(TAGK, 
        "ogg-\\d.ops.ankh.morpork.com");
    TagVFilter filter_d = new TagVRegexFilter(TAGK, 
        "ogg-\\d+.ops.ankh.morpork.co");
    
    assertEquals(filter_a.hashCode(), filter_b.hashCode());
    assertFalse(filter_a.hashCode() == filter_c.hashCode());
    assertFalse(filter_a.hashCode() == filter_d.hashCode());
    
    assertEquals(filter_a, filter_b);
    assertFalse(filter_a.equals(filter_c));
    assertFalse(filter_a.equals(filter_d));
  }
}

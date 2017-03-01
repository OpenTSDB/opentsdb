package net.opentsdb.query.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

public class TestTagVLiteralOrFilter {
  private static final String TAGK = "host";
  private Map<String, String> tags;
  
  @Before
  public void before() throws Exception {
    tags = new HashMap<String, String>(1);
    tags.put(TAGK, "CMTDibbler");
  }
  
  @Test
  public void matchMiddle() throws Exception {
    TagVFilter filter = new TagVLiteralOrFilter(TAGK, 
        "LutZe|CMTDibbler|Slant");
    assertTrue(filter.match(tags).join());
    assertFalse(((TagVLiteralOrFilter)filter).isCaseInsensitive());
  }
  
  @Test
  public void matchStart() throws Exception {
    TagVFilter filter = new TagVLiteralOrFilter(TAGK, 
        "CMTDibbler|LutZe|Slant");
    assertTrue(filter.match(tags).join());
  }
  
  @Test
  public void matchEnd() throws Exception {
    TagVFilter filter = new TagVLiteralOrFilter(TAGK, 
        "LutZe|Slant|CMTDibbler");
    assertTrue(filter.match(tags).join());
  }
  
  @Test
  public void matchNoPipes() throws Exception {
    TagVFilter filter = new TagVLiteralOrFilter(TAGK, 
        "CMTDibbler");
    assertTrue(filter.match(tags).join());
  }
  
  @Test
  public void matchPipeNoValueAfter() throws Exception {
    TagVFilter filter = new TagVLiteralOrFilter(TAGK, 
        "CMTDibbler|");
    assertTrue(filter.match(tags).join());
  }
  
  @Test
  public void matchPipeNoValueBefore() throws Exception {
    TagVFilter filter = new TagVLiteralOrFilter(TAGK, 
        "|CMTDibbler");
    assertTrue(filter.match(tags).join());
  }
  
  @Test
  public void matchFail() throws Exception {
    TagVFilter filter = new TagVLiteralOrFilter(TAGK, 
        "LutZe|Keli|Slant");
    assertFalse(filter.match(tags).join());
  }
  
  @Test
  public void matchFailCase() throws Exception {
    TagVFilter filter = new TagVLiteralOrFilter(TAGK, 
        "LutZe|CMtDibbler|Slant");
    assertFalse(filter.match(tags).join());
  }
  
  @Test
  public void matchCaseInsensitive() throws Exception {
    TagVFilter filter = new TagVLiteralOrFilter(TAGK, 
        "LutZe|CMtDibbler|Slant", true);
    assertTrue(filter.match(tags).join());
    assertTrue(((TagVLiteralOrFilter)filter).isCaseInsensitive());
  }
  
  @Test
  public void matchCaseInsensitiveValue() throws Exception {
    tags.put(TAGK, "CMTDIBBLER");
    TagVFilter filter = new TagVLiteralOrFilter(TAGK, 
        "LutZe|CMtDibbler|Slant", true);
    assertTrue(filter.match(tags).join());
    assertTrue(((TagVLiteralOrFilter)filter).isCaseInsensitive());
  }
  
  @Test
  public void matchCaseInsensitiveFail() throws Exception {
    TagVFilter filter = new TagVLiteralOrFilter(TAGK, 
        "LutZe|CMtDibble|Slant", true);
    assertFalse(filter.match(tags).join());
    assertTrue(((TagVLiteralOrFilter)filter).isCaseInsensitive());
  }
  
  @Test
  public void matchNoSuchTagk() throws Exception {
    TagVFilter filter = new TagVLiteralOrFilter(TAGK, "LutZe|Keli|Slant");
    tags.clear();
    tags.put("colo", "lga");
    assertFalse(filter.match(tags).join());
  }
  
  @Test
  public void matchNoSuchTagkCaseInsensitive() throws Exception {
    TagVFilter filter = new TagVLiteralOrFilter(TAGK, "LutZe|Keli|Slant", true);
    tags.clear();
    tags.put("colo", "lga");
    assertFalse(filter.match(tags).join());
  }
  
  @Test
  public void matchSingle() throws Exception {
    TagVFilter filter = new TagVLiteralOrFilter(TAGK, "CMTDibbler");
    assertTrue(filter.match(tags).join());
    assertFalse(((TagVLiteralOrFilter)filter).isCaseInsensitive());
  }
  
  @Test
  public void matchSingleCaseInsensitive() throws Exception {
    TagVFilter filter = new TagVLiteralOrFilter(TAGK, "cmtDibbler", true);
    assertTrue(filter.match(tags).join());
    assertTrue(((TagVLiteralOrFilter)filter).isCaseInsensitive());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorNullTagk() throws Exception {
    new TagVLiteralOrFilter(null, "LutZe|Keli|Slant");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorEmptyTagk() throws Exception {
    new TagVLiteralOrFilter("", "LutZe|Keli|Slant");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorNullFilter() throws Exception {
    new TagVLiteralOrFilter(TAGK, null);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorEmptyFilter() throws Exception {
    new TagVLiteralOrFilter(TAGK, "");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorJustAPipe() throws Exception {
    new TagVLiteralOrFilter(TAGK, "|");
  }

  @Test
  public void toStringTest() throws Exception {
    TagVFilter filter = new TagVLiteralOrFilter(TAGK, "LutZe|CMTDibbler|Slant");
    assertTrue(filter.toString().contains("literal_or"));
  }
  
  @Test
  public void hashCodeAndEqualsTest() throws Exception {
    TagVFilter filter_a = new TagVLiteralOrFilter(TAGK, "LutZe|CMTDibbler|Slant");
    TagVFilter filter_b = new TagVLiteralOrFilter(TAGK, "LutZe|CMTDibbler|Slant");
    TagVFilter filter_c = new TagVLiteralOrFilter(TAGK, "LutZe|Slant");
    TagVFilter filter_d = new TagVLiteralOrFilter(TAGK, "LutZe|cmtdibbler|Slant");
    
    assertEquals(filter_a.hashCode(), filter_b.hashCode());
    assertFalse(filter_a.hashCode() == filter_c.hashCode());
    assertFalse(filter_a.hashCode() == filter_d.hashCode());
    
    assertEquals(filter_a, filter_b);
    assertFalse(filter_a.equals(filter_c));
    assertFalse(filter_a.equals(filter_d));
  }
}

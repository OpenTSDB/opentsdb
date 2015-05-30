package net.opentsdb.query.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

public class TestTagVNotLiteralOrFilter {
  private static final String TAGK = "host";
  private Map<String, String> tags;
  
  @Before
  public void before() throws Exception {
    tags = new HashMap<String, String>(1);
    tags.put(TAGK, "CMTDibbler");
  }
  
  @Test
  public void matchMiddle() throws Exception {
    TagVFilter filter = new TagVNotLiteralOrFilter(TAGK, 
        "LutZe|CMTDibbler|Slant");
    assertFalse(filter.match(tags));
    assertFalse(((TagVNotLiteralOrFilter)filter).isCaseInsensitive());
  }
  
  @Test
  public void matchStart() throws Exception {
    TagVFilter filter = new TagVNotLiteralOrFilter(TAGK, 
        "CMTDibbler|LutZe|Slant");
    assertFalse(filter.match(tags));
  }
  
  @Test
  public void matchEnd() throws Exception {
    TagVFilter filter = new TagVNotLiteralOrFilter(TAGK, 
        "LutZe|Slant|CMTDibbler");
    assertFalse(filter.match(tags));
  }
  
  @Test
  public void matchNoPipes() throws Exception {
    TagVFilter filter = new TagVNotLiteralOrFilter(TAGK, 
        "CMTDibbler");
    assertFalse(filter.match(tags));
  }
  
  @Test
  public void matchPipeNoValueAfter() throws Exception {
    TagVFilter filter = new TagVNotLiteralOrFilter(TAGK, 
        "CMTDibbler|");
    assertFalse(filter.match(tags));
  }
  
  @Test
  public void matchPipeNoValueBefore() throws Exception {
    TagVFilter filter = new TagVNotLiteralOrFilter(TAGK, 
        "|CMTDibbler");
    assertFalse(filter.match(tags));
  }
  
  @Test
  public void matchFail() throws Exception {
    TagVFilter filter = new TagVNotLiteralOrFilter(TAGK, 
        "LutZe|Keli|Slant");
    assertTrue(filter.match(tags));
  }
  
  @Test
  public void matchFailCase() throws Exception {
    TagVFilter filter = new TagVNotLiteralOrFilter(TAGK, 
        "LutZe|CMtDibbler|Slant");
    assertTrue(filter.match(tags));
  }
  
  @Test
  public void matchCaseInsensitive() throws Exception {
    TagVFilter filter = new TagVNotLiteralOrFilter(TAGK, 
        "LutZe|CMtDibbler|Slant", true);
    assertFalse(filter.match(tags));
    assertTrue(((TagVNotLiteralOrFilter)filter).isCaseInsensitive());
  }
  
  @Test
  public void matchCaseInsensitiveFail() throws Exception {
    TagVFilter filter = new TagVNotLiteralOrFilter(TAGK, 
        "LutZe|CMtDibble|Slant", true);
    assertTrue(filter.match(tags));
    assertTrue(((TagVNotLiteralOrFilter)filter).isCaseInsensitive());
  }
  
  @Test
  public void matchNoSuchTagk() throws Exception {
    TagVFilter filter = new TagVNotLiteralOrFilter(TAGK, "LutZe|Keli|Slant");
    tags.clear();
    tags.put("colo", "lga");
    assertTrue(filter.match(tags));
  }
  
  @Test
  public void matchNoSuchTagkCaseInsensitive() throws Exception {
    TagVFilter filter = new TagVNotLiteralOrFilter(TAGK, "LutZe|Keli|Slant", true);
    tags.clear();
    tags.put("colo", "lga");
    assertTrue(filter.match(tags));
  }
  
  @Test
  public void matchSingle() throws Exception {
    TagVFilter filter = new TagVNotLiteralOrFilter(TAGK, "CMTDibbler");
    assertFalse(filter.match(tags));
    assertFalse(((TagVNotLiteralOrFilter)filter).isCaseInsensitive());
  }
  
  @Test
  public void matchSingleCaseInsensitive() throws Exception {
    TagVFilter filter = new TagVNotLiteralOrFilter(TAGK, "cmtDibbler", true);
    assertFalse(filter.match(tags));
    assertTrue(((TagVNotLiteralOrFilter)filter).isCaseInsensitive());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorNullTagk() throws Exception {
    new TagVNotLiteralOrFilter(null, "LutZe|Keli|Slant");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorEmptyTagk() throws Exception {
    new TagVNotLiteralOrFilter("", "LutZe|Keli|Slant");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorNullFilter() throws Exception {
    new TagVNotLiteralOrFilter(TAGK, null);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorEmptyFilter() throws Exception {
    new TagVNotLiteralOrFilter(TAGK, "");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorJustAPipe() throws Exception {
    new TagVNotLiteralOrFilter(TAGK, "|");
  }

  @Test
  public void toStringTest() throws Exception {
    TagVFilter filter = new TagVNotLiteralOrFilter(TAGK, "LutZe|CMTDibbler|Slant");
    assertTrue(filter.toString().contains("literal_or"));
  }
  
  @Test
  public void hashCodeAndEqualsTest() throws Exception {
    TagVFilter filter_a = new TagVNotLiteralOrFilter(TAGK, "LutZe|CMTDibbler|Slant");
    TagVFilter filter_b = new TagVNotLiteralOrFilter(TAGK, "LutZe|CMTDibbler|Slant");
    TagVFilter filter_c = new TagVNotLiteralOrFilter(TAGK, "LutZe|Slant");
    TagVFilter filter_d = new TagVNotLiteralOrFilter(TAGK, "LutZe|cmtdibbler|Slant");
    
    assertEquals(filter_a.hashCode(), filter_b.hashCode());
    assertFalse(filter_a.hashCode() == filter_c.hashCode());
    assertFalse(filter_a.hashCode() == filter_d.hashCode());
    
    assertEquals(filter_a, filter_b);
    assertFalse(filter_a.equals(filter_c));
    assertFalse(filter_a.equals(filter_d));
  }
}

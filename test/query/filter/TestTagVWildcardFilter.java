package net.opentsdb.query.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

public class TestTagVWildcardFilter {
  private static final String TAGK = "host";
  private Map<String, String> tags;
  
  @Before
  public void before() throws Exception {
    tags = new HashMap<String, String>(1);
    tags.put(TAGK, "ogg-01.ops.ankh.morpork.com");
  }
  
  @Test
  public void matchAll() throws Exception {
    TagVFilter filter = new TagVWildcardFilter(TAGK, "*");
    assertTrue(filter.match(tags).join());
  }
  
  @Test
  public void matchAllNoSuchKey() throws Exception {
    TagVFilter filter = new TagVWildcardFilter("hobbes", "*");
    assertFalse(filter.match(tags).join());
  }
  
  @Test
  public void matchPostfix() throws Exception {
    TagVFilter filter = new TagVWildcardFilter(TAGK, "*.morpork.com");
    assertTrue(filter.match(tags).join());
  }
  
  @Test
  public void matchPrefix() throws Exception {
    TagVFilter filter = new TagVWildcardFilter(TAGK, 
        "ogg*");
    assertTrue(filter.match(tags).join());
  }
  
  @Test
  public void matchInfix() throws Exception {
    TagVFilter filter = new TagVWildcardFilter(TAGK, 
        "ogg*com");
    assertTrue(filter.match(tags).join());
  }
  
  @Test
  public void matchDoubleInfix() throws Exception {
    TagVFilter filter = new TagVWildcardFilter(TAGK, 
        "ogg*ops*ank*com");
    assertTrue(filter.match(tags).join());
  }
  
  @Test
  public void matchTripleInfix() throws Exception {
    TagVFilter filter = new TagVWildcardFilter(TAGK, 
        "ogg*ops*com");
    assertTrue(filter.match(tags).join());
  }
  
  @Test
  public void matchPreAndPostfix() throws Exception {
    TagVFilter filter = new TagVWildcardFilter(TAGK, 
        "*morpork*");
    assertTrue(filter.match(tags).join());
  }
  
  @Test
  public void matchPostAndInfix() throws Exception {
    TagVFilter filter = new TagVWildcardFilter(TAGK, 
        "*ops*com");
    assertTrue(filter.match(tags).join());
  }
  
  @Test
  public void matchPostAndDoubleInfix() throws Exception {
    TagVFilter filter = new TagVWildcardFilter(TAGK, 
        "*ops*mor*com");
    assertTrue(filter.match(tags).join());
  }
  
  @Test
  public void matchPreAndInfix() throws Exception {
    TagVFilter filter = new TagVWildcardFilter(TAGK, 
        "ogg*ops*");
    assertTrue(filter.match(tags).join());
  }
  
  @Test
  public void matchPreAndDoubleInfix() throws Exception {
    TagVFilter filter = new TagVWildcardFilter(TAGK, 
        "ogg*ops*mor*");
    assertTrue(filter.match(tags).join());
  }
  
  @Test
  public void matchMultiWildcardInfix() throws Exception {
    TagVFilter filter = new TagVWildcardFilter(TAGK, 
        "ogg***com");
    assertTrue(filter.match(tags).join());
  }
  
  @Test
  public void matchMultiWildcardPrefix() throws Exception {
    TagVFilter filter = new TagVWildcardFilter(TAGK, 
        "ogg*****");
    assertTrue(filter.match(tags).join());
  }
  
  @Test
  public void matchMultiWildcardPostfix() throws Exception {
    TagVFilter filter = new TagVWildcardFilter(TAGK, 
        "****com");
    assertTrue(filter.match(tags).join());
  }
  
  @Test
  public void matchWildcardsEverywhere() throws Exception {
    TagVFilter filter = new TagVWildcardFilter(TAGK, 
        "****ogg*****mor****com****");
    assertTrue(filter.match(tags).join());
  }
  
  @Test
  public void matchExactPostfix() throws Exception {
    tags.put(TAGK, "*ops*mor");
    TagVFilter filter = new TagVWildcardFilter(TAGK, 
        "*ops*mor");
    assertTrue(filter.match(tags).join());
  }
  
  @Test
  public void matchExactPretfix() throws Exception {
    tags.put(TAGK, "ogg*ops*");
    TagVFilter filter = new TagVWildcardFilter(TAGK, 
        "ogg*ops*");
    assertTrue(filter.match(tags).join());
  }
  
  @Test
  public void matchExactInfix() throws Exception {
    tags.put(TAGK, "ogg*ops*mor");
    TagVFilter filter = new TagVWildcardFilter(TAGK, 
        "ogg*ops*mor");
    assertTrue(filter.match(tags).join());
  }
  
  // Make sure this file is encoded in UTF-8 of the following will fail
  @Test
  public void matchUTF8Postfix() throws Exception {
    tags.put(TAGK, "Здравей'_хора");
    TagVFilter filter = new TagVWildcardFilter(TAGK, 
        "*хора");
    assertTrue(filter.match(tags).join());
  }
  
  @Test
  public void matchUTF8Prefix() throws Exception {
    tags.put(TAGK, "Здравей'_хора");
    TagVFilter filter = new TagVWildcardFilter(TAGK, 
        "Здр*");
    assertTrue(filter.match(tags).join());
  }
  
  @Test
  public void matchUTF8Infix() throws Exception {
    tags.put(TAGK, "Здравей'_хора");
    TagVFilter filter = new TagVWildcardFilter(TAGK, 
        "Здр*ра");
    assertTrue(filter.match(tags).join());
  }

  @Test
  public void matchPostfixFail() throws Exception {
    TagVFilter filter = new TagVWildcardFilter(TAGK, 
        "*.morpork.org");
    assertFalse(filter.match(tags).join());
  }
  
  @Test
  public void matchPrefixFail() throws Exception {
    TagVFilter filter = new TagVWildcardFilter(TAGK, 
        "magrat*");
    assertFalse(filter.match(tags).join());
  }
  
  @Test
  public void matchInfixFail() throws Exception {
    TagVFilter filter = new TagVWildcardFilter(TAGK, 
        "magrat*com");
    assertFalse(filter.match(tags).join());
  }
  
  @Test
  public void matchPreAndPostfixFail() throws Exception {
    TagVFilter filter = new TagVWildcardFilter(TAGK, 
        "*quirm*");
    assertFalse(filter.match(tags).join());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorNullFilter() throws Exception {
    new TagVWildcardFilter(TAGK, null);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorEmptyFilter() throws Exception {
    new TagVWildcardFilter(TAGK, "");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorNoWildcard() throws Exception {
    new TagVWildcardFilter(TAGK, "someliteral");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorNullTagk() throws Exception {
    new TagVWildcardFilter(null, "*quirm*");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorEmptyTagk() throws Exception {
    new TagVWildcardFilter("", "*quirm*");
  }

  @Test
  public void matchPostfixCaseFail() throws Exception {
    TagVFilter filter = new TagVWildcardFilter(TAGK, 
        "*.MorPork.com");
    assertFalse(filter.match(tags).join());
  }
  
  @Test
  public void matchPrefixCaseFail() throws Exception {
    TagVFilter filter = new TagVWildcardFilter(TAGK, 
        "Ogg*");
    assertFalse(filter.match(tags).join());
  }
  
  @Test
  public void matchInfixCaseFail() throws Exception {
    TagVFilter filter = new TagVWildcardFilter(TAGK, 
        "ogG*Com");
    assertFalse(filter.match(tags).join());
  }
  
  @Test
  public void matchPostfixCaseInsensitive() throws Exception {
    TagVFilter filter = new TagVWildcardFilter(TAGK, 
        "*.MorPork.com", true);
    assertTrue(filter.match(tags).join());
  }
  
  @Test
  public void matchPrefixCaseInsensitive() throws Exception {
    TagVFilter filter = new TagVWildcardFilter(TAGK, 
        "Ogg*", true);
    assertTrue(filter.match(tags).join());
  }
  
  @Test
  public void matchInfixCaseInsensitive() throws Exception {
    TagVFilter filter = new TagVWildcardFilter(TAGK, 
        "ogG*Com", true);
    assertTrue(filter.match(tags).join());
  }
  
  @Test
  public void matchNothingButStars() throws Exception {
    TagVFilter filter = new TagVWildcardFilter(TAGK, "****");
    assertTrue(filter.match(tags).join());
  }
  
  @Test
  public void matchNoSuchTagk() throws Exception {
    final TagVFilter filter = new TagVWildcardFilter(TAGK, "*.morpork.com");
    tags.remove("host");
    tags.put("colo", "lga");
    assertFalse(filter.match(tags).join());
  }
  
  @Test
  public void matchNoSuchTagkCaseInsensitive() throws Exception {
    TagVFilter filter = new TagVWildcardFilter(TAGK, "*.morpork.com", true);
    tags.remove("host");
    tags.put("colo", "lga");
    assertFalse(filter.match(tags).join());
  }
  
  @Test
  public void toStringTest() throws Exception {
    TagVFilter filter = new TagVWildcardFilter(TAGK, "ogg*com");
    assertTrue(filter.toString().contains("wild"));
  }
  
  @Test
  public void hashCodeAndEqualsTest() throws Exception {
    TagVFilter filter_a = new TagVWildcardFilter(TAGK, "ogg*com");
    TagVFilter filter_b = new TagVWildcardFilter(TAGK, "ogg*com");
    TagVFilter filter_c = new TagVWildcardFilter(TAGK, "*com");
    TagVFilter filter_d = new TagVWildcardFilter(TAGK, "Ogg*com");
    
    assertEquals(filter_a.hashCode(), filter_b.hashCode());
    assertFalse(filter_a.hashCode() == filter_c.hashCode());
    assertFalse(filter_a.hashCode() == filter_d.hashCode());
    
    assertEquals(filter_a, filter_b);
    assertFalse(filter_a.equals(filter_c));
    assertFalse(filter_a.equals(filter_d));
  }

}

package net.opentsdb.query.filter;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

public class TestTagVNotKeyFilter {
  private static final String TAGK = "host";
  private static final String TAGK2 = "owner";
  private Map<String, String> tags;
  
  @Before
  public void before() throws Exception {
    tags = new HashMap<String, String>(1);
    tags.put(TAGK, "ogg-01.ops.ankh.morpork.com");
    tags.put(TAGK2, "Hrun");
  }
  
  @Test
  public void matchHasKey() throws Exception {
    TagVFilter filter = new TagVNotKeyFilter(TAGK, "");
    assertFalse(filter.match(tags).join());
  }
  
  @Test
  public void matchDoesNotHaveKey() throws Exception {
    TagVFilter filter = new TagVNotKeyFilter("colo", "");
    assertTrue(filter.match(tags).join());
  }
  
  @Test
  public void ctorNullFilter() throws Exception {
    TagVFilter filter = new TagVNotKeyFilter(TAGK, null);
    assertTrue(filter.postScan());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorFilterHasValue() throws Exception {
    assertNotNull(new TagVNotKeyFilter(TAGK, "Evadne"));
  }
}

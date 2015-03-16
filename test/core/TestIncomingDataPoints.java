package net.opentsdb.core;

import static org.junit.Assert.assertArrayEquals;
import static org.mockito.Mockito.when;

import net.opentsdb.uid.NoSuchUniqueName;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
public class TestIncomingDataPoints extends BaseTsdbTest {

  @Test
  public void rowKeyTemplate() throws Exception {
    final byte[] expected = new byte[METRIC_BYTES.length + Const.TIMESTAMP_BYTES
                                     + TAGK_BYTES.length + TAGV_BYTES.length];
    System.arraycopy(METRIC_BYTES, 0, expected, 0, METRIC_BYTES.length);
    System.arraycopy(TAGK_BYTES, 0, expected, 
        METRIC_BYTES.length + Const.TIMESTAMP_BYTES, TAGK_BYTES.length);
    System.arraycopy(TAGV_BYTES, 0, expected, 
        METRIC_BYTES.length + Const.TIMESTAMP_BYTES + TAGK_BYTES.length, 
        TAGV_BYTES.length);
    
    final byte[] key = IncomingDataPoints.rowKeyTemplate(tsdb, 
        METRIC_STRING, tags);
    assertArrayEquals(expected, key);
  }
  
  @Test
  public void rowKeyTemplateWithSalt1Byte() throws Exception {
    PowerMockito.mockStatic(Const.class);
    PowerMockito.when(Const.SALT_WIDTH()).thenReturn(1);
    final byte[] expected = new byte[METRIC_BYTES.length + Const.TIMESTAMP_BYTES
                                     + TAGK_BYTES.length + TAGV_BYTES.length + 1];
    System.arraycopy(METRIC_BYTES, 0, expected, 1, METRIC_BYTES.length);
    System.arraycopy(TAGK_BYTES, 0, expected, 
        METRIC_BYTES.length + Const.TIMESTAMP_BYTES + 1, TAGK_BYTES.length);
    System.arraycopy(TAGV_BYTES, 0, expected, 
        METRIC_BYTES.length + Const.TIMESTAMP_BYTES + TAGK_BYTES.length + 1, 
        TAGV_BYTES.length);
    
    final byte[] key = IncomingDataPoints.rowKeyTemplate(tsdb, 
        METRIC_STRING, tags);
    assertArrayEquals(expected, key);
  }
  
  @Test
  public void rowKeyTemplateWithSalt2Bytes() throws Exception {
    PowerMockito.mockStatic(Const.class);
    PowerMockito.when(Const.SALT_WIDTH()).thenReturn(2);
    final byte[] expected = new byte[METRIC_BYTES.length + Const.TIMESTAMP_BYTES
                                     + TAGK_BYTES.length + TAGV_BYTES.length + 2];
    System.arraycopy(METRIC_BYTES, 0, expected, 2, METRIC_BYTES.length);
    System.arraycopy(TAGK_BYTES, 0, expected, 
        METRIC_BYTES.length + Const.TIMESTAMP_BYTES + 2, TAGK_BYTES.length);
    System.arraycopy(TAGV_BYTES, 0, expected, 
        METRIC_BYTES.length + Const.TIMESTAMP_BYTES + TAGK_BYTES.length + 2, 
        TAGV_BYTES.length);
    
    final byte[] key = IncomingDataPoints.rowKeyTemplate(tsdb, 
        METRIC_STRING, tags);
    assertArrayEquals(expected, key);
  }

  @Test (expected = NoSuchUniqueName.class)
  public void rowKeyTemplateNoSuchMetric() throws Exception {
    IncomingDataPoints.rowKeyTemplate(tsdb, NSUN_METRIC, tags);
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void rowKeyTemplateNoSuchTagK() throws Exception {
    tags.clear();
    tags.put(NSUN_TAGK, TAGV_STRING);
    IncomingDataPoints.rowKeyTemplate(tsdb, NSUN_METRIC, tags);
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void rowKeyTemplateNoSuchTagV() throws Exception {
    tags.put(TAGK_STRING, NSUN_TAGV);
    IncomingDataPoints.rowKeyTemplate(tsdb, NSUN_METRIC, tags);
  }
  
  @Test (expected = NullPointerException.class)
  public void rowKeyTemplateNullTSDB() throws Exception {
    IncomingDataPoints.rowKeyTemplate(null, NSUN_METRIC, tags);
  }
  
  @Test (expected = NullPointerException.class)
  public void rowKeyTemplateNullMetric() throws Exception {
    IncomingDataPoints.rowKeyTemplate(tsdb, null, tags);
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void rowKeyTemplateEmptyMetric() throws Exception {
    when(metrics.getOrCreateId("")).thenThrow(new NoSuchUniqueName("metrics", ""));
    IncomingDataPoints.rowKeyTemplate(tsdb, "", tags);
  }
  
  @Test (expected = NullPointerException.class)
  public void rowKeyTemplateNullTags() throws Exception {
    IncomingDataPoints.rowKeyTemplate(tsdb, METRIC_STRING, null);
  }
  
  // NOTE: This method doesn't enforce that we have tags
  @Test
  public void rowKeyTemplateEmptyTags() throws Exception {
    tags.clear();
    final byte[] expected = new byte[METRIC_BYTES.length + Const.TIMESTAMP_BYTES];
    System.arraycopy(METRIC_BYTES, 0, expected, 0, METRIC_BYTES.length);
    
    final byte[] key = IncomingDataPoints.rowKeyTemplate(tsdb, 
        METRIC_STRING, tags);
    assertArrayEquals(expected, key);
  }
}

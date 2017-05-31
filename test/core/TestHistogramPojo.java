package net.opentsdb.core;

import static org.mockito.Mockito.when;

import org.hbase.async.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import net.opentsdb.utils.Config;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ TSDB.class })
public class TestHistogramPojo {

  private TSDB tsdb;
  private Config config;
  private HistogramCodecManager manager;
  
  @Before
  public void before() throws Exception {
    tsdb = PowerMockito.mock(TSDB.class);
    config = new Config(false);
    
    config.overrideConfig("tsd.core.histograms.config", 
        "{\"net.opentsdb.core.SimpleHistogramDecoder\": 0}");
    when(tsdb.getConfig()).thenReturn(config);
    
    manager = new HistogramCodecManager(tsdb);
    when(tsdb.histogramManager()).thenReturn(manager);
  }
  
  @Test
  public void foo() throws Exception {
    int v = 255;
    
    System.out.println("CV: " + (byte) v);
    
//    SimpleHistogram y1Hist = new SimpleHistogram();
//    y1Hist.addBucket(1.0f, 2.0f, 5L);
//    y1Hist.addBucket(2.0f, 3.0f, 5L);
//    y1Hist.addBucket(3.0f, 10.0f, 0L);
//    
//    byte[] raw = y1Hist.histogram(manager);
//    System.out.println(HistogramPojo.bytesToBase64String(raw));
//    System.out.println(HistogramPojo.bytesToHexString(raw));
//    
    //HistogramDataPoint h = tsdb.histogramManager().getDecoder((byte) 0).decode(raw, 1356998400000L);
    //System.out.println(h);
  }
  
}

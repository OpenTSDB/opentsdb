package net.opentsdb.core;

import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.util.HashMap;
import java.util.Map;

import net.opentsdb.storage.MockBase;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.Config;

import org.hbase.async.HBaseClient;
import org.jboss.netty.util.HashedWheelTimer;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import com.stumbleupon.async.Deferred;

/**
 * Sets up a real TSDB with mocked client, compaction queue and timer along
 * with mocked UID assignment, fetches for common unit tests.
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@PrepareForTest({TSDB.class, Config.class, UniqueId.class, HBaseClient.class, 
  HashedWheelTimer.class, CompactionQueue.class, Const.class })
public class BaseTsdbTest {
  
  public static final String METRIC_STRING = "sys.cpu.user";
  public static final byte[] METRIC_BYTES = new byte[] { 0, 0, 1 };
  public static final String METRIC_B_STRING = "sys.cpu.system";
  public static final byte[] METRIC_B_BYTES = new byte[] { 0, 0, 2 };
  public static final String NSUN_METRIC = "sys.cpu.nice";
  public static final byte[] NSUI_METRIC = new byte[] { 0, 0, 3 };
  
  public static final String TAGK_STRING = "host";
  public static final byte[] TAGK_BYTES = new byte[] { 0, 0, 1 };
  public static final String TAGK_B_STRING = "owner";
  public static final byte[] TAGK_B_BYTES = new byte[] { 0, 0, 3 };
  public static final String NSUN_TAGK = "dc";
  public static final byte[] NSUI_TAGK = new byte[] { 0, 0, 4 };
  
  public static final String TAGV_STRING = "web01";
  public static final byte[] TAGV_BYTES = new byte[] { 0, 0, 1 };
  public static final String TAGV_B_STRING = "web02";
  public static final byte[] TAGV_B_BYTES = new byte[] { 0, 0, 2 };
  public static final String NSUN_TAGV = "web03";
  public static final byte[] NSUI_TAGV = new byte[] { 0, 0, 3 };

  protected HashedWheelTimer timer;
  protected CompactionQueue compaction_queue;
  protected Config config;
  protected TSDB tsdb;
  protected HBaseClient client = mock(HBaseClient.class);
  protected UniqueId metrics = mock(UniqueId.class);
  protected UniqueId tag_names = mock(UniqueId.class);
  protected UniqueId tag_values = mock(UniqueId.class);
  protected Map<String, String> tags = new HashMap<String, String>(1);
  protected MockBase storage;
  
  @Before
  public void before() throws Exception {
    timer = mock(HashedWheelTimer.class);
    compaction_queue = mock(CompactionQueue.class);
    
    PowerMockito.whenNew(HashedWheelTimer.class).withNoArguments()
      .thenReturn(timer);
    PowerMockito.whenNew(HBaseClient.class).withAnyArguments()
      .thenReturn(client);
    PowerMockito.whenNew(CompactionQueue.class).withAnyArguments()
      .thenReturn(compaction_queue);
    
    config = new Config(false);
    tsdb = PowerMockito.spy(new TSDB(config));

    config.setAutoMetric(true);
    
    Whitebox.setInternalState(tsdb, "metrics", metrics);
    Whitebox.setInternalState(tsdb, "tag_names", tag_names);
    Whitebox.setInternalState(tsdb, "tag_values", tag_values);

    setupMetricMaps();
    setupTagkMaps();
    setupTagvMaps();
    
    when(metrics.width()).thenReturn((short)3);
    when(tag_names.width()).thenReturn((short)3);
    when(tag_values.width()).thenReturn((short)3);
    
    tags.put(TAGK_STRING, TAGV_STRING);
  }
  
  void setupMetricMaps() {
    when(metrics.getId(METRIC_STRING)).thenReturn(METRIC_BYTES);
    when(metrics.getIdAsync(METRIC_STRING))
      .thenReturn(Deferred.fromResult(METRIC_BYTES));
    when(metrics.getOrCreateId(METRIC_STRING))
      .thenReturn(METRIC_BYTES);
    
    when(metrics.getId(METRIC_B_STRING)).thenReturn(METRIC_B_BYTES);
    when(metrics.getIdAsync(METRIC_B_STRING))
      .thenReturn(Deferred.fromResult(METRIC_B_BYTES));
    when(metrics.getOrCreateId(METRIC_B_STRING))
      .thenReturn(METRIC_B_BYTES);
    
    when(metrics.getNameAsync(METRIC_BYTES))
      .thenReturn(Deferred.fromResult(METRIC_STRING));
    when(metrics.getNameAsync(METRIC_B_BYTES))
    .thenReturn(Deferred.fromResult(METRIC_B_STRING));
    
    final NoSuchUniqueName nsun = new NoSuchUniqueName(NSUN_METRIC, "metric");
    
    when(metrics.getId(NSUN_METRIC)).thenThrow(nsun);
    when(metrics.getIdAsync(NSUN_METRIC))
      .thenReturn(Deferred.<byte[]>fromError(nsun));
    when(metrics.getOrCreateId(NSUN_METRIC)).thenThrow(nsun);
  }
  
  void setupTagkMaps() {
    when(tag_names.getId(TAGK_STRING)).thenReturn(TAGK_BYTES);
    when(tag_names.getOrCreateId(TAGK_STRING)).thenReturn(TAGK_BYTES);
    when(tag_names.getIdAsync(TAGK_STRING))
      .thenReturn(Deferred.fromResult(TAGK_BYTES));
    when(tag_names.getOrCreateIdAsync(TAGK_STRING))
      .thenReturn(Deferred.fromResult(TAGK_BYTES));
    
    when(tag_names.getId(TAGK_B_STRING)).thenReturn(TAGK_B_BYTES);
    when(tag_names.getOrCreateId(TAGK_B_STRING)).thenReturn(TAGK_B_BYTES);
    when(tag_names.getIdAsync(TAGK_B_STRING))
      .thenReturn(Deferred.fromResult(TAGK_B_BYTES));
    when(tag_names.getOrCreateIdAsync(TAGK_B_STRING))
      .thenReturn(Deferred.fromResult(TAGK_B_BYTES));
    
    when(tag_names.getNameAsync(TAGK_BYTES))
      .thenReturn(Deferred.fromResult(TAGK_STRING));
    
    final NoSuchUniqueName nsun = new NoSuchUniqueName(NSUN_TAGK, "tagk");
    
    when(tag_names.getId(NSUN_TAGK))
      .thenThrow(nsun);
    when(tag_names.getIdAsync(NSUN_TAGK))
      .thenReturn(Deferred.<byte[]>fromError(nsun));
  }
  
  void setupTagvMaps() {
    when(tag_values.getId(TAGV_STRING)).thenReturn(TAGV_BYTES);
    when(tag_values.getOrCreateId(TAGV_STRING)).thenReturn(TAGV_BYTES);
    when(tag_values.getIdAsync(TAGV_STRING))
      .thenReturn(Deferred.fromResult(TAGV_BYTES));
    when(tag_values.getOrCreateIdAsync(TAGV_STRING))
      .thenReturn(Deferred.fromResult(TAGV_BYTES));
  
    when(tag_values.getId(TAGV_B_STRING)).thenReturn(TAGV_B_BYTES);
    when(tag_values.getOrCreateId(TAGV_B_STRING)).thenReturn(TAGV_B_BYTES);
    when(tag_values.getIdAsync(TAGV_B_STRING))
      .thenReturn(Deferred.fromResult(TAGV_B_BYTES));
    when(tag_values.getOrCreateIdAsync(TAGV_B_STRING))
      .thenReturn(Deferred.fromResult(TAGV_B_BYTES));
    
    when(tag_values.getNameAsync(TAGV_BYTES))
      .thenReturn(Deferred.fromResult(TAGV_STRING));
    when(tag_values.getNameAsync(TAGV_B_BYTES))
      .thenReturn(Deferred.fromResult(TAGV_B_STRING));
    
    final NoSuchUniqueName nsun = new NoSuchUniqueName(NSUN_TAGV, "tagv");
    
    when(tag_values.getId(NSUN_TAGV)).thenThrow(nsun);
    when(tag_values.getIdAsync(NSUN_TAGV))
      .thenReturn(Deferred.<byte[]>fromError(nsun));
  }
}
package net.opentsdb.tools;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.ArrayList;

import org.junit.Before;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;

import com.stumbleupon.async.Deferred;

import net.opentsdb.core.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.Tags;
import net.opentsdb.storage.MockBase;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.utils.Config;

@PrepareForTest({ Const.class })
public class TestFsckSalted extends TestFsck {

  @Before
  public void before() throws Exception {
    PowerMockito.mockStatic(Const.class);
    PowerMockito.when(Const.SALT_BUCKETS()).thenReturn(2);
    PowerMockito.when(Const.SALT_WIDTH()).thenReturn(1);
    PowerMockito.when(Const.MAX_NUM_TAGS()).thenReturn((short) 8);
    
    GLOBAL_ROW = new byte[] {0, 0, 0, 0, 0x52, (byte)0xC3, 0x5A, (byte)0x80};
    ROW = MockBase.stringToBytes("0000000150E22700000001000001");
    ROW2 = MockBase.stringToBytes("0100000150E23510000001000001");
    ROW3 = MockBase.stringToBytes("0100000150E24320000001000001");
    BAD_KEY = new byte[] { 0x01, 0x00, 0x00, 0x01 };
    
    config = new Config(false);
    tsdb = new TSDB(client, config);
    when(client.flush()).thenReturn(Deferred.fromResult(null));
    
    storage = new MockBase(tsdb, client, true, true, true, true);
    storage.setFamily("t".getBytes(MockBase.ASCII()));
    
    when(options.fix()).thenReturn(false);
    when(options.compact()).thenReturn(false);
    when(options.resolveDupes()).thenReturn(false);
    when(options.lastWriteWins()).thenReturn(false);
    when(options.deleteOrphans()).thenReturn(false);
    when(options.deleteUnknownColumns()).thenReturn(false);
    when(options.deleteBadValues()).thenReturn(false);
    when(options.deleteBadRows()).thenReturn(false);
    when(options.deleteBadCompacts()).thenReturn(false);
    when(options.threads()).thenReturn(1);

    // replace the "real" field objects with mocks
    Field met = tsdb.getClass().getDeclaredField("metrics");
    met.setAccessible(true);
    met.set(tsdb, metrics);
    
    Field tagk = tsdb.getClass().getDeclaredField("tag_names");
    tagk.setAccessible(true);
    tagk.set(tsdb, tag_names);
    
    Field tagv = tsdb.getClass().getDeclaredField("tag_values");
    tagv.setAccessible(true);
    tagv.set(tsdb, tag_values);
    
    // mock UniqueId
    when(metrics.getId("sys.cpu.user")).thenReturn(new byte[] { 0, 0, 1 });
    when(metrics.getNameAsync(new byte[] { 0, 0, 1 }))
      .thenReturn(Deferred.fromResult("sys.cpu.user"));
    when(metrics.getId("sys.cpu.system"))
      .thenThrow(new NoSuchUniqueName("sys.cpu.system", "metric"));
    when(metrics.getId("sys.cpu.nice")).thenReturn(new byte[] { 0, 0, 2 });
    when(metrics.getName(new byte[] { 0, 0, 2 })).thenReturn("sys.cpu.nice");
    when(tag_names.getId("host")).thenReturn(new byte[] { 0, 0, 1 });
    when(tag_names.getName(new byte[] { 0, 0, 1 })).thenReturn("host");
    when(tag_names.getOrCreateId("host")).thenReturn(new byte[] { 0, 0, 1 });
    when(tag_names.getId("dc")).thenThrow(new NoSuchUniqueName("dc", "metric"));
    when(tag_values.getId("web01")).thenReturn(new byte[] { 0, 0, 1 });
    when(tag_values.getName(new byte[] { 0, 0, 1 })).thenReturn("web01");
    when(tag_values.getOrCreateId("web01")).thenReturn(new byte[] { 0, 0, 1 });
    when(tag_values.getId("web02")).thenReturn(new byte[] { 0, 0, 2 });
    when(tag_values.getName(new byte[] { 0, 0, 2 })).thenReturn("web02");
    when(tag_values.getOrCreateId("web02")).thenReturn(new byte[] { 0, 0, 2 });
    when(tag_values.getId("web03"))
      .thenThrow(new NoSuchUniqueName("web03", "metric"));

    PowerMockito.mockStatic(Tags.class);
    when(Tags.resolveIds((TSDB)any(), (ArrayList<byte[]>)any()))
      .thenReturn(null); // don't care

    when(metrics.width()).thenReturn((short)3);
    when(tag_names.width()).thenReturn((short)3);
    when(tag_values.width()).thenReturn((short)3);
  }
}

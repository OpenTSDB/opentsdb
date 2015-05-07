package net.opentsdb.tools;

import org.junit.Before;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;

import net.opentsdb.core.Const;
import net.opentsdb.storage.MockBase;

@PrepareForTest({ Const.class })
public class TestFsckSalted extends TestFsck {

  @Before
  public void beforeLocal() throws Exception {
    PowerMockito.mockStatic(Const.class);
    PowerMockito.when(Const.SALT_BUCKETS()).thenReturn(2);
    PowerMockito.when(Const.SALT_WIDTH()).thenReturn(1);
    
    GLOBAL_ROW = new byte[] {0, 0, 0, 0, 0x52, (byte)0xC3, 0x5A, (byte)0x80};
    ROW = MockBase.stringToBytes("0000000150E22700000001000001");
    ROW2 = MockBase.stringToBytes("0100000150E23510000001000001");
    ROW3 = MockBase.stringToBytes("0100000150E24320000001000001");
    BAD_KEY = new byte[] { 0x01, 0x00, 0x00, 0x01 };
  }
}

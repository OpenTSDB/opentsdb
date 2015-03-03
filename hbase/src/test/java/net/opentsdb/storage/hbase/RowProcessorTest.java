package net.opentsdb.storage.hbase;

import com.google.common.collect.Lists;
import com.stumbleupon.async.Deferred;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Scanner.class})
public class RowProcessorTest {
  private final String result = "result";

  private Scanner scanner;
  private RowProcessor rowProcessor;

  @Before
  public void setUp() throws IOException {
    scanner = PowerMockito.mock(Scanner.class);

    final ArrayList<ArrayList<KeyValue>> tsuids = Lists.newArrayListWithCapacity(2);
    tsuids.add(Lists.<KeyValue>newArrayListWithCapacity(1));
    tsuids.add(Lists.<KeyValue>newArrayListWithCapacity(1));

    when(scanner.nextRows())
        .thenReturn(Deferred.fromResult(tsuids))
        .thenReturn(Deferred.<ArrayList<ArrayList<KeyValue>>>fromResult(null));

    rowProcessor = mock(RowProcessor.class);
    when(rowProcessor.getResult()).thenReturn(result);
  }

  @Test (expected = NullPointerException.class)
  public void testProcessRowsNullScanner() throws Exception {
    RowProcessor.processRows(null, rowProcessor);
  }

  @Test (expected = NullPointerException.class)
  public void testProcessRowsNullRowProcessor() throws Exception {
    RowProcessor.processRows(scanner, null);
  }

  @Test
  public void testProcessRowGetsCalledForEachRow() throws Exception {
    RowProcessor.processRows(scanner, rowProcessor);
    verify(rowProcessor, times(2)).processRow(any(ArrayList.class));
  }

  @Test
  public void testRowProcessorClosesScannerOnSuccess() throws Exception {
    RowProcessor.processRows(scanner, rowProcessor);
    verify(scanner).close();
  }

  @Test
  public void testRowProcessorClosesScannerOnError() throws Exception {
    when(scanner.nextRows())
        .thenReturn(Deferred.<ArrayList<ArrayList<KeyValue>>>fromError(new Exception("")));

    RowProcessor.processRows(scanner, rowProcessor);
    verify(scanner).close();
  }

  @Test
  public void testRowProcessorReturnsResult() throws Exception {
    Deferred deferred = RowProcessor.processRows(scanner, rowProcessor);
    assertEquals(result, deferred.joinUninterruptibly());
  }
}
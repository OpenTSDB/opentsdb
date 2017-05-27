package net.opentsdb.core;

import com.stumbleupon.async.Deferred;
import net.opentsdb.storage.MockBase;
import net.opentsdb.tsd.RTPublisher;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;

public class TestTSDBAddHistogramPoint extends BaseTsdbTest {

    private static final byte HISTOGRAM_PREFIX = 0x6;

    @Before
    public void beforeLocal() throws Exception {
        storage = new MockBase(tsdb, client, true, true, true, true);
    }

    @Test
    public void addHistogramPoint() throws Exception {
        byte[] testRawValue = "Test Raw Value".getBytes();
        tsdb.addHistogramPoint(METRIC_STRING, 1356998400, testRawValue, tags).joinUninterruptibly();
        final byte[] row = getRowKey(METRIC_STRING, 1356998400, TAGK_STRING, TAGV_STRING);
        byte[] qualifier = Internal.getQualifier(1356998400, HISTOGRAM_PREFIX);
        final byte[] value = storage.getColumn(row, qualifier);
        assertNotNull(value);
        assertArrayEquals(testRawValue, value);
    }

    @Test (expected = IllegalArgumentException.class)
    public void addHistogramPointShortRawData() throws Exception {
        byte[] testRawValue = new byte[1];
        tsdb.addHistogramPoint(METRIC_STRING, 1356998400, testRawValue, tags).joinUninterruptibly();
    }

    @Test (expected = IllegalArgumentException.class)
    public void addHistogramPointNullRawData() throws Exception {
        tsdb.addHistogramPoint(METRIC_STRING, 1356998400, null, tags).joinUninterruptibly();
    }

    @Test
    public void addHistogramPointCallRTPublisher() throws Exception {
        byte[] raw_data = new byte[5];
        RTPublisher rt_publisher = mock(RTPublisher.class);
        setField(tsdb, "rt_publisher", rt_publisher);
        tsdb.addHistogramPoint(METRIC_STRING, 1356998400, raw_data, tags).joinUninterruptibly();

        byte[] tsuid = new byte[]{ 0, 0, 1, 0, 0, 1, 0, 0, 1};
        verify(rt_publisher, times(1)).publishHistogramPoint(METRIC_STRING, 1356998400, raw_data, tags, tsuid);
    }

    @Test
    public void addHistogramPointTSFilterPlugin() throws Exception {
        byte[] raw_data = new byte[5];
        WriteableDataPointFilterPlugin ts_filter = mock(WriteableDataPointFilterPlugin.class);
        when(ts_filter.filterDataPoints()).thenReturn(true);
        when(ts_filter.allowHistogramPoint(METRIC_STRING, 1356998400, raw_data, tags)).
                thenReturn(Deferred.fromResult(true));
        setField(tsdb, "ts_filter", ts_filter);
        tsdb.addHistogramPoint(METRIC_STRING, 1356998400, raw_data, tags).joinUninterruptibly();

        verify(ts_filter, times(1)).allowHistogramPoint(METRIC_STRING, 1356998400, raw_data, tags);
    }

    private static void setField(TSDB tsdb, String field_name, Object field_value)
            throws NoSuchFieldException, IllegalAccessException {
        Field field = TSDB.class.getDeclaredField(field_name);
        field.setAccessible(true);
        field.set(tsdb, field_value);
    }

}

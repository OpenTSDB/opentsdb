// This file is part of OpenTSDB.
// Copyright (C) 2015  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.core;

import net.opentsdb.rollup.RollupSpan;
import net.opentsdb.utils.Config;
import org.hbase.async.Bytes;
import org.hbase.async.HBaseClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({TSDB.class, HBaseClient.class, Config.class, SpanGroup.class,
        Span.class, RollupSpan.class})
public class TestSplitRollupSpanGroup {
    private final static long START_TS = 1356998400L;
    private final static long SPLIT_TS = 1356998500L;
    private final static long END_TS = 1356998600L;

    private TSDB tsdb;
    private SpanGroup rollupSpanGroup;
    private SpanGroup rawSpanGroup;

    @Before
    public void before() {
        rawSpanGroup = PowerMockito.spy(new SpanGroup(tsdb, START_TS, SPLIT_TS, null, false, Aggregators.SUM, 0, null));
        rollupSpanGroup = PowerMockito.spy(new SpanGroup(tsdb, SPLIT_TS, END_TS, null, false, Aggregators.SUM, 0, null));

        doAnswer(new SeekableViewAnswer(START_TS, 100, 1, true)).when(rollupSpanGroup).iterator();
        doAnswer(new SeekableViewAnswer(SPLIT_TS, 100, 2, true)).when(rawSpanGroup).iterator();

        tsdb = PowerMockito.mock(TSDB.class);
    }

    @Test
    public void testConstructorFiltersNullSpanGroups() {
        SplitRollupSpanGroup group = new SplitRollupSpanGroup(null, rawSpanGroup);
        final ArrayList<SpanGroup> actual = Whitebox.getInternalState(group, "spanGroups");
        assertEquals(1, actual.size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorThrowsWhenAllNull() {
        new SplitRollupSpanGroup(null, null);
    }

    @Test
    public void testMetricName() {
        when(rollupSpanGroup.metricName()).thenReturn("metric name");
        assertEquals("metric name", (new SplitRollupSpanGroup(rollupSpanGroup, rawSpanGroup)).metricName());
        verifyZeroInteractions(rawSpanGroup);
    }

    @Test
    public void testMetricUID() {
        when(rollupSpanGroup.metricUID()).thenReturn(new byte[]{0, 0, 1});
        assertArrayEquals(new byte[]{0, 0, 1}, (new SplitRollupSpanGroup(rollupSpanGroup, rawSpanGroup)).metricUID());
        verifyZeroInteractions(rawSpanGroup);
    }

    @Test
    public void testSize() {
        when(rollupSpanGroup.size()).thenReturn(5);
        when(rawSpanGroup.size()).thenReturn(7);
        assertEquals(12, (new SplitRollupSpanGroup(rollupSpanGroup, rawSpanGroup)).size());
    }

    @Test
    public void testAggregatedSize() {
        when(rollupSpanGroup.aggregatedSize()).thenReturn(5);
        when(rawSpanGroup.aggregatedSize()).thenReturn(7);
        assertEquals(12, (new SplitRollupSpanGroup(rollupSpanGroup, rawSpanGroup)).aggregatedSize());
    }

    @Test
    public void testGetTagUids() {
        final Bytes.ByteMap<byte[]> uids1 = new Bytes.ByteMap<byte[]>();
        uids1.put(new byte[]{0, 0, 1}, new byte[]{0, 0, 2});
        final Bytes.ByteMap<byte[]> uids2 = new Bytes.ByteMap<byte[]>();
        uids2.put(new byte[]{0, 0, 3}, new byte[]{0, 0, 4});

        when(rollupSpanGroup.getTagUids()).thenReturn(uids1);
        when(rawSpanGroup.getTagUids()).thenReturn(uids2);

        Bytes.ByteMap<byte[]> actual = (new SplitRollupSpanGroup(rollupSpanGroup, rawSpanGroup)).getTagUids();

        assertEquals(2, actual.size());
        assertArrayEquals(new byte[]{0, 0, 1}, actual.firstKey());
        assertArrayEquals(new byte[]{0, 0, 2}, actual.firstEntry().getValue());
        assertArrayEquals(new byte[]{0, 0, 3}, actual.lastKey());
        assertArrayEquals(new byte[]{0, 0, 4}, actual.lastEntry().getValue());
    }

    @Test
    public void testGetAggregatedTagUids() {
        final List<byte[]> uids1 = new ArrayList<byte[]>();
        uids1.add(new byte[]{0, 0, 1});
        final List<byte[]> uids2 = new ArrayList<byte[]>();
        uids2.add(new byte[]{0, 0, 2});

        when(rollupSpanGroup.getAggregatedTagUids()).thenReturn(uids1);
        when(rawSpanGroup.getAggregatedTagUids()).thenReturn(uids2);

        List<byte[]> actual = (new SplitRollupSpanGroup(rollupSpanGroup, rawSpanGroup)).getAggregatedTagUids();

        assertEquals(2, actual.size());
        assertArrayEquals(new byte[]{0, 0, 1}, actual.get(0));
        assertArrayEquals(new byte[]{0, 0, 2}, actual.get(1));
    }

    @Test
    public void testIterator() {
        SeekableView iterator = (new SplitRollupSpanGroup(rollupSpanGroup, rawSpanGroup)).iterator();
        int size = 0;
        while (iterator.hasNext()) {
            size++;
            iterator.next();
        }
        assertEquals(3, size);
    }

    @Test
    public void testTimestamp() {
        SplitRollupSpanGroup group = new SplitRollupSpanGroup(rollupSpanGroup, rawSpanGroup);
        assertEquals(START_TS, group.timestamp(0));
        assertEquals(SPLIT_TS, group.timestamp(1));
        assertEquals(END_TS, group.timestamp(2));
    }

    @Test
    public void testIsInteger() {
        assertTrue(new SplitRollupSpanGroup(rollupSpanGroup).isInteger(0));
    }

    @Test
    public void testLongValue() {
        assertEquals(0L, new SplitRollupSpanGroup(rollupSpanGroup).longValue(0));
    }

    @Test
    public void testDoubleValue() {
        doAnswer(new SeekableViewAnswer(START_TS, 100, 1, false)).when(rollupSpanGroup).iterator();
        assertEquals(0, new SplitRollupSpanGroup(rollupSpanGroup).doubleValue(0), 0.0);
    }

    @Test
    public void testGroup() {
        when(rollupSpanGroup.metricName()).thenReturn("group name");
        assertEquals("group name", (new SplitRollupSpanGroup(rollupSpanGroup, rawSpanGroup)).metricName());
        verifyZeroInteractions(rawSpanGroup);
    }

    static class SeekableViewAnswer implements Answer<SeekableView> {
        private final long timestamp;
        private final int samplePeriod;
        private final int numPoints;
        private final boolean isInteger;

        SeekableViewAnswer(long startTimestamp, int samplePeriod, int numPoints, boolean isInteger) {
            this.timestamp = startTimestamp;
            this.samplePeriod = samplePeriod;
            this.numPoints = numPoints;
            this.isInteger = isInteger;
        }

        @Override
        public SeekableView answer(InvocationOnMock ignored) {
            return SeekableViewsForTest.generator(timestamp, samplePeriod, numPoints, isInteger);
        }
    }
}

// This file is part of OpenTSDB.
// Copyright (C) 2012-2021  The OpenTSDB Authors.
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

import com.stumbleupon.async.Deferred;
import net.opentsdb.utils.DateTime;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({DateTime.class, TsdbQuery.class})
public class TestSplitRollupQuery extends BaseTsdbTest {
    private SplitRollupQuery queryUnderTest;
    private TsdbQuery rollupQuery;

    @Before
    public void beforeLocal() {
        rollupQuery = spy(new TsdbQuery(tsdb));
        queryUnderTest = new SplitRollupQuery(tsdb, rollupQuery, Deferred.fromResult(null));
    }

    @Test
    public void setStartTime() {
        queryUnderTest.setStartTime(42L);
        assertEquals(42000L, queryUnderTest.getStartTime());
        assertEquals(42000L, rollupQuery.getStartTime());
    }

    @Test
    public void setStartTimeBeyondOriginalEnd() {
        TsdbQuery rawQuery = new TsdbQuery(tsdb);
        Whitebox.setInternalState(queryUnderTest, "rawQuery", rawQuery);

        rollupQuery.setEndTime(41L);
        queryUnderTest.setStartTime(42L);

        assertEquals(42000L, queryUnderTest.getStartTime());
    }

    @Test
    public void setStartTimeWithoutRollupQuery() {
        TsdbQuery rawQuery = new TsdbQuery(tsdb);
        Whitebox.setInternalState(queryUnderTest, "rollupQuery", (TsdbQuery)null);
        Whitebox.setInternalState(queryUnderTest, "rawQuery", rawQuery);

        queryUnderTest.setStartTime(42L);

        assertEquals(42000L, queryUnderTest.getStartTime());
    }

    @Test
    public void setEndTime() {
        TsdbQuery rawQuery = new TsdbQuery(tsdb);
        Whitebox.setInternalState(queryUnderTest, "rawQuery", rawQuery);

        rollupQuery.setStartTime(0);
        rollupQuery.setEndTime(21000L);
        rawQuery.setStartTime(21000L);

        queryUnderTest.setEndTime(42L);

        assertEquals(42000L, queryUnderTest.getEndTime());
        assertEquals(21000L, rollupQuery.getEndTime());
        assertEquals(42000L, rawQuery.getEndTime());
    }

    @Test(expected = IllegalArgumentException.class)
    public void setEndTimeBeforeRawStartTime() {
        TsdbQuery rawQuery = new TsdbQuery(tsdb);
        rawQuery.setStartTime(DateTime.currentTimeMillis());

        Whitebox.setInternalState(queryUnderTest, "rawQuery", rawQuery);

        queryUnderTest.setEndTime(42L);
    }

    @Test
    public void setDeletePassesThroughToBoth() {
        TsdbQuery rawQuery = spy(new TsdbQuery(tsdb));
        Whitebox.setInternalState(queryUnderTest, "rawQuery", rawQuery);

        queryUnderTest.setDelete(true);

        verify(rollupQuery).setDelete(true);
        verify(rawQuery).setDelete(true);
    }

    @Test
    public void setTimeSeriesPassesThroughToBoth() {
        TsdbQuery rawQuery = spy(new TsdbQuery(tsdb));
        Whitebox.setInternalState(queryUnderTest, "rawQuery", rawQuery);

        RateOptions options = new RateOptions();

        queryUnderTest.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, false, options);

        verify(rollupQuery).setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, false, options);
        verify(rawQuery).setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, false, options);
    }

    @Test
    public void setTimeSeriesWithoutRateOptionsPassesThroughToBoth() {
        TsdbQuery rawQuery = spy(new TsdbQuery(tsdb));
        Whitebox.setInternalState(queryUnderTest, "rawQuery", rawQuery);

        queryUnderTest.setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, false);

        verify(rollupQuery).setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, false);
        verify(rawQuery).setTimeSeries(METRIC_STRING, tags, Aggregators.SUM, false);
    }

    @Test
    public void setTimeSeriesWithTSUIDsPassesThroughToBoth() {
        TsdbQuery rawQuery = spy(new TsdbQuery(tsdb));
        Whitebox.setInternalState(queryUnderTest, "rawQuery", rawQuery);

        List<String> tsuids = Arrays.asList("000001000001000001", "000001000001000002");
        RateOptions options = new RateOptions();

        queryUnderTest.setTimeSeries(tsuids, Aggregators.SUM, false, options);

        verify(rollupQuery).setTimeSeries(tsuids, Aggregators.SUM, false, options);
        verify(rawQuery).setTimeSeries(tsuids, Aggregators.SUM, false, options);
    }

    @Test
    public void setTimeSeriesWithTSUIDsWithoutRateOptionsPassesThroughToBoth() {
        TsdbQuery rawQuery = spy(new TsdbQuery(tsdb));
        Whitebox.setInternalState(queryUnderTest, "rawQuery", rawQuery);

        List<String> tsuids = Arrays.asList("000001000001000001", "000001000001000002");

        queryUnderTest.setTimeSeries(tsuids, Aggregators.SUM, false);

        verify(rollupQuery).setTimeSeries(tsuids, Aggregators.SUM, false);
        verify(rawQuery).setTimeSeries(tsuids, Aggregators.SUM, false);
    }

    @Test
    public void downsamplePassesThroughToBoth() {
        TsdbQuery rawQuery = spy(new TsdbQuery(tsdb));
        Whitebox.setInternalState(queryUnderTest, "rawQuery", rawQuery);

        queryUnderTest.downsample(42L, Aggregators.SUM, FillPolicy.ZERO);

        verify(rollupQuery).downsample(42L, Aggregators.SUM, FillPolicy.ZERO);
        verify(rawQuery).downsample(42L, Aggregators.SUM, FillPolicy.ZERO);
    }

    @Test
    public void downsampleWithoutFillPolicyPassesThroughToBoth() {
        TsdbQuery rawQuery = spy(new TsdbQuery(tsdb));
        Whitebox.setInternalState(queryUnderTest, "rawQuery", rawQuery);

        queryUnderTest.downsample(42L, Aggregators.SUM);

        verify(rollupQuery).downsample(42L, Aggregators.SUM);
        verify(rawQuery).downsample(42L, Aggregators.SUM);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void configureFromQueryThrowsIfForcedRaw() {
        queryUnderTest.configureFromQuery(null, 0, true);
    }

    @Test
    public void configureFromQuerySplitsRollupQuery() {
        mockEnableRollupQuerySplitting();
        doReturn(Deferred.fromResult(null)).when(rollupQuery).split(any(TSQuery.class), anyInt(), any(TsdbQuery.class));

        assertNull(Whitebox.getInternalState(queryUnderTest, "rawQuery"));

        rollupQuery.setStartTime(0);
        queryUnderTest.configureFromQuery(null, 0, false);

        verify(rollupQuery).split(eq((TSQuery) null), eq(0), any(TsdbQuery.class));
        assertNotNull(Whitebox.getInternalState(queryUnderTest, "rawQuery"));
    }

    @Test
    public void configureFromQuerySplitsRollupQueryWithRawOnlyQuery() {
        mockEnableRollupQuerySplitting();
        doReturn(true).when(rollupQuery).needsSplitting();
        doReturn(Deferred.fromResult(null)).when(rollupQuery).split(any(TSQuery.class), anyInt(), any(TsdbQuery.class));

        rollupQuery.setStartTime(DateTime.currentTimeMillis());

        assertNull(Whitebox.getInternalState(queryUnderTest, "rawQuery"));

        queryUnderTest.configureFromQuery(null, 0, false);

        verify(rollupQuery).split(eq((TSQuery) null), eq(0), any(TsdbQuery.class));
        assertNotNull(Whitebox.getInternalState(queryUnderTest, "rawQuery"));
        assertNull(Whitebox.getInternalState(queryUnderTest, "rollupQuery"));
    }

    @Test
    public void setPercentiles() {
        TsdbQuery rawQuery = spy(new TsdbQuery(tsdb));
        Whitebox.setInternalState(queryUnderTest, "rawQuery", rawQuery);

        List<Float> percentiles = Arrays.asList(50f, 75f, 99f, 99.9f);

        queryUnderTest.setPercentiles(percentiles);

        verify(rollupQuery).setPercentiles(percentiles);
        verify(rawQuery).setPercentiles(percentiles);
    }

    @Test
    public void run() {
        TsdbQuery rawQuery = spy(new TsdbQuery(tsdb));
        Whitebox.setInternalState(queryUnderTest, "rawQuery", rawQuery);

        doReturn(Deferred.fromResult(new DataPoints[0])).when(rollupQuery).runAsync();
        doReturn(Deferred.fromResult(new DataPoints[0])).when(rawQuery).runAsync();

        DataPoints[] actualPoints = queryUnderTest.run();

        verify(rollupQuery).runAsync();
        verify(rawQuery).runAsync();

        assertEquals(0, actualPoints.length);
    }

    @Test
    public void runHistogram() {
        TsdbQuery rawQuery = spy(new TsdbQuery(tsdb));
        Whitebox.setInternalState(queryUnderTest, "rawQuery", rawQuery);

        doReturn(true).when(rollupQuery).isHistogramQuery();
        doReturn(Deferred.fromResult(new DataPoints[0])).when(rollupQuery).runHistogramAsync();
        doReturn(true).when(rawQuery).isHistogramQuery();
        doReturn(Deferred.fromResult(new DataPoints[0])).when(rawQuery).runHistogramAsync();

        DataPoints[] actualPoints = queryUnderTest.runHistogram();

        verify(rollupQuery).runHistogramAsync();
        verify(rawQuery).runHistogramAsync();

        assertEquals(0, actualPoints.length);
    }

    @Test
    public void runAsyncMergesResults() {
        TsdbQuery rawQuery = spy(new TsdbQuery(tsdb));
        Whitebox.setInternalState(queryUnderTest, "rawQuery", rawQuery);

        rollupQuery.setStartTime(0);
        rollupQuery.setEndTime(21);
        rawQuery.setStartTime(21);
        rawQuery.setEndTime(42);

        DataPoints[] rollupDataPoints = new DataPoints[] {
                makeSpanGroup("group1"),
                makeSpanGroup("group2"),
                makeSpanGroup("group3"),
        };

        DataPoints[] rawDataPoints = new DataPoints[] {
                makeSpanGroup("group2"),
                makeSpanGroup("group3"),
                makeSpanGroup("group4"),
                makeSpanGroup("group5"),
        };

        doReturn(Deferred.fromResult(rollupDataPoints)).when(rollupQuery).runAsync();
        doReturn(Deferred.fromResult(rawDataPoints)).when(rawQuery).runAsync();

        DataPoints[] actualPoints = queryUnderTest.run();

        verify(rollupQuery).runAsync();
        verify(rawQuery).runAsync();

        List<String> actualGroups = new ArrayList<String>(actualPoints.length);
        for (DataPoints dataPoints : actualPoints) {
            actualGroups.add(new String(((SplitRollupSpanGroup) dataPoints).group()));
        }
        Collections.sort(actualGroups);

        assertEquals(Arrays.asList("group1", "group2", "group3", "group4", "group5"), actualGroups);
    }

    private SpanGroup makeSpanGroup(String group) {

        return new SpanGroup(
                tsdb,
                0,
                42,
                new ArrayList<Span>(),
                false,
                new RateOptions(),
                Aggregators.SUM,
                DownsamplingSpecification.NO_DOWNSAMPLER,
                0,
                42,
                0,
                null,
                group.getBytes()
        );
    }

    private void mockEnableRollupQuerySplitting() {
        Whitebox.setInternalState(tsdb, "rollups_split_queries", true);
        Whitebox.setInternalState(rollupQuery, "rollup_query", makeRollupQuery());
    }
}

// This file is part of OpenTSDB.
// Copyright (C) 2015-2017  The OpenTSDB Authors.
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
package net.opentsdb.rollup;

import net.opentsdb.core.Aggregators;
import net.opentsdb.utils.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
        DateTime.class
})
public class TestRollupQuery {

    private static final long MOCK_TIMESTAMP = 1554117071000L;
    private static final int ONE_HOUR_SECONDS = 60 * 60;
    private static final int ONE_DAY_SECONDS = 24 * ONE_HOUR_SECONDS;
    private static final int TWO_DAYS_SECONDS = 2 * ONE_DAY_SECONDS;

    private RollupQuery query;

    @Before
    public void before() {
        final RollupInterval oneHourWithDelay = RollupInterval.builder()
                .setTable("fake-rollup-table")
                .setPreAggregationTable("fake-preagg-table")
                .setInterval("1h")
                .setRowSpan("1d")
                .setDelaySla("2d")
                .build();
        query = new RollupQuery(
                oneHourWithDelay,
                Aggregators.SUM,
                3600000,
                Aggregators.SUM
        );
        PowerMockito.mockStatic(DateTime.class);
        PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(MOCK_TIMESTAMP);
    }


    @Test
    public void testGetLastRollupTimestamp() {
        long nowSeconds = MOCK_TIMESTAMP / 1000;
        long twoDaysAgo = nowSeconds - TWO_DAYS_SECONDS;

        assertEquals(twoDaysAgo, query.getLastRollupTimestampSeconds());
    }

    @Test
    public void testIsInBlackoutPeriod() {
        long oneHourAgo = MOCK_TIMESTAMP - ONE_HOUR_SECONDS * 1000;
        assertTrue(query.isInBlackoutPeriod(oneHourAgo));

        long threeDaysAgo = MOCK_TIMESTAMP - 3 * ONE_DAY_SECONDS * 1000;
        assertFalse(query.isInBlackoutPeriod(threeDaysAgo));
    }
}

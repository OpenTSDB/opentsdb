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

import com.google.common.collect.ImmutableMap;
import com.stumbleupon.async.Deferred;
import net.opentsdb.storage.MockBase;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.Config;
import org.hbase.async.HBaseClient;
import org.hbase.async.Scanner;
import org.jboss.netty.util.HashedWheelTimer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

/**
 * Sets up a real TSDB with mocked client, compaction queue and timer along
 * with mocked UID assignment, fetches for common unit tests.
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
    "ch.qos.*", "org.slf4j.*",
    "com.sum.*", "org.xml.*"})
@PrepareForTest({TSDB.class, Config.class, UniqueId.class, HBaseClient.class,
    HashedWheelTimer.class, Scanner.class, Const.class })
public class TestTsdbTSConfig {
    /** A list of UIDs from A to Z for unit testing UIDs values */
    public static final Map<String, byte[]> METRIC_UIDS =
        new HashMap<String, byte[]>(26);
    public static final Map<String, byte[]> TAGK_UIDS =
        new HashMap<String, byte[]>(26);
    public static final Map<String, byte[]> TAGV_UIDS =
        new HashMap<String, byte[]>(26);
    static {
        char letter = 'A';
        int uid = 10;
        for (int i = 0; i < 26; i++) {
            METRIC_UIDS.put(Character.toString(letter),
                UniqueId.longToUID(uid, TSDB.metrics_width()));
            TAGK_UIDS.put(Character.toString(letter),
                UniqueId.longToUID(uid, TSDB.tagk_width()));
            TAGV_UIDS.put(Character.toString(letter++),
                UniqueId.longToUID(uid++, TSDB.tagv_width()));
        }
    }

    public static final String METRIC_STRING = "sys.cpu.user";
    public static final byte[] METRIC_BYTES = new byte[] { 0, 0, 1 };

    public static final String TAGK_STRING = "host";
    public static final byte[] TAGK_BYTES = new byte[] { 0, 0, 1 };

    public static final String TAGV_STRING = "web01";
    public static final byte[] TAGV_BYTES = new byte[] { 0, 0, 1 };

    protected Config config;
    protected TSDB tsdb;
    protected HBaseClient client = mock(HBaseClient.class);
    protected UniqueId metrics = mock(UniqueId.class);
    protected UniqueId tag_names = mock(UniqueId.class);
    protected UniqueId tag_values = mock(UniqueId.class);
    protected Map<String, String> tags = new HashMap<String, String>(1);
    protected MockBase storage;

    public void before(Map<String, String> overrideConfigs) throws Exception {
        config = new Config(false);
        for (Map.Entry<String, String> entry : overrideConfigs.entrySet()) {
            config.overrideConfig(entry.getKey(), entry.getValue());
        }

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

    /** Adds the static UIDs to the metrics UID mock object */
    void setupMetricMaps() {
        when(metrics.getId(METRIC_STRING)).thenReturn(METRIC_BYTES);
        when(metrics.getIdAsync(METRIC_STRING))
            .thenAnswer(new Answer<Deferred<byte[]>>() {
                @Override
                public Deferred<byte[]> answer(InvocationOnMock invocation)
                    throws Throwable {
                    return Deferred.fromResult(METRIC_BYTES);
                }
            });
        when(metrics.getOrCreateId(METRIC_STRING))
            .thenReturn(METRIC_BYTES);
    }

    /** Adds the static UIDs to the tag keys UID mock object */
    void setupTagkMaps() {
        when(tag_names.getId(TAGK_STRING)).thenReturn(TAGK_BYTES);
        when(tag_names.getOrCreateId(TAGK_STRING)).thenReturn(TAGK_BYTES);
        when(tag_names.getIdAsync(TAGK_STRING))
            .thenAnswer(new Answer<Deferred<byte[]>>() {
                @Override
                public Deferred<byte[]> answer(InvocationOnMock invocation)
                    throws Throwable {
                    return Deferred.fromResult(TAGK_BYTES);
                }
            });
        when(tag_names.getOrCreateIdAsync(TAGK_STRING))
            .thenReturn(Deferred.fromResult(TAGK_BYTES));

    }

    /** Adds the static UIDs to the tag values UID mock object */
    void setupTagvMaps() {
        when(tag_values.getId(TAGV_STRING)).thenReturn(TAGV_BYTES);
        when(tag_values.getOrCreateId(TAGV_STRING)).thenReturn(TAGV_BYTES);
        when(tag_values.getIdAsync(TAGV_STRING))
            .thenAnswer(new Answer<Deferred<byte[]>>() {
                @Override
                public Deferred<byte[]> answer(InvocationOnMock invocation)
                    throws Throwable {
                    return Deferred.fromResult(TAGV_BYTES);
                }
            });
        when(tag_values.getOrCreateIdAsync(TAGV_STRING))
            .thenReturn(Deferred.fromResult(TAGV_BYTES));

    }

    @Test
    public void scannerTimestampsEqualToQueryTimestamps() throws Exception {
        before(ImmutableMap.of("tsd.storage.enable_compaction", "false"));
        TSQuery q = new TSQuery();
        q.setStart("1h-ago");

        TSSubQuery subQuery = new TSSubQuery();
        subQuery.setMetric(METRIC_STRING);
        subQuery.setAggregator("none");

        ArrayList<TSSubQuery> list = new ArrayList<TSSubQuery>();
        list.add(subQuery);
        q.setQueries(list);
        q.validateAndSetQuery();

        TsdbQuery query = new TsdbQuery(tsdb);
        query.configureFromQuery(q, 0);

        Scanner scanner = query.getScanner();
        long minTs = scanner.getMinTimestamp();
        long maxTs = scanner.getMaxTimestamp();
        // For 1h - ago, the TSDB will create a window of 2 hrs, For example if the current time is 2:45 PM, the window will be 1 PM to 3 PM
        assert((maxTs - minTs) == (2 * 3600000));
    }

    @Test
    public void scannerTimestampsAreOnlySetWhenDTCTimestampIsOlder() throws Exception {
        String now = String.valueOf(System.currentTimeMillis());
        before(ImmutableMap.of("tsd.storage.get_date_tiered_compaction_start", now));
        TSQuery q = new TSQuery();
        q.setStart("1h-ago");

        TSSubQuery subQuery = new TSSubQuery();
        subQuery.setMetric(METRIC_STRING);
        subQuery.setAggregator("none");

        ArrayList<TSSubQuery> list = new ArrayList<TSSubQuery>();
        list.add(subQuery);
        q.setQueries(list);
        q.validateAndSetQuery();

        TsdbQuery query = new TsdbQuery(tsdb);
        query.configureFromQuery(q, 0);

        Scanner scanner = query.getScanner();
        long minTs = scanner.getMinTimestamp();
        long maxTs = scanner.getMaxTimestamp();
        assertEquals(0L, minTs);
        assertEquals(Long.MAX_VALUE, maxTs);
    }

}
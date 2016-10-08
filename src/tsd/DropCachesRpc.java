// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
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
package net.opentsdb.tsd;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Atomics;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.tools.BuildData;
import net.opentsdb.core.Aggregators;
import net.opentsdb.core.TSDB;
import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.JSON;
import net.opentsdb.utils.PluginLoader;

import java.io.IOException;

/** The "dropcaches" command. */
public final class DropCachesRpc implements TelnetRpc, HttpRpc {
    private static final Logger LOG = LoggerFactory.getLogger(DropCachesRpc.class);

    public Deferred<Object> execute(final TSDB tsdb, final Channel chan,
                                    final String[] cmd) {
        dropCaches(tsdb, chan);
        chan.write("Caches dropped.\n");
        return Deferred.fromResult(null);
    }

    public void execute(final TSDB tsdb, final HttpQuery query)
            throws IOException {

        // only accept GET/DELETE
        RpcUtil.allowedMethods(query.method(), HttpMethod.GET.getName(), HttpMethod.DELETE.getName());

        dropCaches(tsdb, query.channel());

        if (query.apiVersion() > 0) {
            final HashMap<String, String> response = new HashMap<String, String>();
            response.put("status", "200");
            response.put("message", "Caches dropped");
            query.sendReply(query.serializer().formatDropCachesV1(response));
        } else { // deprecated API
            query.sendReply("Caches dropped.\n");
        }
    }

    /** Drops in memory caches.  */
    private void dropCaches(final TSDB tsdb, final Channel chan) {
        LOG.warn(chan + " Dropping all in-memory caches.");
        tsdb.dropCaches();
    }
}
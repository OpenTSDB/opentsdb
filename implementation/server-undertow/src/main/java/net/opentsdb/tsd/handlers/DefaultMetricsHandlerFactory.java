/*
 * This file is part of OpenTSDB.
 * Copyright (C) 2021  Yahoo.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.opentsdb.tsd.handlers;

import com.stumbleupon.async.Deferred;
import io.undertow.server.HttpHandler;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.tsd.MetricsHandler;

public class DefaultMetricsHandlerFactory extends BaseTSDBPlugin
    implements TSDBMetricsHandlerFactory {

  public static final String TYPE = DefaultMetricsHandlerFactory.class.getSimpleName();

  private StatsCollector statsCollector;

  public DefaultMetricsHandlerFactory() {}

  public DefaultMetricsHandlerFactory(StatsCollector statsCollector) {
    this.statsCollector = statsCollector;
  }

  @Override
  public String type() {
    return TYPE;
  }

  @Override
  public Deferred<Object> initialize(TSDB tsdb, String id) {
    this.statsCollector = tsdb.getStatsCollector();
    return super.initialize(tsdb, id);
  }

  @Override
  public HttpHandler getMetricsHandler(final HttpHandler next) {
    return new MetricsHandler(statsCollector, next);
  }
}

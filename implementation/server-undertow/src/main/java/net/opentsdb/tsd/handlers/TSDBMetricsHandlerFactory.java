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

import io.undertow.server.HttpHandler;
import net.opentsdb.core.TSDBPlugin;

/** Factory to build the metrics handler for the Undertow server. */
public interface TSDBMetricsHandlerFactory extends TSDBPlugin {

  /**
   * builds the metrics handler
   *
   * @param next next http handler
   * @return the newly built metrics handler
   */
  HttpHandler getMetricsHandler(final HttpHandler next);
}

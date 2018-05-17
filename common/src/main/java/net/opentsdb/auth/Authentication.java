// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.auth;

import net.opentsdb.core.TSDB;
import net.opentsdb.stats.StatsCollector;
import com.stumbleupon.async.Deferred;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.handler.codec.http.HttpRequest;

/**
 * A plugin interface for performing authentication for OpenTSDB API access.
 * The plugin is embedded within the Netty pipeline and intercepts requests
 * on new channels. Once a channel is authenticated successfully, the plugin is
 * removed from the pipeline so further calls on that channel are not evaluated.
 * <p>
 * An AuthState object is attached to the channel for evaluation later in the
 * pipeline. This state cannot be changed but cane be replaced.
 * <p>
 * The plugin also includes an acessor to an Authorization plugin to allow or 
 * disallow operations per user.  
 * 
 * @since 2.4
 */
public abstract class Authentication {

  /**
   * Called by TSDB to initialize the plugin
   * Implementations are responsible for setting up any IO they need as well
   * as starting any required background threads.
   * <b>Note:</b> Implementations should throw exceptions if they can't start
   * up properly. The TSD will then shutdown so the operator can fix the
   * problem. Please use IllegalArgumentException for configuration issues.
   * @param tsdb The parent TSDB object
   * @throws IllegalArgumentException if required configuration parameters are
   * missing
   * @throws RuntimeException if something else goes wrong
   */
  public abstract void initialize(final TSDB tsdb);

  /**
   * Called to gracefully shutdown the plugin. Implementations should close
   * any IO they have open
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has not special meaning and can be {@code null}
   * (think of it as {@code Deferred<Void>}).
   */
  public abstract Deferred<Object> shutdown();

  /**
   * Should return the version of this plugin in the format:
   * MAJOR.MINOR.MAINT, e.g. 2.0.1. The MAJOR version should match the major
   * version of OpenTSDB the plugin is meant to work with.
   * @return A version string used to log the loaded version
   */
  public abstract String version();

  /**
   * Called by the TSD when a request for statistics collection has come in. The
   * implementation may provide one or more statistics. If no statistics are
   * available for the implementation, simply stub the method.
   * @param collector The collector used for emitting statistics
   */
  public abstract void collectStats(final StatsCollector collector);

  /**
   * Authenticate Telnet connections, provides the first line of the incoming
   * connection.
   * <p>
   * NOTE: This method should not throw exceptions, rather return a state object
   * with the AuthStatus.ERROR status.
   * 
   * @param channel A non-null Netty channel to associate with the request.
   * @param command A non-null list of "words" from a Telnet style command 
   * (strings or numbers separated by spaces)
   * @return A non-null AuthState object with a valid AuthStatus to evaluate for
   * a successful or unsuccessful authentication.
   */
  public abstract AuthState authenticateTelnet(final Channel channel, 
      final String[] command);

  /**
   * Authenticate HTTP connections, provides the HTTPRequest object for the
   * incoming connection.
   * <p>
   * NOTE: This method should not throw exceptions, rather return a state object
   * with the AuthStatus.ERROR status.
   * 
   * @param channel A non-null Netty channel to associate with the request.
   * @param req A non-null HTTP request.
   * @return A non-null AuthState object with a valid AuthStatus to evaluate for
   * a successful or unsuccessful authentication.
   */
  public abstract AuthState authenticateHTTP(final Channel channel,
      final HttpRequest req);
  
  /**
   * An optional authorization object. If authorization is not enabled, this 
   * call may return null.
   * @return An authorization object or null;
   */
  public abstract Authorization authorization();
  
}
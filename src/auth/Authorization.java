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

import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSQuery;
import net.opentsdb.query.pojo.Query;
import net.opentsdb.stats.StatsCollector;

import java.util.EnumSet;

/**
 * A plugin interface for authorization calls, allowing or disallowing operations
 * in OpenTSDB.
 * 
 * @since 2.4
 */
public interface Authorization {

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
   * Determines if the user has a specified permission
   * @param state
   * @param permission
   * @return
   */
  public abstract AuthState hasPermission(final AuthState state, final Permissions permission);

  /**
   * Determines if the user is allowed to execute the given query.
   * The returned state contains a status code regarding whether or not the query
   * is allowed. If the query IS allowed, the same user state passed as an 
   * argument maybe returned.
   * @param state A non-null auth state with the user and AuthStatus.SUCCESS.
   * @param query A non-null query.
   * @return An AuthState object with a valid AuthStatus to evaluate for 
   * permission.
   */
  public abstract AuthState allowQuery(final AuthState state, 
      final TSQuery query);
  
  /**
   * Determines if the user is allowed to execute the given query.
   * The returned state contains a status code regarding whether or not the query
   * is allowed. If the query IS allowed, the same user state passed as an 
   * argument maybe returned.
   * @param state A non-null auth state with the user and AuthStatus.SUCCESS.
   * @param query A non-null query.
   * @return An AuthState object with a valid AuthStatus to evaluate for 
   * permission.
   */
  public abstract AuthState allowQuery(final AuthState state, 
      final Query query);
}

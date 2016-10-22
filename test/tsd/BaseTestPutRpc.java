// This file is part of OpenTSDB.
// Copyright (C) 2016  The OpenTSDB Authors.
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

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.atomic.AtomicLong;

import org.hbase.async.HBaseClient;
import org.hbase.async.PleaseThrottleException;
import org.hbase.async.Scanner;
import org.jboss.netty.util.HashedWheelTimer;
import org.junit.Before;
import org.junit.Ignore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.reflect.Whitebox;

import com.stumbleupon.async.TimeoutException;

import net.opentsdb.core.TSDB;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.core.BaseTsdbTest;
import net.opentsdb.core.Const;
import net.opentsdb.core.IncomingDataPoint;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.Threads;

@Ignore
@PrepareForTest({ TSDB.class, Config.class, HttpQuery.class, UniqueId.class, 
  HBaseClient.class, HashedWheelTimer.class, Scanner.class, Const.class, Threads.class,
  TimeoutException.class, StorageExceptionHandler.class, PutDataPointRpc.class,
  PleaseThrottleException.class, RollupDataPointRpc.class })
public class BaseTestPutRpc extends BaseTsdbTest {
  protected AtomicLong telnet_requests = new AtomicLong();
  protected AtomicLong http_requests = new AtomicLong();
  protected AtomicLong raw_dps = new AtomicLong();
  protected AtomicLong rollup_dps = new AtomicLong();
  protected AtomicLong raw_stored = new AtomicLong();
  protected AtomicLong rollup_stored = new AtomicLong();
  protected AtomicLong hbase_errors = new AtomicLong();
  protected AtomicLong unknown_errors = new AtomicLong();
  protected AtomicLong invalid_values = new AtomicLong();
  protected AtomicLong illegal_arguments = new AtomicLong();
  protected AtomicLong unknown_metrics = new AtomicLong();
  protected AtomicLong inflight_exceeded = new AtomicLong();
  protected AtomicLong writes_blocked = new AtomicLong();
  protected AtomicLong writes_timedout = new AtomicLong();
  protected AtomicLong requests_timedout = new AtomicLong();
  protected StorageExceptionHandler handler;
  
  @Before
  public void beforeCounters() throws Exception {
    telnet_requests = Whitebox.getInternalState(PutDataPointRpc.class, "telnet_requests");
    telnet_requests.set(0);
    http_requests = Whitebox.getInternalState(PutDataPointRpc.class, "http_requests");
    http_requests.set(0);
    raw_dps = Whitebox.getInternalState(PutDataPointRpc.class, "raw_dps");
    raw_dps.set(0);
    rollup_dps = Whitebox.getInternalState(PutDataPointRpc.class, "rollup_dps");
    rollup_dps.set(0);
    hbase_errors = Whitebox.getInternalState(PutDataPointRpc.class, "hbase_errors");
    hbase_errors.set(0);
    unknown_errors = Whitebox.getInternalState(PutDataPointRpc.class, "unknown_errors");
    unknown_errors.set(0);
    raw_stored = Whitebox.getInternalState(PutDataPointRpc.class, "raw_stored");
    raw_stored.set(0);
    rollup_stored = Whitebox.getInternalState(PutDataPointRpc.class, "rollup_stored");
    rollup_stored.set(0);
    invalid_values = Whitebox.getInternalState(PutDataPointRpc.class, "invalid_values");
    invalid_values.set(0);
    illegal_arguments = Whitebox.getInternalState(PutDataPointRpc.class, "illegal_arguments");
    illegal_arguments.set(0);
    unknown_metrics = Whitebox.getInternalState(PutDataPointRpc.class, "unknown_metrics");
    unknown_metrics.set(0);
    inflight_exceeded = Whitebox.getInternalState(PutDataPointRpc.class, "inflight_exceeded");
    inflight_exceeded.set(0);
    writes_blocked = Whitebox.getInternalState(PutDataPointRpc.class, "writes_blocked");
    writes_blocked.set(0);
    writes_timedout = Whitebox.getInternalState(PutDataPointRpc.class, "writes_timedout");
    writes_timedout.set(0);
    requests_timedout = Whitebox.getInternalState(PutDataPointRpc.class, "requests_timedout");
    requests_timedout.set(0);
  }
  
  /**
   * Helper to set the storage exception handler in the TSDB under test.
   */
  protected void setStorageExceptionHandler() {
    handler = mock(StorageExceptionHandler.class);
    Whitebox.setInternalState(tsdb, "storage_exception_handler", handler);
  }
  
  /**
   * Helper to validate calls to the storage exception handler.
   * @param called Whether or not SEH should have been called.
   */
  protected void validateSEH(final boolean called) {
    if (called) {
      verify(tsdb, times(1)).getStorageExceptionHandler();
      if (handler != null) {
        verify(handler, times(1)).handleError((IncomingDataPoint)any(), 
            (Exception)any());
      }
    } else {
      verify(tsdb, never()).getStorageExceptionHandler();
      if (handler != null) {
        verify(handler, never()).handleError((IncomingDataPoint)any(), 
            (Exception)any());
      }
    }
  }
  
  // Helper to validate all the counters per call.
  protected void validateCounters(
      final long telnet_requests,
      final long http_requests,
      final long raw_dps,
      final long rollup_dps,
      final long raw_stored,
      final long rollup_stored,
      final long hbase_errors,
      final long unknown_errors,
      final long invalid_values,
      final long illegal_arguments,
      final long unknown_metrics,
      final long inflight_exceeded,
      final long writes_blocked,
      final long writes_timedout,
      final long requests_timedout) {
    assertEquals(telnet_requests, this.telnet_requests.get());
    assertEquals(http_requests, this.http_requests.get());
    assertEquals(raw_dps, this.raw_dps.get());
    assertEquals(rollup_dps, this.rollup_dps.get());
    assertEquals(raw_stored, this.raw_stored.get());
    assertEquals(rollup_stored, this.rollup_stored.get());
    assertEquals(hbase_errors, this.hbase_errors.get());
    assertEquals(unknown_errors, this.unknown_errors.get());
    assertEquals(invalid_values, this.invalid_values.get());
    assertEquals(illegal_arguments, this.illegal_arguments.get());
    assertEquals(unknown_metrics, this.unknown_metrics.get());
    assertEquals(inflight_exceeded, this.inflight_exceeded.get());
    assertEquals(writes_blocked, this.writes_blocked.get());
    assertEquals(writes_timedout, this.writes_timedout.get());
    assertEquals(requests_timedout, this.requests_timedout.get());
  }
}

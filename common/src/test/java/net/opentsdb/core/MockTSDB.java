// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.core;

import static org.mockito.Mockito.mock;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.configuration.UnitTestConfiguration;
import net.opentsdb.stats.BlackholeStatsCollector;
import net.opentsdb.stats.StatsCollector;

/**
 * Class for unit testing.
 */
public class MockTSDB implements TSDB {
  public UnitTestConfiguration config;
  public Registry registry;
  public BlackholeStatsCollector stats;
  public FakeTaskTimer timer;
  
  public MockTSDB() {
    config = (UnitTestConfiguration) UnitTestConfiguration.getConfiguration();
    registry = mock(Registry.class);
    stats = new BlackholeStatsCollector();
    timer = new FakeTaskTimer();
    timer.multi_task = true;
  }
  
  @Override
  public Configuration getConfig() {
    return config;
  }

  @Override
  public Registry getRegistry() {
    return registry;
  }

  @Override
  public StatsCollector getStatsCollector() {
    return stats;
  }

  @Override
  public Timer getMaintenanceTimer() {
    return timer;
  }
  
  public static final class FakeTaskTimer extends HashedWheelTimer {
    public boolean multi_task;
    public TimerTask newPausedTask = null;
    public TimerTask pausedTask = null;
    public Timeout timeout = null;

    @Override
    public synchronized Timeout newTimeout(final TimerTask task,
                                           final long delay,
                                           final TimeUnit unit) {
      if (pausedTask == null) {
        pausedTask = task;
      }  else if (newPausedTask == null) {
        newPausedTask = task;
      } else if (!multi_task) {
        throw new IllegalStateException("Cannot Pause Two Timer Tasks");
      }
      timeout = mock(Timeout.class);
      return timeout;
    }

    @Override
    public Set<Timeout> stop() {
      return null;
    }

    public boolean continuePausedTask() {
      if (pausedTask == null) {
        return false;
      }
      try {
        if (!multi_task && newPausedTask != null) {
          throw new IllegalStateException("Cannot be in this state");
        }
        pausedTask.run(null);  // Argument never used in this code base
        pausedTask = newPausedTask;
        newPausedTask = null;
        return true;
      } catch (Exception e) {
        throw new RuntimeException("Timer task failed: " + pausedTask, e);
      }
    }
  }


}

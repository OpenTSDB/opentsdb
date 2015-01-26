package net.opentsdb.stats;

import java.util.List;

import net.opentsdb.core.IncomingDataPoint;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Reporter;
import com.google.common.collect.ImmutableList;

/**
 * See net.opentsdb.tsd.StatsRpc.SerializerCollector in 0fffa11cd9dffe6605597c0fbbe8ecb7167a3b01.
 *
 * TODO
 * We have to decide what to do with this class. It might be nicer with a JMX
 * reporter? In any case the metrics library has a JSON reporter that uses
 * jackson, we just have to add that module.
 */
public class SerializerReporter implements Reporter {
  public SerializerReporter(final MetricRegistry registry) {
  }

  public List<IncomingDataPoint> report() {
    return ImmutableList.of();
  }
}

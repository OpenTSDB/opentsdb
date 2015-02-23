package net.opentsdb.stats;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Reporter;

/**
 * See net.opentsdb.tsd.StatsRpc.ASCIICollector in 0fffa11cd9dffe6605597c0fbbe8ecb7167a3b01.
 *
 * TODO
 * We have to decide what to do with this class. It might be nicer with a JMX
 * reporter? In any case the metrics library has a JSON reporter that uses
 * jackson, we just have to add that module.
 */
public class JsonReporter implements Reporter {
  public JsonReporter(final MetricRegistry registry) {
  }

  public byte[] report() {
    return new byte[] {};
  }
}

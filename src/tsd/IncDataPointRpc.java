package net.opentsdb.tsd;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

import net.opentsdb.core.TSDB;
import net.opentsdb.core.Tags;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.uid.NoSuchUniqueName;

import org.jboss.netty.channel.Channel;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

/**
 * Increment/add datapoint rpc.
 *
 */
public class IncDataPointRpc implements TelnetRpc {

	private static final AtomicLong requests = new AtomicLong();
	private static final AtomicLong hbase_errors = new AtomicLong();
	private static final AtomicLong invalid_values = new AtomicLong();
	private static final AtomicLong illegal_arguments = new AtomicLong();
	private static final AtomicLong unknown_metrics = new AtomicLong();

	@Override
	public Deferred<? extends Object> execute(final TSDB tsdb, final Channel chan,
			final String[] cmd) {

		requests.incrementAndGet();
		String errmsg = null;
		try {
			final class PutErrback implements Callback<Exception, Exception> {
				public Exception call(final Exception arg) {
					if (chan.isConnected()) {
						chan.write("inc: HBase error: " + arg.getMessage()
								+ '\n');
					}
					hbase_errors.incrementAndGet();
					return arg;
				}

				public String toString() {
					return "report error to channel";
				}
			}
			return importDataPoint(tsdb, cmd).addErrback(new PutErrback());
		} catch (NumberFormatException x) {
			errmsg = "inc: invalid value: " + x.getMessage() + '\n';
			invalid_values.incrementAndGet();
		} catch (IllegalArgumentException x) {
			errmsg = "inc: illegal argument: " + x.getMessage() + '\n';
			illegal_arguments.incrementAndGet();
		} catch (NoSuchUniqueName x) {
			errmsg = "inc: unknown metric: " + x.getMessage() + '\n';
			unknown_metrics.incrementAndGet();
		}
		if (errmsg != null && chan.isConnected()) {
			chan.write(errmsg);
		}
		return Deferred.fromResult(null);

	}

	/**
	 * Collects the stats and metrics tracked by this instance.
	 * 
	 * @param collector
	 *            The collector to use.
	 */
	public static void collectStats(final StatsCollector collector) {
		collector.record("rpc.received", requests, "type=inc");
		collector.record("rpc.errors", hbase_errors, "type=hbase_errors");
		collector.record("rpc.errors", invalid_values, "type=invalid_values");
		collector.record("rpc.errors", illegal_arguments,
				"type=illegal_arguments");
		collector.record("rpc.errors", unknown_metrics, "type=unknown_metrics");
	}

	/**
	 * Increments a single data point.
	 * 
	 * @param tsdb
	 *            The TSDB to increment the data point into.
	 * @param words
	 *            The words describing the data point to import, in the
	 *            following format: {@code [metric, timestamp, value, ..tags..]}
	 * @return A deferred Long that indicates the completion of the request.
	 * @throws NumberFormatException
	 *             if the timestamp or value is invalid.
	 * @throws IllegalArgumentException
	 *             if any other argument is invalid.
	 * @throws net.opentsdb.uid.NoSuchUniqueName
	 *             if the metric isn't registered.
	 */
	private Deferred<? extends Object> importDataPoint(final TSDB tsdb,
			final String[] words) {
		
		words[0] = null; // Ditch the "inc".
		
		
		if (words.length < 5) { // Need at least: metric timestamp value tag
			// ^ 5 and not 4 because words[0] is "inc".
			throw new IllegalArgumentException("not enough arguments"
					+ " (need least 4, got " + (words.length - 1) + ')');
		}
		final String metric = words[1];
		if (metric.length() <= 0) {
			throw new IllegalArgumentException("empty metric name");
		}
		final long timestamp = Tags.parseLong(words[2]);
		if (timestamp <= 0) {
			throw new IllegalArgumentException("invalid timestamp: "
					+ timestamp);
		}
		final String value = words[3];
		if (value.length() <= 0) {
			throw new IllegalArgumentException("empty value");
		}
		final HashMap<String, String> tags = new HashMap<String, String>();
		for (int i = 4; i < words.length; i++) {
			if (!words[i].isEmpty()) {
				Tags.parse(tags, words[i]);
			}
		}
		if (value.indexOf('.') < 0) { // integer value
			return tsdb.incPoint(metric, timestamp, Tags.parseLong(value), tags);
		} else { // floating point value
			throw new IllegalArgumentException("Submitted value for increment does not work: " + timestamp);
		}
	}

}


package net.opentsdb.plugins;

import net.opentsdb.meta.Annotation;
import net.opentsdb.uid.TimeseriesId;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.Map;

/**
 * A class that describes the interface that is used to emit data points and annotations in
 * real-time to a backing sink.
 */
public abstract class RTPublisher extends Plugin {
  /**
   * Publish the long data point to the backing sink.
   *
   * @param metric The name of the metric associated with the data point
   * @param timestamp Timestamp as a POSIX time in milliseconds
   * @param value Value for the data point
   * @param tags Tag key/value pairs
   * @param timeSeriesId The time series ID of the data point
   * @return A future that indicates the completion of the call
   */
  public abstract ListenableFuture<Void> publishDataPoint(final String metric,
                                                          final long timestamp,
                                                          final long value,
                                                          final Map<String, String> tags,
                                                          final TimeseriesId timeSeriesId);

  /**
   * Publish the float data point to the backing sink.
   *
   * @param metric The name of the metric associated with the data point
   * @param timestamp Timestamp as a POSIX time in milliseconds
   * @param value Value for the data point
   * @param tags Tag key/value pairs
   * @param timeSeriesId The time series ID of the data point
   * @return A future that indicates the completion of the call
   */
  public abstract ListenableFuture<Void> publishDataPoint(final String metric,
                                                          final long timestamp,
                                                          final double value,
                                                          final Map<String, String> tags,
                                                          final TimeseriesId timeSeriesId);

  /**
   * Publish the provided annotation to the backing sink.
   *
   * @param annotation The published annotation
   * @return A future that indicates the completion of the call
   */
  public abstract ListenableFuture<Void> publishAnnotation(Annotation annotation);
}

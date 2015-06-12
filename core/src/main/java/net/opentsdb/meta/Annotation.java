
package net.opentsdb.meta;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import com.google.common.base.Strings;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import javax.annotation.Nonnull;

/**
 * Annotations record user-defined arbitrary messages about specified periods of time.
 *
 * <p>Annotations must be associated with a time series and have a start time as well as a message.
 * They may also have an end time and zero or more user-defined properties which are simple string
 * based key value pairs.
 *
 * <p>The start time must be larger than 0. The end time must be larger than zero or equal to {@link
 * #NOT_ENDED}. An annotation with an end time that is equal to {@link #NOT_ENDED} will be
 * interpreted as not having ended yet.
 */
@AutoValue
public abstract class Annotation implements Comparable<Annotation> {
  public static final long NOT_ENDED = -1;

  /**
   * Create a new annotation instance with the provided information.
   */
  public static Annotation create(@Nonnull final String timeSeriesId,
                                  final long startTime,
                                  final long endTime,
                                  @Nonnull final String message,
                                  @Nonnull final Map<String, String> properties) {
    checkArgument(!Strings.isNullOrEmpty(timeSeriesId));
    checkArgument(startTime > 0, "Start time must but larger than 0 but was %s", startTime);
    checkArgument(endTime > 0 || endTime == NOT_ENDED,
        "End time must be larger than 0 or equal to Annotation.END_TIME but was %s", endTime);
    checkArgument(startTime <= endTime);
    final ImmutableMap<String, String> immutableProperties = ImmutableMap.copyOf(properties);
    return new AutoValue_Annotation(timeSeriesId, startTime, endTime, message, immutableProperties);
  }

  /**
   * Create a new annotation instance with the provided information and an empty set of properties.
   */
  public static Annotation create(@Nonnull final String timeSeriesId,
                                  final long startTime,
                                  final long endTime,
                                  @Nonnull final String message) {
    return create(timeSeriesId, startTime, endTime, message, ImmutableMap.<String, String>of());
  }

  /**
   * Hide the constructor and prevent subclasses other than the one provided by {@link AutoValue}.
   */
  Annotation() {
  }

  /** The time series this annotation belongs to. */
  @Nonnull
  public abstract String timeSeriesId();

  /** A timestamp indicating at which point this this annotation became relevant. */
  public abstract long startTime();

  /** A timestamp indicating at which point this annotation was no longer relevant. */
  public abstract long endTime();

  /** A user-defined arbitrary message. */
  @Nonnull
  public abstract String message();

  /** A map of user-defined arbitrary keys. */
  @Nonnull
  public abstract ImmutableMap<String, String> properties();

  /**
   * Compares the {@code #startTime} of this annotation to the given note.
   *
   * @return 1 if the local start time is greater, -1 if it's less or 0 if equal
   */
  @Override
  public int compareTo(@Nonnull Annotation note) {
    return ComparisonChain.start()
        .compare(startTime(), note.startTime())
        .compare(endTime(), note.endTime())
        .result();
  }
}

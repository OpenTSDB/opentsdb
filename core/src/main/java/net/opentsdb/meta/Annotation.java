
package net.opentsdb.meta;

import static com.google.common.base.Preconditions.checkArgument;

import net.opentsdb.uid.LabelId;

import com.google.auto.value.AutoValue;
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
  public static final long NOT_ENDED = -1L;

  /**
   * Create a new annotation instance with the provided information.
   *
   * {@link ImmutableMap Immutable} copies will be made of both the provided tags and properties
   * maps. Because of this it is highly recommended to use them from the start since Guava can do
   * some optimizations in that case.
   */
  public static Annotation create(final LabelId metric,
                                  final Map<LabelId, LabelId> tags,
                                  final long startTime,
                                  final long endTime,
                                  final String message,
                                  final Map<String, String> properties) {
    checkArgument(!tags.isEmpty(), "An annotation must be associated with at least one tag");
    checkArgument(startTime > 0L, "Start time must but larger than 0 but was %s", startTime);
    checkArgument(endTime > 0L || endTime == NOT_ENDED,
        "End time must be larger than 0 or equal to Annotation.END_TIME but was %s", endTime);
    checkArgument(startTime <= endTime);
    final ImmutableMap<LabelId, LabelId> immutableTags = ImmutableMap.copyOf(tags);
    final ImmutableMap<String, String> immutableProperties = ImmutableMap.copyOf(properties);
    return new AutoValue_Annotation(metric, immutableTags, startTime, endTime, message,
        immutableProperties);
  }

  /**
   * Create a new annotation instance with the provided information and an empty set of properties.
   */
  public static Annotation create(final LabelId metric,
                                  final ImmutableMap<LabelId, LabelId> tags,
                                  final long startTime,
                                  final long endTime,
                                  final String message) {
    return create(metric, tags, startTime, endTime, message, ImmutableMap.<String, String>of());
  }

  /**
   * Hide the constructor and prevent subclasses other than the one provided by {@link AutoValue}.
   */
  Annotation() {
  }

  /** The metric associated with this annotation. */
  @Nonnull
  public abstract LabelId metric();

  /** The tags associated with this annotation. */
  @Nonnull
  public abstract ImmutableMap<LabelId, LabelId> tags();

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
  public int compareTo(Annotation note) {
    return ComparisonChain.start()
        .compare(startTime(), note.startTime())
        .compare(endTime(), note.endTime())
        .result();
  }
}

package net.opentsdb.meta;

import static com.google.common.base.Preconditions.checkArgument;

import net.opentsdb.uid.LabelId;
import net.opentsdb.uid.IdType;

import com.google.auto.value.AutoValue;

/**
 * LabelMeta objects are associated with individual labels. LabelMeta objects are generated at the
 * same time as the identifier they are associated with.
 *
 * <p>A LabelMeta object is identified by it's {@code identifier} and {@code type}.
 *
 * <p>None of the fields may be empty or null.
 */
@AutoValue
public abstract class LabelMeta {
  /**
   * Create an instance with the provided information.
   */
  public static LabelMeta create(final LabelId identifier,
                                 final IdType type,
                                 final String name,
                                 final String description,
                                 final long created) {
    checkArgument(!name.isEmpty(), "Name may not be empty");
    checkArgument(!description.isEmpty(), "Description may not be empty");
    return new AutoValue_LabelMeta(identifier, type, name, description, created);
  }

  /** The id of this label. */
  public abstract LabelId identifier();

  /** What type of label this is. */
  public abstract IdType type();

  /** The name of the label. */
  public abstract String name();

  /** A free-form description of what this label represents. */
  public abstract String description();

  /** The timestamp in milliseconds at which this label was created. */
  public abstract long created();
}


package net.opentsdb.meta;

import com.google.auto.value.AutoValue;

import net.opentsdb.uid.UniqueIdType;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * LabelMeta objects are associated with individual labels. LabelMeta objects are
 * generated at the same time as the identifier they are associated with.
 *
 * A LabelMeta object is identified by it's {@code uid} and {@code type}.
 *
 * None of the fields may be empty or null.
 */
@AutoValue
public abstract class LabelMeta {
  public static LabelMeta create(final byte[] uid,
                               final UniqueIdType type,
                               final String name,
                               final String description,
                               final long created) {
    checkArgument(type.width == uid.length, "UID length must match the UID type width");
    checkArgument(!name.isEmpty(), "Name may not be empty");
    checkArgument(!description.isEmpty(), "Description may not be empty");
    return new AutoValue_LabelMeta(uid, type, name, description, created);
  }

  /** The id of this label */
  public abstract byte[] uid();

  /** What type of label this is */
  public abstract UniqueIdType type();

  /** The name of the label */
  public abstract String name();

  /** A free-form description of what this label represents */
  public abstract String description();

  /** The timestamp in milliseconds at which this label was created */
  public abstract long created();
}

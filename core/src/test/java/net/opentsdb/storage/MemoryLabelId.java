package net.opentsdb.storage;

import static com.google.common.base.Preconditions.checkNotNull;

import net.opentsdb.uid.LabelId;

import java.util.UUID;
import javax.annotation.Nonnull;

/**
 * The LabelId that is used by the {@link MemoryStore}.
 */
public class MemoryLabelId implements LabelId<MemoryLabelId> {
  private final UUID uuid;

  public MemoryLabelId() {
    this(UUID.randomUUID());
  }

  public MemoryLabelId(@Nonnull final UUID uuid) {
    this.uuid = checkNotNull(uuid);
  }

  @Override
  public int compareTo(@Nonnull final MemoryLabelId other) {
    return uuid.compareTo(other.uuid);
  }

  @Override
  public boolean equals(final Object that) {
    if (that == this) {
      return true;
    }

    if (that instanceof MemoryLabelId) {
      return ((MemoryLabelId) that).uuid.equals(uuid);
    }

    return false;
  }

  @Override
  public int hashCode() {
    return uuid.hashCode();
  }

  static class MemoryLabelIdSerializer implements LabelId.LabelIdSerializer<MemoryLabelId> {
    @Nonnull
    @Override
    public String serialize(@Nonnull final MemoryLabelId identifier) {
      return identifier.uuid.toString();
    }
  }

  static class MemoryLabelIdDeserializer implements LabelId.LabelIdDeserializer<MemoryLabelId> {
    @Nonnull
    @Override
    public MemoryLabelId deserialize(@Nonnull final String stringIdentifier) {
      return new MemoryLabelId(UUID.fromString(stringIdentifier));
    }
  }
}

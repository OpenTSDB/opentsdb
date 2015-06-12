package net.opentsdb.storage.cassandra;

import net.opentsdb.uid.LabelId;

import com.google.common.base.MoreObjects;
import com.google.common.primitives.Longs;

import java.util.Objects;
import java.util.UUID;
import javax.annotation.Nonnull;

class CassandraLabelId implements LabelId<CassandraLabelId> {
  private final long id;

  CassandraLabelId(final long id) {
    this.id = id;
  }

  static LabelId fromLong(final long id) {
    return new CassandraLabelId(id);
  }

  static long toLong(final LabelId labelId) {
    return ((CassandraLabelId) labelId).id;
  }

  long toLong() {
    return id;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("id", id)
        .toString();
  }

  @Override
  public int hashCode() {
    return Longs.hashCode(id);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final CassandraLabelId other = (CassandraLabelId) obj;
    return this.id == other.id;
  }

  @Override
  public int compareTo(@Nonnull final CassandraLabelId that) {
    return Long.compare(id, that.id);
  }

  static class CassandraLabelIdSerializer implements LabelId.LabelIdSerializer<CassandraLabelId> {
    @Nonnull
    @Override
    public String serialize(@Nonnull final CassandraLabelId identifier) {
      return Long.toString(identifier.id);
    }
  }

  static class CassandraLabelIdDeserializer implements LabelId.LabelIdDeserializer<CassandraLabelId> {
    @Nonnull
    @Override
    public CassandraLabelId deserialize(@Nonnull final String stringIdentifier) {
      return new CassandraLabelId(Long.parseLong(stringIdentifier));
    }
  }
}

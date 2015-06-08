package net.opentsdb.storage.cassandra;

import com.google.common.base.MoreObjects;
import net.opentsdb.uid.LabelId;

import javax.annotation.Nonnull;
import java.util.Objects;

class CassandraLabelId implements LabelId<CassandraLabelId> {
  private final long id;

  static LabelId fromLong(final long id) {
    return new CassandraLabelId(id);
  }

  static long toLong(final LabelId labelId) {
    return ((CassandraLabelId) labelId).id;
  }

  CassandraLabelId(final long id) {
    this.id = id;
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
    return Objects.hash(id);
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
}

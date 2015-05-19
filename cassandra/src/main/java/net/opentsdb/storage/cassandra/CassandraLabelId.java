package net.opentsdb.storage.cassandra;

import com.google.common.primitives.Longs;
import net.opentsdb.uid.LabelId;

import javax.annotation.Nonnull;

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

  @Override
  public byte[] bytes() {
    return Longs.toByteArray(id);
  }

  long toLong() {
    return id;
  }

  @Override
  public int compareTo(@Nonnull final CassandraLabelId that) {
    return Long.compare(id, that.id);
  }
}

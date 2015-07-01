package net.opentsdb.uid;

import javax.annotation.Nonnull;

/**
 * A general interface that represents the full conceptual label (it contains the id, type and name
 * together).
 */
public interface Label {
  @Nonnull
  LabelId id();

  @Nonnull
  LabelType type();

  @Nonnull
  String name();
}

package net.opentsdb.uid;

import javax.annotation.Nonnull;

public interface LabelId<K extends LabelId> extends Comparable<K> {

  /**
   * an interface whose implementations should be able to serialize the corresponding label id
   * implementations.
   *
   * <p>If it receives an object that it is not capable of serializing then it should throw an
   * {@link IllegalArgumentException}.
   */
  interface LabelIdSerializer<K extends LabelId> {
    @Nonnull
    String serialize(@Nonnull final K identifier);
  }

  /**
   * an interface whose implementations should be able to deserialize the corresponding label id
   * implementations.
   *
   * <p>If it receives an object that it is not capable of serializing then it should throw an
   * {@link IllegalArgumentException}.
   */
  interface LabelIdDeserializer<K extends LabelId> {
    @Nonnull
    K deserialize(@Nonnull final String stringIdentifier);
  }
}

package net.opentsdb.uid;

public interface LabelId<K extends LabelId> extends Comparable<K> {
  @Deprecated
  byte[] bytes();
}

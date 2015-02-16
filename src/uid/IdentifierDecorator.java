package net.opentsdb.uid;

/**
 * An interface that describes the triple (id, type, name).
 */
public interface IdentifierDecorator {
  byte[] getId();
  UniqueIdType getType();
  String getName();
}

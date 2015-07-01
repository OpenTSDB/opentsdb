package net.opentsdb.uid;

/**
 * An interface that describes the triple (id, type, name).
 */
public interface IdentifierDecorator {
  LabelId getId();

  IdType getType();

  String getName();
}

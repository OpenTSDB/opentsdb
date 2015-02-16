package net.opentsdb.uid;

/**
 * Test helper implementation of the Label interface that allows us to hard code
 * the return values.
 */
public class StaticIdentifierDecorator implements IdentifierDecorator {
  private final byte[] id;
  private final UniqueIdType type;
  private final String name;

  public StaticIdentifierDecorator(final byte[] id,
                                   final UniqueIdType type,
                                   final String name) {
    this.id = id;
    this.type = type;
    this.name = name;
  }

  @Override
  public byte[] getId() {
    return id;
  }

  @Override
  public UniqueIdType getType() {
    return type;
  }

  @Override
  public String getName() {
    return name;
  }
}

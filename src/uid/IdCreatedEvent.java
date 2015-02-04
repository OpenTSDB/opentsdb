package net.opentsdb.uid;

/**
 * The event that should be published to an {@link com.google.common.eventbus.EventBus}
 * when an ID has been created.
 */
public class IdCreatedEvent {
  private final byte[] id;
  private final String name;
  private final UniqueIdType type;

  /**
   * Create an ID with the specified arguments. No arguments should be {@code null}.
   * @param id    The id that has been created
   * @param name  The name that the id is associated with
   * @param type  The type of id that has been allocated.
   */
  public IdCreatedEvent(final byte[] id,
                        final String name,
                        final UniqueIdType type) {
    this.id = id;
    this.name = name;
    this.type = type;
  }

  /**
   * The ID that has been allocated
   */
  public byte[] getId() {
    return id;
  }

  /**
   * The name that has been allocated to the ID.
   */
  public String getName() {
    return name;
  }

  /**
   * The type of ID that has been allocated.
   */
  public UniqueIdType getType() {
    return type;
  }
}

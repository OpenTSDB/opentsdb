package net.opentsdb.uid;

/**
 * The event that should be published to an {@link com.google.common.eventbus.EventBus}
 * when a label has been deleted.
 */
public class LabelDeletedEvent extends LabelEvent {
  /**
   * Create an event for the label with the specified arguments. No arguments
   * should be {@code null}.
   *
   * @param id   The id of the label that has been deleted
   * @param name The name of the label that has been deleted
   * @param type The type of the label that has been deleted
   */
  public LabelDeletedEvent(final LabelId id,
                           final String name,
                           final UniqueIdType type) {
    super(id, name, type);
  }
}

package net.opentsdb.uid;

import org.junit.Test;

import static org.junit.Assert.*;

public class LabelEventTest {
  @Test
  public void ctorSetsArguments() {
    final byte[] id = new byte[] {0, 0, 1};
    final String name = "sys.cpu";
    final UniqueIdType type = UniqueIdType.METRIC;

    LabelCreatedEvent event = new LabelCreatedEvent(id, name, type);

    assertArrayEquals(id, event.getId());
    assertEquals(name, event.getName());
    assertEquals(type, event.getType());
  }
}
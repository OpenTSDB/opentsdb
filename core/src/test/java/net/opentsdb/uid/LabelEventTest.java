package net.opentsdb.uid;

import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class LabelEventTest {
  @Test
  public void ctorSetsArguments() {
    final LabelId id = mock(LabelId.class);
    final String name = "sys.cpu";
    final UniqueIdType type = UniqueIdType.METRIC;

    LabelCreatedEvent event = new LabelCreatedEvent(id, name, type);

    assertSame(id, event.getId());
    assertEquals(name, event.getName());
    assertEquals(type, event.getType());
  }
}
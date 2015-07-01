package net.opentsdb.uid;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

import org.junit.Test;

public class LabelEventTest {
  @Test
  public void ctorSetsArguments() {
    final LabelId id = mock(LabelId.class);
    final String name = "sys.cpu";
    final LabelType type = LabelType.METRIC;

    LabelCreatedEvent event = new LabelCreatedEvent(id, name, type);

    assertSame(id, event.getId());
    assertEquals(name, event.getName());
    assertEquals(type, event.getType());
  }
}
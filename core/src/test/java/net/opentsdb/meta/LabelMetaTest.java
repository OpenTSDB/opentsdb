
package net.opentsdb.meta;

import net.opentsdb.uid.LabelId;
import net.opentsdb.uid.UniqueIdType;
import org.junit.Test;

import static net.opentsdb.uid.UniqueIdType.METRIC;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public final class LabelMetaTest {
  private final LabelId VALID_UID = mock(LabelId.class);
  private final UniqueIdType VALID_TYPE = METRIC;
  private final String VALID_NAME = "valid_name";
  private final String VALID_DESCRIPTION = "valid_description";
  private final long VALID_CREATED = 100;

  @Test(expected = NullPointerException.class)
  public void testCtorNoUID() {
    LabelMeta.create(null, VALID_TYPE, VALID_NAME, VALID_DESCRIPTION, VALID_CREATED);
  }

  @Test(expected = NullPointerException.class)
  public void testCtorNoType() {
    LabelMeta.create(VALID_UID, null, VALID_NAME, VALID_DESCRIPTION, VALID_CREATED);
  }

  @Test(expected = NullPointerException.class)
  public void testCtorNoName() {
    LabelMeta.create(VALID_UID, VALID_TYPE, null, VALID_DESCRIPTION, VALID_CREATED);
  }

  @Test(expected = NullPointerException.class)
  public void testCtorEmptyName() {
    LabelMeta.create(VALID_UID, VALID_TYPE, "", VALID_DESCRIPTION, VALID_CREATED);
  }

  @Test(expected = NullPointerException.class)
  public void testCtorNoDescription() {
    LabelMeta.create(VALID_UID, VALID_TYPE, VALID_NAME, null, VALID_CREATED);
  }

  @Test(expected = NullPointerException.class)
  public void testCtorEmptyDescription() {
    LabelMeta.create(VALID_UID, VALID_TYPE, VALID_NAME, "", VALID_CREATED);
  }

  @Test(expected = NullPointerException.class)
  public void testCtorArgumentOrder() {
    final LabelMeta meta = LabelMeta.create(VALID_UID, VALID_TYPE, VALID_NAME, VALID_DESCRIPTION, VALID_CREATED);
    assertSame(VALID_UID, meta.identifier());
    assertEquals(VALID_TYPE, meta.type());
    assertEquals(VALID_NAME, meta.name());
    assertEquals(VALID_DESCRIPTION, meta.description());
    assertEquals(VALID_CREATED, meta.created());
  }
}

package net.opentsdb.meta;

import static net.opentsdb.uid.IdType.METRIC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

import net.opentsdb.uid.IdType;
import net.opentsdb.uid.LabelId;

import org.junit.Test;

public final class LabelMetaTest {
  private static final LabelId VALID_UID = mock(LabelId.class);
  private static final IdType VALID_TYPE = METRIC;
  private static final String VALID_NAME = "valid_name";
  private static final String VALID_DESCRIPTION = "valid_description";
  private static final long VALID_CREATED = 100L;

  @Test(expected = NullPointerException.class)
  public void testCtorNullId() {
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
    final LabelMeta meta = LabelMeta.create(VALID_UID, VALID_TYPE, VALID_NAME, VALID_DESCRIPTION,
        VALID_CREATED);
    assertSame(VALID_UID, meta.identifier());
    assertEquals(VALID_TYPE, meta.type());
    assertEquals(VALID_NAME, meta.name());
    assertEquals(VALID_DESCRIPTION, meta.description());
    assertEquals(VALID_CREATED, meta.created());
  }
}

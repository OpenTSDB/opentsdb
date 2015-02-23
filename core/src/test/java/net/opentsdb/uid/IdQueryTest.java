package net.opentsdb.uid;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class IdQueryTest {
  private static final String ARBITRARY_QUERY = "myQuery";
  private static final UniqueIdType ARBITRARY_TYPE = UniqueIdType.METRIC;
  private static final int ARBITRARY_LIMIT = 100;
  private static final int BAD_LIMIT = -100;

  @Test
  public void constructor() {
    final IdQuery query = new IdQuery(ARBITRARY_QUERY,
            ARBITRARY_TYPE, ARBITRARY_LIMIT);

    assertEquals(ARBITRARY_QUERY, query.getQuery());
    assertEquals(ARBITRARY_TYPE, query.getType());
    assertEquals(ARBITRARY_LIMIT, query.getMaxResults());
  }

  @Test
  public void constructorEmptyQuery() {
    final IdQuery query = new IdQuery("", ARBITRARY_TYPE, ARBITRARY_LIMIT);

    assertNull(query.getQuery());
    assertEquals(ARBITRARY_TYPE, query.getType());
    assertEquals(ARBITRARY_LIMIT, query.getMaxResults());
  }

  @Test
  public void constructorNullQuery() {
    final IdQuery query = new IdQuery(null, ARBITRARY_TYPE, ARBITRARY_LIMIT);

    assertNull(query.getQuery());
    assertEquals(ARBITRARY_TYPE, query.getType());
    assertEquals(ARBITRARY_LIMIT, query.getMaxResults());
  }

  @Test
  public void constructorNullType() {
    final IdQuery query = new IdQuery(ARBITRARY_QUERY, null, ARBITRARY_LIMIT);

    assertEquals(ARBITRARY_QUERY, query.getQuery());
    assertNull(query.getType());
    assertEquals(ARBITRARY_LIMIT, query.getMaxResults());
  }

  @Test(expected = IllegalArgumentException.class)
  public void constructorNegativeLimit() {
    new IdQuery(ARBITRARY_QUERY, ARBITRARY_TYPE, BAD_LIMIT);
  }

  @Test
  public void constructorNoLimit() {
    final IdQuery query = new IdQuery(ARBITRARY_QUERY,
            ARBITRARY_TYPE, IdQuery.NO_LIMIT);

    assertEquals(ARBITRARY_QUERY, query.getQuery());
    assertEquals(ARBITRARY_TYPE, query.getType());
    assertEquals(IdQuery.NO_LIMIT, query.getMaxResults());
  }
}
package net.opentsdb.storage.hbase;

import dagger.ObjectGraph;
import net.opentsdb.storage.DatabaseTests;
import net.opentsdb.storage.MockBase;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.uid.IdQuery;
import net.opentsdb.uid.IdentifierDecorator;
import net.opentsdb.uid.UniqueIdType;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.inject.Inject;
import java.util.List;

import static org.junit.Assert.assertEquals;

@Category(DatabaseTests.class)
public class IdQueryRunnerTest {
  @Inject TsdbStore store;

  @Before
  public void setUp() throws Exception {
    ObjectGraph.create(new HBaseTestModule()).inject(this);

    store.allocateUID("olga1", UniqueIdType.METRIC);
    store.allocateUID("olga2", UniqueIdType.TAGK);
    store.allocateUID("olga3", UniqueIdType.TAGV);
    store.allocateUID("bogda1", UniqueIdType.METRIC);
    store.allocateUID("bogda2", UniqueIdType.TAGK);
    store.allocateUID("bogda3", UniqueIdType.TAGV);
  }

  @Test
  public void testFiltersNameWithoutType() throws Exception {
    IdQuery query = new IdQuery("olga", null);

    List<IdentifierDecorator> identifiers = store.executeIdQuery(query).join(MockBase.DEFAULT_TIMEOUT);

    assertEquals(identifiers.get(0).getName(), "olga1");
    assertEquals(identifiers.get(0).getType(), UniqueIdType.METRIC);
    assertEquals(identifiers.get(0).getName(), "olga2");
    assertEquals(identifiers.get(0).getType(), UniqueIdType.TAGK);
    assertEquals(identifiers.get(0).getName(), "olga3");
    assertEquals(identifiers.get(0).getType(), UniqueIdType.TAGV);
  }

  @Test
  public void testFiltersTypeWithoutName() throws Exception {
    IdQuery query = new IdQuery(null, UniqueIdType.METRIC);

    List<IdentifierDecorator> identifiers = store.executeIdQuery(query).join(MockBase.DEFAULT_TIMEOUT);

    assertEquals(identifiers.get(0).getName(), "olga1");
    assertEquals(identifiers.get(0).getType(), UniqueIdType.METRIC);
    assertEquals(identifiers.get(0).getName(), "bogda1");
    assertEquals(identifiers.get(0).getType(), UniqueIdType.METRIC);
  }

  @Test
  public void testFiltersTypeAndName() throws Exception {
    IdQuery query = new IdQuery("olga", UniqueIdType.METRIC);

    List<IdentifierDecorator> identifiers = store.executeIdQuery(query).join(MockBase.DEFAULT_TIMEOUT);

    assertEquals(identifiers.get(0).getName(), "olga1");
    assertEquals(identifiers.get(0).getType(), UniqueIdType.METRIC);
  }

  @Test
  public void testLimitsNumberOfResults() throws Exception {
    IdQuery query = new IdQuery("olga", null, 2);

    List<IdentifierDecorator> identifiers = store.executeIdQuery(query).join(MockBase.DEFAULT_TIMEOUT);

    assertEquals(identifiers.get(0).getName(), "olga1");
    assertEquals(identifiers.get(0).getType(), UniqueIdType.METRIC);
    assertEquals(identifiers.get(0).getName(), "olga2");
    assertEquals(identifiers.get(0).getType(), UniqueIdType.TAGK);
  }
}
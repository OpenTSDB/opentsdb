package net.opentsdb.storage.cassandra;

import com.google.common.base.Optional;
import net.opentsdb.core.StoreSupplier;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.uid.UniqueIdType;
import net.opentsdb.utils.Config;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TestCassandraStore {

    TsdbStore store;

    @Before
    public void setUp() throws Exception {

        StoreSupplier ss = new StoreSupplier(new Config(false));
        store = ss.get();
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void constructor() throws IOException {
        StoreSupplier ss = new StoreSupplier(new Config(false));
        store = ss.get();
    }

    @Test
    public void allocateUID() {
        store.allocateUID("new",UniqueIdType.METRIC);
    }

    @Test
    public void allocateUIDRename() {
        store.allocateUID("renamed",new byte[]{0,0,4},UniqueIdType.METRIC);
    }

    @Test
    public void getName() throws Exception {
        //TODO (zeeck) make timeout ot a const
        Optional<String> name;
        name = store.getName(new byte[] {0, 0, 1}, UniqueIdType.METRIC)
                .joinUninterruptibly(20);
        assertEquals("sys",name.get());
        name = store.getName(new byte[] {0, 0, 2}, UniqueIdType.METRIC)
                .joinUninterruptibly(20);
        assertEquals("cpu0",name.get());
        name = store.getName(new byte[] {0, 0, 3}, UniqueIdType.METRIC)
                .joinUninterruptibly(20);
        assertEquals("cpu1",name.get());
        name = store.getName(new byte[] {0, 0, 4}, UniqueIdType.METRIC)
                .joinUninterruptibly(20);
        assertEquals("renamed",name.get());
        name = store.getName(new byte[] {0, 0, 5}, UniqueIdType.METRIC)
                .joinUninterruptibly(20);
        assertFalse(name.isPresent());
    }
}
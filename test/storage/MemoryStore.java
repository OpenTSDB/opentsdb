package net.opentsdb.storage;

import com.stumbleupon.async.Deferred;
import org.hbase.async.*;

import java.util.ArrayList;

public class MemoryStore implements TsdbStore {
  @Override
  public Deferred<Long> atomicIncrement(AtomicIncrementRequest air) {
    throw new UnsupportedOperationException("Method seems to not be mocked?");
  }

  @Override
  public Deferred<Long> bufferAtomicIncrement(AtomicIncrementRequest request) {
    throw new UnsupportedOperationException("Method seems to not be mocked?");
  }

  @Override
  public Deferred<Boolean> compareAndSet(PutRequest edit, byte[] expected) {
    throw new UnsupportedOperationException("Method seems to not be mocked?");
  }

  @Override
  public Deferred<Object> delete(DeleteRequest request) {
    throw new UnsupportedOperationException("Method seems to not be mocked?");
  }

  @Override
  public Deferred<Object> ensureTableExists(String table) {
    throw new UnsupportedOperationException("Method seems to not be mocked?");
  }

  @Override
  public Deferred<Object> flush() {
    throw new UnsupportedOperationException("Method seems to not be mocked?");
  }

  @Override
  public Deferred<ArrayList<KeyValue>> get(GetRequest request) {
    throw new UnsupportedOperationException("Method seems to not be mocked?");
  }

  @Override
  public long getFlushInterval() {
    throw new UnsupportedOperationException("Method seems to not be mocked?");
  }

  @Override
  public Scanner newScanner(byte[] table) {
    throw new UnsupportedOperationException("Method seems to not be mocked?");
  }

  @Override
  public Deferred<Object> put(PutRequest request) {
    throw new UnsupportedOperationException("Method seems to not be mocked?");
  }

  @Override
  public void setFlushInterval(short aShort) {
    throw new UnsupportedOperationException("Method seems to not be mocked?");
  }

  @Override
  public Deferred<Object> shutdown() {
    throw new UnsupportedOperationException("Method seems to not be mocked?");
  }

  @Override
  public ClientStats stats() {
    throw new UnsupportedOperationException("Method seems to not be mocked?");
  }

  @Override
  public Deferred<byte[]> getId(String name, byte[] table, byte[] kind) {
    throw new UnsupportedOperationException("Method seems to not be mocked?");
  }

  @Override
  public Deferred<String> getName(byte[] id, byte[] table, byte[] kind) {
    throw new UnsupportedOperationException("Method seems to not be mocked?");
  }
}

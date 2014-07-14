// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.storage;

import com.google.common.base.Charsets;
import com.stumbleupon.async.Deferred;
import net.opentsdb.utils.Config;
import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.PutRequest;
import org.hbase.async.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import org.hbase.async.ClientStats;

import java.nio.charset.Charset;
import java.util.ArrayList;

/**
 * The HBaseStore that implements the client interface required by TSDB.
 */
public final class HBaseStore implements TsdbStore {
  /** Charset used to convert Strings to byte arrays and back. */
  private static final Charset CHARSET = Charsets.ISO_8859_1;

  private final org.hbase.async.HBaseClient client;

  private final boolean enable_realtime_ts;
  private final boolean enable_realtime_uid;
  private final boolean enable_tsuid_incrementing;
  private final boolean enable_tree_processing;

  private final byte[] data_table_name;
  private final byte[] uid_table_name;
  private final byte[] tree_table_name;
  private final byte[] meta_table_name;

    public HBaseStore(final Config config) {
        super();
        this.client = new org.hbase.async.HBaseClient(
                config.getString("tsd.storage.hbase.zk_quorum"),
                config.getString("tsd.storage.hbase.zk_basedir"));

      enable_tree_processing = config.enable_tree_processing();
      enable_realtime_ts = config.enable_realtime_ts();
      enable_realtime_uid = config.enable_realtime_uid();
      enable_tsuid_incrementing = config.enable_tsuid_incrementing();

      data_table_name = config.getString("tsd.storage.hbase.data_table").getBytes(CHARSET);
      uid_table_name = config.getString("tsd.storage.hbase.uid_table").getBytes(CHARSET);
      tree_table_name = config.getString("tsd.storage.hbase.tree_table").getBytes(CHARSET);
      meta_table_name = config.getString("tsd.storage.hbase.meta_table").getBytes(CHARSET);
    }

    @Override
    public Deferred<Long> bufferAtomicIncrement(AtomicIncrementRequest request) {
        return this.client.bufferAtomicIncrement(request);
    }

    @Override
    public Deferred<Boolean> compareAndSet(PutRequest edit, byte[] expected) {
        return this.client.compareAndSet(edit ,expected);
    }

    @Override
    public Deferred<Object> delete(DeleteRequest request) {
        return this.client.delete(request);
    }

    @Override
    public Deferred<ArrayList<Object>> checkNecessaryTablesExist() {
      final ArrayList<Deferred<Object>> checks = new ArrayList<Deferred<Object>>(4);
      checks.add(client.ensureTableExists(data_table_name));
      checks.add(client.ensureTableExists(uid_table_name));

      if (enable_tree_processing) {
        checks.add(client.ensureTableExists(tree_table_name));
      }
      if (enable_realtime_ts || enable_realtime_uid ||
              enable_tsuid_incrementing) {
        checks.add(client.ensureTableExists(meta_table_name));
      }

      return Deferred.group(checks);
    }

    @Override
    public Deferred<Object> flush() {
        return this.client.flush();
    }

    @Override
    public Deferred<ArrayList<KeyValue>> get(GetRequest request) {
        return this.client.get(request);
    }

    @Override
    public Scanner newScanner(byte[] table) {
        return this.client.newScanner(table);
    }

    @Override
    public Deferred<Object> put(PutRequest request) {
        return this.client.put(request);
    }

    @Override
    public Deferred<Object> shutdown() {
        return this.client.shutdown();
    }

    @Override
    public ClientStats stats() {
        return this.client.stats();
    }

    @Override
    public void setFlushInterval(short aShort) {
        this.client.setFlushInterval(aShort);
    }

    @Override
    public long getFlushInterval() {
        return this.client.getFlushInterval();
    }

    @Override
    public Deferred<Long> atomicIncrement(AtomicIncrementRequest air) {
        return this.client.atomicIncrement(air);
    }
}

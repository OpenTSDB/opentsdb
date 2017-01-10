// This file is part of OpenTSDB.
// Copyright (C) 2015  The OpenTSDB Authors.
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
package net.opentsdb.rollup;

import org.hbase.async.Bytes;
import org.hbase.async.KeyValue;

import net.opentsdb.core.RowSeq;
import net.opentsdb.core.Span;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.iRowSeq;

/**
 * Represents a read-only sequence of continuous data points.
 * <p>
 * This class stores a continuous sequence of {@link RowSeq}s in memory.
 * @since 2.4
 */
public final class RollupSpan extends Span {

  private RollupQuery rollup_query;
  
  private byte[] tsuid;
  
  /**
   * Default constructor.
   * @param tsdb The TSDB to which we belong
   * @param rollup_query holds information about a rollup interval and 
   *  rollup aggregator
   * @throws IllegalStateException if it is default rollup interval
   */
  public RollupSpan(final TSDB tsdb, RollupQuery rollup_query) {
    super(tsdb);

    if (rollup_query.getRollupInterval().isDefaultRollupInterval()) {
      throw new IllegalStateException("Rolup Span is not applicable to default "
        + "rollup interval. Default rollup interval is encoded in the same way"
        + " as the raw data.");
    }
    
    this.rollup_query = rollup_query;
  }
  
  @Override
  protected void addRow(final KeyValue row) {
    if (rows.size() > 0) {
      final byte[] key = row.key();
      final iRowSeq last = rows.get(rows.size() - 1);
      String error = null;
      if (key.length != last.key().length) {
        error = "row key length mismatch";
      }

      if (Bytes.memcmp(last.key(), key) == 0) {
        last.addRow(row);
        return;
      }
    }
    final iRowSeq rowseq = createRowSequence(tsdb);
    rowseq.setRow(row);
    rows.add(rowseq);
  }

  /**
   * RowSeq abstract factory API implementation
   * @param tsdb The TSDB to which we belong
   * @return RollupSeq object which stores  read-only sequence 
   *    of continuous HBase rows - rollup data
   */
  @Override
  protected iRowSeq createRowSequence(final TSDB tsdb) {
    return new RollupSeq(tsdb, this.rollup_query);
  }
}

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
package net.opentsdb.core;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;
import org.hbase.async.Bytes;
import org.hbase.async.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/*
 * A wrapper class to abstract the format changes required to handle appends
 */

public class AppendKeyValue {
  private static final Logger LOG = LoggerFactory.getLogger(AppendKeyValue.class);
  public static final int AUTO_HEAL_THRESHOLD = 2*3600; //in seconds
  
  private byte[] value;
  private byte[] qualifier;
  //Tells that whether the KeyValue contains out of order data or not 
  private boolean outofOrderData = false;
  //Tells that whether the KeyValue contains duplicate data or not 
  private boolean hasDuplicates = false;
  //Tells that whether the KeyValue contains qualifiers with mix of seconds
  //and milliseconds
  private boolean hasMixofSecondsAndms = false;

  public AppendKeyValue() {
  }

  public AppendKeyValue(KeyValue kv, TSDB tsdb) {
    parseAndFixKeyValue(kv, tsdb, true);
  }

  public AppendKeyValue(byte[] qualifier, byte[] value) {
    this.value = value;
    this.qualifier = qualifier;
  }

  public byte[] toByteArray() {
    byte[] bytes = new byte[qualifier.length+value.length];
    
    System.arraycopy(this.qualifier, 0, bytes, 0, qualifier.length);
    System.arraycopy(value, 0, bytes, qualifier.length, value.length);
    return bytes;
  }
  
  /**
   * Parses the KeyValue object into Cell objects. It also detects the out of
   * order and duplicate cells. It update the hbase cell back with the healed 
   * data if the tsdb is configured with auto heal flag.
   * @param kv KeyValue object - which stores all the data point in values as
   *  q1v1q2v2q3v3.....
   * @param tsdb TSDB object
   * @param copyFixedCell true - Copy the extracted qualifiers and values into 
   * current objects qualifier and value byte array like compacted Cell
   * false - doesn't copy the qualifiers and values
   * @return List of Cell objects each representing one data point
   * @since 2.0
   */
  public final Collection<Internal.Cell> parseAndFixKeyValue(KeyValue kv, TSDB 
    tsdb, boolean copyFixedCell) {
    
    boolean autoHeal = tsdb.isAutoHealOnRead();
    
    if (tsdb == null) {
      throw new IllegalArgumentException("It expects TSDB object, it can not be null");
    }
    
    long baseTimestamp = Internal.baseTime(tsdb, kv.key());

    if (!Arrays.equals(kv.qualifier(), Const.APPEND_QUALIFIER)) {
      throw new IllegalArgumentException("Can not parse cell, it is not " +
        " an appended cell. It has a different qualifier " + 
        Bytes.pretty(kv.qualifier()) + 
        ", row key " + Bytes.pretty(kv.key()));
    }
    
    int val_idx = 0;
    int val_length = 0;
    int qual_length = 0;
    int last_delta = -1;  // Time delta, extracted from the qualifier.
    
    Map<Integer, Internal.Cell> deltas = new TreeMap<Integer, Internal.Cell>();
    int qualTime = 0;
    
    while (val_idx < kv.value().length) {
      final byte[] q = Internal.extractQualifier(kv.value(), val_idx);
      System.arraycopy(kv.value(), val_idx, q, 0, q.length);
      val_idx=val_idx + q.length;
      
      int vlen = Internal.getValueLengthFromQualifier(q);
      byte[] v = new byte[vlen];
      System.arraycopy(kv.value(), val_idx, v, 0, vlen);
      val_idx += vlen;
      int delta = Internal.getOffsetFromQualifier(q);

      Internal.Cell duplicate = deltas.get(delta);
      if (duplicate != null) {
        //This is a duplicate cell, skip it
        hasDuplicates = true;
        LOG.info("There are duplicate data points at " + 
        (baseTimestamp + delta) + ", skiping the duplicates, " +
        "row key " + Bytes.pretty(kv.key()));

        qual_length -= duplicate.qualifier.length;
        val_length -= duplicate.value.length;
      }

      qual_length += q.length;
      val_length += vlen;
      final Internal.Cell cell = new Internal.Cell(q, v);
      deltas.put(delta, cell);

      if (!outofOrderData) {
        //Data points needs to be sorted if we find atleat one out of order data

        if (delta <= last_delta) {
          LOG.info("There are out of order data, that needs to be sorted for the row key " + Bytes.pretty(kv.key()));
          outofOrderData = true;
        }
      
        last_delta = delta;
      }
      
      if (!hasMixofSecondsAndms) {
        if ((qualTime == 2 && q.length == 4) || (qualTime == 4 && q.length == 2)){
          hasMixofSecondsAndms = true;
        }
        else {
          qualTime = q.length;
        }
      }
     }
    
    if (autoHeal) {
      if (!outofOrderData && !hasDuplicates) {
        autoHeal = false;
      }
      else {
        //Check whether this KeyValue is eligible for auto healing
        long currentBaseTime = (System.currentTimeMillis()/1000) - 
          ((System.currentTimeMillis()/1000) % Const.MAX_TIMESPAN);
        
        if ((currentBaseTime - baseTimestamp) < AUTO_HEAL_THRESHOLD) {
          //This is recently added row, don't need to heal it
          autoHeal = false;
        }
      }
    }

    // Check we consumed all the bytes of the value.
    if (val_idx != kv.value().length) {
      throw new IllegalDataException("Corrupted value: couldn't break down"
      + " into individual values (consumed " + val_idx + " bytes, but was"
      + " expecting to consume " + (kv.value().length) + "): " + kv
      + ", cells so far: " + deltas.values());
    }

    val_idx = 0;
    int qual_idx = 0;
    byte[] healedCell = null;
    int healedIndex = 0;
    
    if (copyFixedCell) {
      this.value = new byte[val_length];
      this.qualifier = new byte[qual_length];
    }
    
    if (autoHeal) {
      healedCell = new byte[val_length+qual_length];
    }
    
    for (Internal.Cell k:deltas.values()) {
      if (copyFixedCell) {
        System.arraycopy(k.qualifier, 0, this.qualifier, qual_idx, k.qualifier.length);
        qual_idx += k.qualifier.length;
        System.arraycopy(k.value, 0, this.value, val_idx, k.value.length);
        val_idx += k.value.length;
      }
      
      if (autoHeal) {
        System.arraycopy(k.qualifier, 0, healedCell, healedIndex, k.qualifier.length);
        healedIndex += k.qualifier.length;
        System.arraycopy(k.value, 0, healedCell, healedIndex, k.value.length);
        healedIndex += k.value.length;
      }
    }

    if (autoHeal) {
      autoHealOutOfOrderData(kv, healedCell, tsdb);
    }
    
    return deltas.values();
  }

  public byte[] getValue() {
    return value;
  }

  public byte[] getQualifier() {
    return qualifier;
  }

  public short getQualifierAsShort() {
    return (qualifier == null) ? 0 : Bytes.getShort(qualifier);
  }

  public boolean hasOutOfOrderData() {
    return outofOrderData;
  }

  public boolean hasDuplicates() {
    return hasDuplicates;
  }

  public boolean hasMixofSecondsAndms() {
    return hasMixofSecondsAndms;
  }
  
  
  /**
   * Auto heal out of data and duplicate data. It accepts the key value with
   * healed data and put back to hbase. 
   * @param kv KeyValue object - Use to find the actual cell - row key
   * @param healedCell stores the healed qualifiers and values like append does
   * @param tsdb TSDB object
   * @return List of Cell objects each representing one data point
   * @since 2.0
   */
  private void autoHealOutOfOrderData(KeyValue kv, byte[] healedCell, TSDB tsdb) {
    LOG.info("Found OOO data or duplicate cells to be healed, auto healing row key " + Bytes.pretty(kv.key()));
    tsdb.put(kv.key(), kv.qualifier(), healedCell);
  }
}

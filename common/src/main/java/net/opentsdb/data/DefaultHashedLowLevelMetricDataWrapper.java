// This file is part of OpenTSDB.
// Copyright (C) 2021  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.data;

import net.opentsdb.data.LowLevelMetricData.HashedLowLevelMetricData;
import net.opentsdb.pools.CloseablePooledObject;
import net.opentsdb.pools.PooledObject;
import net.opentsdb.storage.TimeSeriesDataConsumer.WriteCallback;
import net.opentsdb.storage.WriteStatus;

import java.io.IOException;
import java.util.Arrays;

/**
 * A wrapper used when forwarding data to another consumer but some of that
 * data needs to be skipped.
 *
 * @since 3.0
 */
public class DefaultHashedLowLevelMetricDataWrapper implements
        HashedLowLevelMetricData,
        LowLevelTimeSeriesDataDropWrapper,
        WriteCallback,
        CloseablePooledObject {

  private PooledObject pooledObject;
  private HashedLowLevelMetricData data;
  private WriteCallback writeCallback;
  private WriteStatus[] status;
  private int dataPoints;

  @Override
  public void setData(LowLevelTimeSeriesData data, WriteCallback writeCallback) {
    this.data = (HashedLowLevelMetricData) data; // will throw the cast exception
    this.writeCallback = writeCallback;
    if (status == null) {
      status = new WriteStatus[16];
    } else {
      Arrays.fill(null, status);
    }
    dataPoints = 0;
  }

  @Override
  public void drop(int index, WriteStatus status) {
    if (index >= this.status.length) {
      // resize;
      WriteStatus[] temp = new WriteStatus[index + 16];
      System.arraycopy(this.status, 0, temp, 0, this.status.length);
      this.status = temp;
    }
    this.status[index] = status;
  }

  @Override
  public WriteCallback wrapperCallback() {
    return this;
  }

  @Override
  public StringFormat metricFormat() {
    return data.metricFormat();
  }

  @Override
  public int metricStart() {
    return data.metricStart();
  }

  @Override
  public int metricLength() {
    return data.metricLength();
  }

  @Override
  public byte[] metricBuffer() {
    return data.metricBuffer();
  }

  @Override
  public ValueFormat valueFormat() {
    return data.valueFormat();
  }

  @Override
  public long longValue() {
    return data.longValue();
  }

  @Override
  public float floatValue() {
    return data.floatValue();
  }

  @Override
  public double doubleValue() {
    return data.doubleValue();
  }

  @Override
  public long metricHash() {
    return data.metricHash();
  }

  @Override
  public boolean advance() {
    while (true) {
      if (!data.advance()) {
        return false;
      }
      if (status[dataPoints++] != null) {
        // drop it
        continue;
      } else {
        return true;
      }
    }
  }

  @Override
  public boolean hasParsingError() {
    return data.hasParsingError();
  }

  @Override
  public String parsingError() {
    return data.parsingError();
  }

  @Override
  public TimeStamp timestamp() {
    return data.timestamp();
  }

  @Override
  public byte[] tagsBuffer() {
    return data.tagsBuffer();
  }

  @Override
  public int tagBufferStart() {
    return data.tagBufferStart();
  }

  @Override
  public int tagBufferLength() {
    return data.tagBufferLength();
  }

  @Override
  public StringFormat tagsFormat() {
    return data.tagsFormat();
  }

  @Override
  public byte tagDelimiter() {
    return data.tagDelimiter();
  }

  @Override
  public int tagSetCount() {
    return data.tagSetCount();
  }

  @Override
  public boolean advanceTagPair() {
    return data.advanceTagPair();
  }

  @Override
  public int tagKeyStart() {
    return data.tagKeyStart();
  }

  @Override
  public int tagKeyLength() {
    return data.tagKeyLength();
  }

  @Override
  public int tagValueStart() {
    return data.tagValueStart();
  }

  @Override
  public int tagValueLength() {
    return data.tagValueLength();
  }

  @Override
  public boolean commonTags() {
    return data.commonTags();
  }

  @Override
  public boolean commonTimestamp() {
    return data.commonTimestamp();
  }

  @Override
  public long timeSeriesHash() {
    return data.timeSeriesHash();
  }

  @Override
  public long tagsSetHash() {
    return data.tagsSetHash();
  }

  @Override
  public long tagPairHash() {
    return data.tagPairHash();
  }

  @Override
  public long tagKeyHash() {
    return data.tagKeyHash();
  }

  @Override
  public long tagValueHash() {
    return data.tagValueHash();
  }

  @Override
  public void close() throws IOException {
    if (writeCallback != null) {
      data.close();
      data = null;
    }
  }

  @Override
  public void setPooledObject(PooledObject pooledObject) {
    if (this.pooledObject != null) {
      this.pooledObject.release();
    }
    this.pooledObject = pooledObject;
  }

  @Override
  public Object object() {
    return this;
  }

  @Override
  public void release() {
    if (pooledObject != null) {
      pooledObject.release();
    }
  }

  // ----------- WRITE CALLBACK -------------
  @Override
  public void success() {
    if (writeCallback != null) {
      writeCallback.success();
    } else {
      try {
        data.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public void partialSuccess(WriteStatus[] status, int length) {
    if (writeCallback == null) {
      try {
        data.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
      return;
    }

    // this is fun. Since downstream has a smaller view, we have to
    // update only the fields we didn't drop.
    int localStatusIndex = 0;
    for (int i = 0; i < length; i++) {
      while (this.status[localStatusIndex] != null) {
        localStatusIndex++;
      }

      this.status[localStatusIndex++] = status[i];
    }
    writeCallback.partialSuccess(this.status, localStatusIndex);
  }

  @Override
  public void retryAll() {
    if (writeCallback != null) {
      // since some were rejected we can fill in the null values with a retry
      // and call partialSuccess (which is now a missnomer)
      for (int i = 0; i < dataPoints; i++) {
        if (status[i] == null) {
          status[i] = WriteStatus.RETRY;
        }
      }
      writeCallback.partialSuccess(status, dataPoints);
    } else {
      try {
        data.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public void failAll(WriteStatus status) {
    if (writeCallback != null) {
      writeCallback.failAll(status);
    } else {
      try {
        data.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public void exception(Throwable t) {
    if (writeCallback != null) {
      writeCallback.exception(t);
    } else {
      try {
        data.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}

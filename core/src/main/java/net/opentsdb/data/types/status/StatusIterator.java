package net.opentsdb.data.types.status;

import com.google.common.reflect.TypeToken;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TypedTimeSeriesIterator;

public class StatusIterator extends StatusValue implements TypedTimeSeriesIterator<StatusType> {

  private String namespace;
  boolean has_next = true;

  public StatusIterator(
      String namespace,
      String application,
      byte statusCode,
      byte[] statusCodeArray,
      byte statusType,
      String message,
      TimeStamp lastUpdateTime,
      TimeStamp[] timestampArray) {
    super(
        application,
        statusCode,
        statusCodeArray,
        statusType,
        message,
        lastUpdateTime,
        timestampArray);
    this.namespace = namespace;
  }

  @Override
  public TypeToken<StatusType> getType() {
    return StatusType.TYPE;
  }

  @Override
  public boolean hasNext() {
    boolean had = has_next;
    has_next = false;
    return had;
  }

  @Override
  public TimeSeriesValue<StatusType> next() {
    return this;
  }

  public String namespace() {
    return namespace;
  }
}

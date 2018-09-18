package net.opentsdb.data.types.event;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeriesDataType;

public interface EventType extends TimeSeriesDataType {
  
  /** The data type reference to pass around. */
  public static final TypeToken<EventType> TYPE = TypeToken.of(EventType.class);
  
  public String text();
  
}

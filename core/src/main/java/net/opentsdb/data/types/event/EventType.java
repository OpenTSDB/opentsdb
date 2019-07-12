package net.opentsdb.data.types.event;

import java.util.Map;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeStamp;

public interface EventType extends TimeSeriesDataType<EventType> {

  /** The data type reference to pass around. */
  public static final TypeToken<EventType> TYPE = TypeToken.of(EventType.class);

  public String source();

  public String title();

  public String message();

  public String priority();

  public String status();

  public TimeStamp timestamp();

  public TimeStamp endTimestamp();

  public String userId();

  public boolean ongoing();

  public String eventId();

  public String parentId();

  public String childId();

  public Map<String, Object> additionalProps();

  public String namespace();

}

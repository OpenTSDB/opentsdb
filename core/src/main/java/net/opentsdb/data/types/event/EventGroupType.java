package net.opentsdb.data.types.event;

import com.google.common.reflect.TypeToken;
import java.util.Map;
import net.opentsdb.data.TimeSeriesDataType;

public interface EventGroupType extends TimeSeriesDataType<EventGroupType> {

  /** The data type reference to pass around. */
  public static final TypeToken<EventGroupType> TYPE = TypeToken.of(EventGroupType.class);

  /** the key value pairs represnting a group */
  public Map<String, String> group();

  /** The latest event of the group */
  public EventsValue event();
}

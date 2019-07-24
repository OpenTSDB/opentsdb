package net.opentsdb.data.types.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.reflect.TypeToken;
import java.util.Map;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;

@JsonInclude(Include.NON_DEFAULT)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonDeserialize(builder = EventsGroupValue.Builder.class)
public class EventsGroupValue implements EventGroupType, TimeSeriesValue<EventGroupType> {


  /** Key value pairs of a group */
  private final Map<String, String> group;

  /** The latest event */
  private final EventsValue event;

  public EventsGroupValue(final Builder builder) {
    this.group = builder.group;
    this.event = builder.event;
  }

  @Override
  public Map<String, String> group() {
    return group;
  }

  @Override
  public EventsValue event() {
    return event;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  @Override
  public TimeStamp timestamp() {
    return null;
  }

  @Override
  public EventGroupType value() {
    return this;
  }

  @Override
  public TypeToken<EventGroupType> type() {
    return EventGroupType.TYPE;
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Builder {

    /** The key value pairs defining the group. */
    @JsonProperty
    private Map<String, String> group;

    /** The latest event for this group. */
    @JsonProperty
    private EventsValue event;

    /**
     *
     * @param group Group defining the current group
     * @return The builder.
     */
    public Builder setGroup(Map<String, String> group) {
      this.group = group;
      return this;
    }

    /**
     *
     * @param event The latest event for the group
     * @return The builder.
     */
    public Builder setEvent(EventsValue event) {
      this.event = event;
      return this;
    }

    public EventsGroupValue build() {
      return new EventsGroupValue(this);
    }
  }

}

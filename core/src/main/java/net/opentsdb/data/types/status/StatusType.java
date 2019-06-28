package net.opentsdb.data.types.status;

import net.opentsdb.data.TimeSeriesDataType;

/**
 * Represents a status
 *
 * @since 3.0
 */
public interface StatusType extends TimeSeriesDataType<StatusType> {

  String message();

  byte statusCode();

  byte statusType();

}

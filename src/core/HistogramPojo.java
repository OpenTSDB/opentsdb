package net.opentsdb.core;

import java.util.List;
import java.util.Map;

import javax.xml.bind.DatatypeConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class HistogramPojo extends IncomingDataPoint {
  private static final Logger LOG = LoggerFactory.getLogger(HistogramPojo.class);
  
  private int id;
  
  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }
  
  @Override
  public boolean validate(final List<Map<String, Object>> details) {
    if (!super.validate(details)) {
      return false;
    }
    if (id < 0 || id > 255) {
      if (details != null) {
        details.add(getHttpDetails("Invalid type. Must be from 0 to 255."));
      }
      LOG.warn("Invalid type. Must be from 0 to 255.");
      return false;
    }
    return true;
  }
  
  @JsonIgnore
  public byte[] getBytes() {
    return base64StringToBytes(value);
  }
  
  public static String bytesToBase64String(final byte[] raw) {
    return DatatypeConverter.printBase64Binary(raw);
  }
  
  public static byte[] base64StringToBytes(final String encoded) {
    return DatatypeConverter.parseBase64Binary(encoded);
  }
}

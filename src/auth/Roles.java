package net.opentsdb.auth;

import java.util.Collections;
import java.util.EnumSet;

public interface Roles {
  public enum Permissions {
    TELNET_PUT, HTTP_PUT, HTTP_QUERY,
    CREATE_TAGK, CREATE_TAGV, CREATE_METRIC;
  }

  Boolean hasPermission(Permissions permission);
}

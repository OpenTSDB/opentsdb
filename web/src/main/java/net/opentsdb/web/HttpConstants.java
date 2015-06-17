package net.opentsdb.web;

import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import io.netty.handler.codec.http.HttpVersion;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class HttpConstants {
  /**
   * The HTTP version that should be returned by all requests.
   */
  public static final HttpVersion HTTP_VERSION = HTTP_1_1;
  /**
   * The default charset to assume that all requests are using.
   */
  static final Charset CHARSET = StandardCharsets.UTF_8;
}

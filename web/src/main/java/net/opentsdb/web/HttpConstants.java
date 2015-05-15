package net.opentsdb.web;

import io.netty.handler.codec.http.HttpVersion;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

public class HttpConstants {
  /**
   * The default charset to assume that all requests are using.
   */
  static final Charset CHARSET = StandardCharsets.UTF_8;

  /**
   * The HTTP version that should be returned by all requests.
   */
  public static final HttpVersion HTTP_VERSION = HTTP_1_1;
}

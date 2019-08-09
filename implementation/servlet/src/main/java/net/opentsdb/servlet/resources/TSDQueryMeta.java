package net.opentsdb.servlet.resources;

import javax.servlet.ServletConfig;
import javax.servlet.http.HttpServletRequest;

public class TSDQueryMeta {
  final ServletConfig servlet_config;
  final HttpServletRequest request;
  final boolean isGet;

  TSDQueryMeta(ServletConfig servlet_config, HttpServletRequest request, boolean isGet) {
    this.servlet_config = servlet_config;
    this.request = request;
    this.isGet = isGet;
  }
}

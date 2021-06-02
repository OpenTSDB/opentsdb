package net.opentsdb.servlet.filter;

import net.opentsdb.auth.Authorization;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import java.io.IOException;
import java.security.Principal;

public class NoAuthFilter implements AuthFilter {
  private final Principal principal;

  public NoAuthFilter() {
    principal = new Principal() {
      @Override
      public String getName() {
        return "NoAuth";
      }
    };
  }

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {

  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
    HttpServletRequestWrapper wrapper = new HttpServletRequestWrapper((HttpServletRequest) request) {
      @Override
      public Principal getUserPrincipal() {
        return principal;
      }
    };
    chain.doFilter(wrapper, response);
  }

  @Override
  public void destroy() {

  }

  @Override
  public Authorization authorization() {
    return null;
  }
}

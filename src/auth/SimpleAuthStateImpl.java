package net.opentsdb.auth;

import org.jboss.netty.channel.Channel;

import java.util.HashSet;
import java.util.Set;

public class SimpleAuthStateImpl implements AuthState {
  private Channel channel;
  private String user;
  private AuthStatus authStatus = AuthStatus.FORBIDDEN;
  private Set<Roles> roles = new HashSet<Roles>();
  private String message;
  private Throwable exception;

  public SimpleAuthStateImpl(String user, AuthStatus authStatus, String message) {
    this.user = user;
    this.authStatus = authStatus;
    this.message = message;
    Set<Roles> roles = new HashSet<Roles>();
    roles.add(SimpleRolesImpl.GUEST);
    this.roles = roles;
  }

  public SimpleAuthStateImpl(String user, AuthStatus authStatus, Set<Roles> roles, String message) {
    this.user = user;
    this.authStatus = authStatus;
    this.roles = roles;
    this.message = message;
  }

  public SimpleAuthStateImpl(String user, Throwable ex) {
    this.user = user;
    this.authStatus = AuthStatus.ERROR;
    this.message = ex.getMessage();
    roles.add(SimpleRolesImpl.GUEST);
    this.exception = ex;
  }

  @Override
  public String getUser() {
    return this.user;
  }

  @Override
  public Set<Roles> getRoles() {
    return this.roles;
  }

  @Override
  public void setRoles(Set<Roles> roles) {
    this.roles = roles;
  }

  @Override
  public AuthState.AuthStatus getStatus() {
    return authStatus;
  }

  @Override
  public void setStatus(AuthState.AuthStatus status) {
    this.authStatus = status;
  }

  @Override
  public String getMessage() {
    return this.message;
  }

  @Override
  public Throwable getException() {
    return this.exception;
  }

  @Override
  public void setChannel(Channel channel) {
    this.channel = channel;
  }

  public Channel getChannel() {
    return channel;
  }

  @Override
  public byte[] getToken() {
    return new byte[0];
  }
}

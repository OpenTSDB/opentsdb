package net.opentsdb.auth;

import net.opentsdb.query.pojo.Query;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSQuery;
import net.opentsdb.utils.Config;
import org.jboss.netty.channel.Channel;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

@RunWith(PowerMockRunner.class)
@PrepareForTest({TSDB.class, TSQuery.class})
public class AllowAllAuthenticatingAuthorizerTest {
  private TSDB tsdb = null;
  private AllowAllAuthenticatingAuthorizer authenticatingAuthorizer;

  private Channel getMockedChannel() {
    Channel channel = mock(Channel.class);
    when(channel.getAttachment()).thenReturn(AllowAllAuthenticatingAuthorizer.accessGranted);
    return channel;
  }

  @Before
  public void setUp() throws Exception {
    this.authenticatingAuthorizer = new AllowAllAuthenticatingAuthorizer();
    this.tsdb = mock(TSDB.class);

    final Config config = new Config(false);
    config.overrideConfig("tsd.core.authentication.enable", "true");
    when(tsdb.getConfig()).thenReturn(config);

    final AuthState state = mock(AuthState.class);
    when(state.getStatus()).thenReturn(AllowAllAuthenticatingAuthorizer.accessGranted.getStatus());

    final Authorization authorization = mock(Authorization.class);
    when(authorization.allowQuery(any(AuthState.class), any(TSQuery.class))).thenReturn(state);

    final Authentication authentication = mock(Authentication.class);
    when(authentication.authorization()).thenReturn(authorization);
    when(authentication.isReady(any(TSDB.class), any(Channel.class))).thenReturn(true);
    when(tsdb.getAuth()).thenReturn(authentication);
  }

  @Test
  public void isReady() throws Exception {
    Channel channel = getMockedChannel();
    assertTrue(authenticatingAuthorizer.isReady(tsdb, channel));
  }

  @Test
  public void hasPermissionAdministrator() throws Exception {
    AuthState authState = mock(AuthState.class);
    this.authenticatingAuthorizer.setRoles(new Roles(Roles.ADMINISTRATOR));
    assertEquals(AllowAllAuthenticatingAuthorizer.accessGranted, this.authenticatingAuthorizer.hasPermission(authState, Permissions.TELNET_PUT));
  }

  @Test
  public void hasPermissionGuest() throws Exception {
    AuthState authState = mock(AuthState.class);
    this.authenticatingAuthorizer.setRoles(new Roles(Roles.GUEST));
    assertEquals(AllowAllAuthenticatingAuthorizer.accessDenied, this.authenticatingAuthorizer.hasPermission(authState, Permissions.TELNET_PUT));
  }

  @Test
  public void allowTSQueryAdministrator() throws Exception {
    AuthState authState = mock(AuthState.class);
    TSQuery tsQuery = mock(TSQuery.class);
    Roles roles = new Roles(Roles.ADMINISTRATOR);
    this.authenticatingAuthorizer.setRoles(roles);
    assertEquals(AllowAllAuthenticatingAuthorizer.accessGranted, this.authenticatingAuthorizer.allowQuery(authState, tsQuery));
  }

  @Test
  public void allowTSQueryGuest() throws Exception {
    AuthState authState = mock(AuthState.class);
    TSQuery tsQuery = mock(TSQuery.class);
    Roles roles = new Roles(Roles.GUEST);
    this.authenticatingAuthorizer.setRoles(roles);
    assertEquals(AllowAllAuthenticatingAuthorizer.accessDenied, this.authenticatingAuthorizer.allowQuery(authState, tsQuery));
  }

  @Test
  public void allowQueryAdministrator() throws Exception {
    AuthState authState = mock(AuthState.class);
    Query query = mock(Query.class);
    Roles roles = new Roles(Roles.ADMINISTRATOR);
    this.authenticatingAuthorizer.setRoles(roles);
    assertEquals(AllowAllAuthenticatingAuthorizer.accessGranted, this.authenticatingAuthorizer.allowQuery(authState, query));
  }

  @Test
  public void allowQueryGuest() throws Exception {
    AuthState authState = mock(AuthState.class);
    Query query = mock(Query.class);
    Roles roles = new Roles(Roles.GUEST);
    this.authenticatingAuthorizer.setRoles(roles);
    assertEquals(AllowAllAuthenticatingAuthorizer.accessDenied, this.authenticatingAuthorizer.allowQuery(authState, query));
  }
}
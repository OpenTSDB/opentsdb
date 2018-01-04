package net.opentsdb.auth;

import com.stumbleupon.async.Deferred;
import net.opentsdb.core.IncomingDataPoint;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSQuery;
import net.opentsdb.query.pojo.Query;
import net.opentsdb.stats.StatsCollector;

import java.util.concurrent.atomic.AtomicLong;

public class SimpleAuthorizationImpl extends Authorization {
  private static final AtomicLong access_granted = new AtomicLong();
  private static final AtomicLong access_denied = new AtomicLong();

  @Override
  public void initialize(TSDB tsdb) {

  }

  @Override
  public Deferred<Object> shutdown() {
    return null;
  }

  @Override
  public String version() {
    return "2.4.0";
  }

  @Override
  public void collectStats(StatsCollector collector) {
    collector.record("auth.authorization.granted", access_granted);
    collector.record("auth.authorization.denied", access_denied);
  }

  @Override
  public boolean hasPermission(final AuthState state, final Roles.Permissions permission) {
    for (Roles role : state.getRoles()) {
      if (role.hasPermission(permission)) {
        access_granted.getAndIncrement();
        return true;
      }
    }
    access_denied.getAndIncrement();
    return false;
  }

  @Override
  public boolean hasRole(final AuthState state, final Roles role) {
    if (state.getRoles().contains(role)) {
      access_granted.getAndIncrement();
      return true;
    } else {
      access_denied.getAndIncrement();
      return false;
    }
  }

  private final AuthState allowQuery(final AuthState state) {
    if (hasPermission(state, Roles.Permissions.HTTP_QUERY)) {
      state.setStatus(AuthState.AuthStatus.SUCCESS);
      return state;
    } else {
      state.setStatus(AuthState.AuthStatus.FORBIDDEN);
      return state;
    }
  }

  @Override
  public AuthState allowQuery(final AuthState state, final TSQuery query) {
    /*
      We could do things like evaluate the content of the query and decide if this user can execute *this* query,
      rather than just any query in general, but, this is the SimpleAuthorizationImpl.
     */
    return allowQuery(state);
  }

  @Override
  public AuthState allowQuery(final AuthState state, final Query query) {
    /*
      We could do things like evaluate the content of the query and decide if this user can execute *this* query,
      rather than just any query in general, but, this is the SimpleAuthorizationImpl.
     */
    return allowQuery(state);
  }

  @Override
  public AuthState allowWrite(final AuthState state, final IncomingDataPoint incomingDataPoint) {
    /*
      We could do things like evaluate the content of the query and decide if this user can execute *this* query,
      rather than just any query in general, but, this is the SimpleAuthorizationImpl.
     */
    if (hasPermission(state, Roles.Permissions.TELNET_PUT) || hasPermission(state, Roles.Permissions.HTTP_PUT)) {
      state.setStatus(AuthState.AuthStatus.SUCCESS);
      return state;
    } else {
      state.setStatus(AuthState.AuthStatus.FORBIDDEN);
      return state;
    }
  }
}

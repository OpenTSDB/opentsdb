package net.opentsdb.auth;

import net.opentsdb.core.TSDB;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.tsd.BadRequestException;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.util.concurrent.atomic.AtomicLong;

public abstract class AbstractAuthentication extends Authentication {
  protected static final AtomicLong login_success = new AtomicLong();
  protected static final AtomicLong login_failures = new AtomicLong();
  protected static final AtomicLong auth_exceptions = new AtomicLong();

  @Override
  public void initialize(TSDB tsdb) {};

  @Override
  public void collectStats(final StatsCollector collector) {
    collector.record("auth.authentication.success", login_success);
    collector.record("auth.authentication.failure", login_failures);
    collector.record("auth.exceptions", auth_exceptions);
  }

  @Override
  public boolean isReady(final TSDB tsdb, final Channel chan) {
    if (tsdb.getAuthentication() != null && tsdb.getAuthorization() != null) {
      if (chan.getAttachment() == null || !(chan.getAttachment() instanceof AuthState)) {
        auth_exceptions.incrementAndGet();
        throw new BadRequestException(HttpResponseStatus.INTERNAL_SERVER_ERROR,
            "Authentication was enabled but the authentication state for "
                + "this channel was not set properly");
      }
      return true;
    } else {
      return false;
    }
  }
}

package net.opentsdb.tsd;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.opentsdb.core.TSDB;
import net.opentsdb.utils.BlacklistManager;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.io.IOException;
import java.util.List;

/**
 * Created by santhosh.r on 26/04/16.
 */
public class BlacklistRpc implements HttpRpc {
  @Override
  public void execute(TSDB tsdb, HttpQuery query) throws IOException {
    if (query.method() != HttpMethod.GET) {
      throw new BadRequestException(HttpResponseStatus.METHOD_NOT_ALLOWED,
        "Method not allowed", "The HTTP method [" + query.method().getName() +
        "] is not permitted for this endpoint");
    }
    List<String> blacklistedMetrics = BlacklistManager.getAllBlacklistedMetrics();
    query.sendReply(new ObjectMapper().writeValueAsString(blacklistedMetrics));
  }
}

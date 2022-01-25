package net.opentsdb.transport;

import net.opentsdb.auth.AuthState;
import net.opentsdb.data.LowLevelTimeSeriesData;
import net.opentsdb.data.TimeSeriesDatum;
import net.opentsdb.data.TimeSeriesSharedTagsAndTimeData;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import net.opentsdb.storage.TimeSeriesDataConsumer;

public class PulsarConsumer extends BaseTSDBPlugin implements 
    TimeSeriesDataConsumer {
  protected static final Logger LOG = LoggerFactory.getLogger(PulsarConsumer.class);
  
  public static final String TYPE = "PulsarConsumer";
  
  /** Configuration keys. */
  public static final String PROJECT_NAME_KEY = "pulsar.service.url";
  
  private PulsarClient client;

  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
    
    try {
      client = PulsarClient.builder()
          .serviceUrl("pulsar://localhost:6650")
          .build();
      
    } catch (PulsarClientException e) {
      LOG.error("Failed to setup client", e);
      return Deferred.fromError(e);
    }
    
    return Deferred.fromResult(null);
  }
  
  @Override
  public String type() {
    return TYPE;
  }
  
  void registerConfigs(final TSDB tsdb) {
    if (!tsdb.getConfig().hasProperty(PROJECT_NAME_KEY)) {
      tsdb.getConfig().register(PROJECT_NAME_KEY, null, false, 
          "The full service URL");
    }
  }

  @Override
  public void write(AuthState state, TimeSeriesDatum datum, WriteCallback callback) {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public void write(AuthState state, TimeSeriesSharedTagsAndTimeData data, WriteCallback callback) {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public void write(AuthState state, LowLevelTimeSeriesData data, WriteCallback callback) {
    throw new UnsupportedOperationException("TODO");
  }
}

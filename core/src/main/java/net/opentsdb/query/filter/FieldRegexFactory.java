package net.opentsdb.query.filter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.stumbleupon.async.Deferred;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;

public class FieldRegexFactory extends BaseTSDBPlugin implements QueryFilterFactory {

  static final String TYPE = "FieldRegex";

  @Override
  public String getType() {
    return TYPE;
  }

  public FieldRegexFilter parse(
      final TSDB tsdb, final ObjectMapper mapper, final JsonNode node) {
    if (node == null) {
      throw new IllegalArgumentException("Node cannot be null.");
    }
    try {
      return mapper.treeToValue(node, FieldRegexFilter.class);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to parse FieldRegex", e);
    }
  }

  @Override
  public String type() {
    return TYPE;
  }

  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
    return Deferred.fromResult(null);
  }

  @Override
  public String version() {
    return "3.0.0";
  }
}

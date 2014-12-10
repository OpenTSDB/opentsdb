package net.opentsdb.storage.json;

import java.io.IOException;

import net.opentsdb.tree.TreeRule;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

/**
 * Helper class for deserializing Tree Rule type enum from human readable
 * strings
 */
public class TreeRuleTypeDeserializer
  extends JsonDeserializer<TreeRule.TreeRuleType> {

  @Override
  public TreeRule.TreeRuleType deserialize(final JsonParser parser,
                                           final DeserializationContext context)
          throws IOException {
    return TreeRule.stringToType(parser.getValueAsString());
  }
}

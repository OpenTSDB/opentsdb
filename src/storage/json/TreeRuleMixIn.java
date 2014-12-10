package net.opentsdb.storage.json;

import java.util.regex.Pattern;

import net.opentsdb.tree.TreeRule;
import net.opentsdb.utils.JSON;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.PUBLIC_ONLY)
abstract class TreeRuleMixIn {

  @JsonDeserialize(using = JSON.TreeRuleTypeDeserializer.class)
  @JsonProperty("type") abstract TreeRule.TreeRuleType getType();

  @JsonProperty("display_format") abstract String getDisplayFormat();

  @JsonIgnore
  abstract Pattern getCompiledRegex();
}

package net.opentsdb.core;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import org.junit.Test;

import java.io.File;

public class ConfigModuleTest {
  @Test(expected = ConfigException.IO.class)
  public void testFromFile() throws Exception {
    ConfigModule.fromFile(new File("doesNotExist"));
  }

  @Test
  public void testDefaultWithOverrides() throws Exception {
    final ImmutableMap<String, String> overrides = ImmutableMap.of("overriddenField", "value");
    final ConfigModule configModule = ConfigModule.defaultWithOverrides(overrides);

    final Config config = configModule.provideConfig();

    assertEquals("value", config.getString("overriddenField"));
  }
}
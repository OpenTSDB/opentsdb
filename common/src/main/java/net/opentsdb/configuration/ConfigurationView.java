// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.configuration;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import net.opentsdb.configuration.provider.ProtocolProviderFactory;
import net.opentsdb.configuration.provider.Provider;
import net.opentsdb.configuration.provider.ProviderFactory;

/**
 * A read-only view into the configuration class. This provides
 * information useful for dumping in a shell or API.
 * 
 * @since 3.0
 */
@JsonInclude(Include.NON_NULL)
public class ConfigurationView {
  private final List<ProviderWrapper> providers;
  private final Map<String, ConfigurationEntryWrapper> entries;
  private final List<ProviderFactoryWrapper> factories;
  
  /**
   * Package private ctor to instantiate the view from the 
   * {@link Configuration} class.
   * @param config The non-null config we'll parse.
   */
  ConfigurationView(final Configuration config) {
    providers = Lists.newArrayListWithCapacity(config.providers.size());
    for (final Provider provider : config.providers) {
      providers.add(new ProviderWrapper(provider));
    }
    
    entries = Maps.newHashMapWithExpectedSize(config.merged_config.size());
    for (final Entry<String, ConfigurationEntry> entry : 
        config.merged_config.entrySet()) {
      entries.put(entry.getKey(), new ConfigurationEntryWrapper(entry.getValue()));
    }
    
    factories = Lists.newArrayListWithCapacity(config.factories().size()); 
    for (final ProviderFactory factory : config.factories()) {
      if (factory instanceof ProtocolProviderFactory) {
        factories.add(new ProtocolProviderFactoryWrapper((ProtocolProviderFactory) factory));
      } else {
        factories.add(new ProviderFactoryWrapper(factory));
      }
    }
  }
  
  /** @return A list of instantiated providers. */
  public List<ProviderWrapper> getInstantiatedProviders() {
    return Collections.unmodifiableList(providers);
  }
  
  /** @return A list of configuration entries, the schemas and overrides. */
  public Map<String, ConfigurationEntryWrapper> getEntries() {
    return Collections.unmodifiableMap(entries);
  }
  
  /** @return A list of the discovered provider factories (not necessarily
   * those that are loaded though. */
  public List<ProviderFactoryWrapper> getProviderFactories() {
    return Collections.unmodifiableList(factories);
  }
  
  /**
   * Simple wrapper around a provider.
   */
  public static class ProviderWrapper {
    private final String name;
    private final String canonical_name;
    private final String source;
    private final long last_reload;
    
    /**
     * Private ctor.
     * @param provider A non-null provider.
     */
    ProviderWrapper(final Provider provider) {
      name = provider.getClass().getSimpleName();
      canonical_name = provider.getClass().getCanonicalName();
      source = provider.source();
      last_reload = provider.lastReload();
    }

    /** @return The string name and identifier of the provider. */
    public String getName() {
      return name;
    }

    /** @return The canonical name of the provider class. */
    public String getCanonicalName() {
      return canonical_name;
    }
    
    /** @return Either the URI processed by the provider or the same simple
     * name of the provider. */
    public String getSource() {
      return source;
    }
    
    /** @return A Unix epoch timestamp in milliseconds when the provider
     * was last reloaded if reloads were supported. Defaults to 0 if
     * the provider doesn't support reloading. */
    public long getLastReload() {
      return last_reload;
    }
  }
  
  /**
   * Simple wrapper around protocol providers.
   */
  public static class ProtocolProviderFactoryWrapper extends ProviderFactoryWrapper {
    private final String protocol;
    
    /**
     * Private ctor.
     * @param factory A non-null factory.
     */
    ProtocolProviderFactoryWrapper(ProtocolProviderFactory factory) {
      super(factory);
      protocol = factory.protocol();
    }
    
    /** @return The protocol portion of a URI this provider handles. */
    public String getProtocol() {
      return protocol;
    }
  }
  
  /**
   * Simple wrapper around the provider factory.
   */
  public static class ProviderFactoryWrapper {
    private final String name;
    private final String canonical_name;
    private final String description;
    private final boolean reloadable;
    
    /**
     * Private ctor.
     * @param factory A non-null factory.
     */
    ProviderFactoryWrapper(final ProviderFactory factory) {
      name = factory.getClass().getSimpleName();
      canonical_name = factory.getClass().getCanonicalName();
      description = factory.description();
      reloadable = factory.isReloadable();
    }

    /** @return The simple name of the factory. This is what's 
     * referenced in the 'config.providers' list. */
    public String getName() {
      return name;
    }

    /** @return The full canonical name of the factory class. */
    public String getCanonicalName() {
      return canonical_name;
    }

    /** @return A useful description of what the provider does. */
    public String getDescription() {
      return description;
    }

    /** @return Whether or not the provider supports reloading. */
    public boolean isReloadable() {
      return reloadable;
    }
  }
  
  /**
   * A simple wrapper around the configuration entry.
   */
  @JsonInclude(Include.NON_NULL)
  public static class ConfigurationEntryWrapper {
    private final ConfigurationSchemaWrapper schema;
    private final List<OverrideWrapper> overrides;
    private final List<String> callbacks;
    
    /**
     * Private ctor.
     * @param entry A non-null config entry.
     */
    ConfigurationEntryWrapper(final ConfigurationEntry entry) {
      schema = new ConfigurationSchemaWrapper(entry.schema());
      overrides = Lists.newArrayListWithExpectedSize(
          entry.settings() == null ? 0 : entry.settings().size());
      if (entry.settings() != null) {
        for (final ConfigurationOverride override : entry.settings()) {
          overrides.add(new OverrideWrapper(override.getSource(), 
                                            override.getValue(), 
                                            override.getLastChange()));
        }
      }
      callbacks = Lists.newArrayListWithCapacity(
          entry.callbacks() == null ? 0 : entry.callbacks().size());
      if (entry.callbacks() != null) {
        for (final ConfigurationCallback<?> callback : entry.callbacks()) {
          callbacks.add(callback.toString());
        }
      }
    }
    
    /** @return The schema object for this entry. */
    public ConfigurationSchemaWrapper getSchema() {
      return schema;
    }
    
    /** @return A list of overrides for the entry. May be empty. */
    public List<OverrideWrapper> getOverrides() {
      return overrides;
    }
    
    /** @return A list of callback names watching this entry for updates. */
    public List<String> getCallbacks() {
      return callbacks;
    }
    
    /**
     * Simple override wraper.
     */
    @JsonInclude(Include.NON_NULL)
    public class OverrideWrapper {
      private final String source;
      private final Object value;
      private final long last_change;
      
      /**
       * Private ctor.
       * @param source The source name.
       * @param value The value of the override (may be null)
       * @param last_change Last change of the override.
       */
      private OverrideWrapper(final String source,
                              final Object value, 
                              final long last_change) {
        this.source = source;
        this.value = value;
        this.last_change = last_change;
      }
      
      /** @return The source of the override. */
      public String getSource() {
        return source;
      }
      
      /** @return The value of the override. May be null. */
      public Object getValue() {
        return schema.isSecret() ? "********" : value;
      }
      
      /** @return The last time the override was updated for dynamic
       * keys. Unix epoch timestamp in milliseconds. */
      public long getLastChange() {
        return last_change;
      }
    }
    
    /**
     * Simple wrapper around the schema.
     */
    @JsonInclude(Include.NON_NULL)
    public class ConfigurationSchemaWrapper {
      private final Class<?> type;
      private final TypeReference<?> type_reference;
      private final String source;
      private final String description;
      // TODO - wrapper here!
      private final ConfigurationValueValidator validator;
      private final Object default_value;
      private final boolean dynamic;
      private final boolean nullable;
      private final boolean secret;
      
      /**
       * Private ctor.
       * @param schema Non-null schema
       */
      ConfigurationSchemaWrapper(final ConfigurationEntrySchema schema) {
        type = schema.getType();
        type_reference = schema.getTypeReference();
        source = schema.getSource();
        description = schema.getDescription();
        validator = schema.getValidator();
        default_value = schema.getDefaultValue();
        dynamic = schema.isDynamic();
        nullable = schema.isNullable();
        secret = schema.isSecret();
      }
      
      /** @return The type of the value (null if {@link #getTypeReference()} is not.*/
      public Class<?> getType() {
        return type;
      }
      
      /** @return A typereference for the value (null if #getType) is not. */
      public TypeReference<?> getTypeReference() {
        return type_reference;
      }
      
      /** @return The source that generated this schema. */
      public String getSource() {
        return source;
      }
      
      /** @return The description of what this config key does. */
      public String getDescription() {
        return description;
      }
      
      /** @return An optional configuration validator. May be null. */
      public ConfigurationValueValidator getValidator() {
        return validator;
      }
      
      /** @return The default value. May be null. */
      public Object getDefaultValue() {
        return secret ? "********" : 
          default_value;
      }
      
      /** @return Whether or not the parameter supports reloading. */
      public boolean isDynamic() {
        return dynamic;
      }
      
      /** @return Whether or not the value is nullable. */
      public boolean isNullable() {
        return nullable;
      }
      
      /** @return Whether or not the value is a secret and should be
       * obfuscated for debugging. */
      public boolean isSecret() {
        return secret;
      }
      
    }

  }
  
}

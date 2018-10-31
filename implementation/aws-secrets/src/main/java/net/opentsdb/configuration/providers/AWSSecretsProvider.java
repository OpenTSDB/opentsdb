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
package net.opentsdb.configuration.providers;

import java.io.IOException;
import java.util.Base64;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.services.secretsmanager.model.AWSSecretsManagerException;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import com.amazonaws.services.secretsmanager.model.ResourceNotFoundException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;

import io.netty.util.HashedWheelTimer;
import net.opentsdb.common.Const;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.configuration.ConfigurationException;
import net.opentsdb.configuration.ConfigurationOverride;
import net.opentsdb.configuration.provider.BaseSecretProvider;
import net.opentsdb.configuration.provider.ProviderFactory;

/**
 * Super simple implementation of a secret provider using the AWS KMS
 * system. Note that right now we don't do any caching.
 * 
 * TODO - handle other types of credentials.
 * 
 * @since 3.0
 */
public class AWSSecretsProvider extends BaseSecretProvider {
  private static final Logger LOG = LoggerFactory.getLogger(
      AWSSecretsProvider.class);
  
  public static final String PREFIX = "aws.secrets.";
  public static final String PROFILE_KEY = "profile";
  public static final String REGION_KEY = "region";
  
  /** The client. */
  protected AWSSecretsManager client;
  
  @Override
  public void initialize(final ProviderFactory factory, 
                         final Configuration config, 
                         final HashedWheelTimer timer,
                         final String id) {
    if (factory == null) {
      throw new IllegalArgumentException("Factory cannot be null.");
    }
    if (config == null) {
      throw new IllegalArgumentException("Config cannot be null.");
    }
    if (timer == null) {
      throw new IllegalArgumentException("Timer cannot be null.");
    }
    this.factory = factory;
    this.config = config;
    this.timer = timer;
    this.id = id;
    
    registerConfigs(config);
    if (Strings.isNullOrEmpty(config.getString(getConfigKey(PROFILE_KEY)))) {
      throw new IllegalArgumentException("Missing required property " 
          + getConfigKey(PROFILE_KEY));
    }
    if (Strings.isNullOrEmpty(config.getString(getConfigKey(REGION_KEY)))) {
      throw new IllegalArgumentException("Missing required property " 
          + getConfigKey(REGION_KEY));
    }
    
    client = AWSSecretsManagerClientBuilder.standard()
        .withRegion(config.getString(getConfigKey(REGION_KEY)))
        .withCredentials(new ProfileCredentialsProvider("default"))
        .build();
  }
  
  @Override
  public String getSecretString(final String key) {
    final GetSecretValueRequest request = new GetSecretValueRequest()
          .withSecretId(key);
    try {
      final GetSecretValueResult result = client.getSecretValue(request);
      if (!Strings.isNullOrEmpty(result.getSecretString())) {
        return result.getSecretString();
      } else {
        return new String(Base64.getDecoder().decode(
            result.getSecretBinary()).array(), Const.UTF8_CHARSET);
      }
    } catch (ResourceNotFoundException e) {
      throw new ConfigurationException("No such key.", e);
    } catch (AWSSecretsManagerException e) {
      throw new ConfigurationException("Failed to fetch key from AWS.", e);
    } catch (Exception e) {
      throw new ConfigurationException("Unexpected exception.", e);
    }
  }

  @Override
  public byte[] getSecretBytes(String key) {
    final GetSecretValueRequest request = new GetSecretValueRequest()
          .withSecretId(key);
    try {
      final GetSecretValueResult result = client.getSecretValue(request);
      if (!Strings.isNullOrEmpty(result.getSecretString())) {
        return result.getSecretString().getBytes(Const.UTF8_CHARSET);
      } else {
        return Base64.getDecoder().decode(
            result.getSecretBinary()).array();
      }
    } catch (ResourceNotFoundException e) {
      throw new ConfigurationException("No such key.", e);
    } catch (AWSSecretsManagerException e) {
      throw new ConfigurationException("Failed to fetch key from AWS.", e);
    } catch (Exception e) {
      throw new ConfigurationException("Unexpected exception.", e);
    }
  }

  @Override
  public ConfigurationOverride getSetting(final String key) {
    return null;
  }

  @Override
  public void close() throws IOException {
    if (client != null) {
      try {
        client.shutdown();
      } catch (Throwable t) {
        LOG.error("Failed to close client", t);
      }
    }
  }

  @Override
  public void reload() {
    // no-op
  }

  /**
   * Helper to register configuration parameters.
   * @param config The non-null config object.
   */
  @VisibleForTesting
  void registerConfigs(final Configuration config) {
    if (!config.hasProperty(getConfigKey(PROFILE_KEY))) {
      config.register(getConfigKey(PROFILE_KEY), "default", false, 
          "The AWS credentials profile to use.");
    }
    if (!config.hasProperty(getConfigKey(REGION_KEY))) {
      config.register(getConfigKey(REGION_KEY), "us-east-2", false, 
          "The AWS region to connect to.");
    }
  }
  
  /**
   * Helper to compute the config keys.
   * @param suffix A non-null suffix to append.
   * @return The converted key.
   */
  @VisibleForTesting
  String getConfigKey(final String suffix) {
    return PREFIX + 
        (Strings.isNullOrEmpty(id) ? "" : id + ".") 
          + suffix;
  }
  
}

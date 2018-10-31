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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import com.amazonaws.services.secretsmanager.model.ResourceNotFoundException;
import com.amazonaws.util.Base64;

import io.netty.util.HashedWheelTimer;
import net.opentsdb.common.Const;
import net.opentsdb.configuration.ConfigurationException;
import net.opentsdb.configuration.UnitTestConfiguration;
import net.opentsdb.configuration.provider.ProviderFactory;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ AWSSecretsProvider.class, 
  AWSSecretsManagerClientBuilder.class,
  AWSSecretsManagerClientBuilder.class })
public class TestAWSSecretsProvider {

  private UnitTestConfiguration config;
  private AWSSecretsManager client;
  
  @Before
  public void before() throws Exception {
    config = (UnitTestConfiguration) UnitTestConfiguration.getConfiguration();
    client = mock(AWSSecretsManager.class);
    AWSSecretsManagerClientBuilder builder = 
        PowerMockito.mock(AWSSecretsManagerClientBuilder.class);
    
    PowerMockito.mockStatic(AWSSecretsManagerClientBuilder.class);
    PowerMockito.when(AWSSecretsManagerClientBuilder.standard()).thenReturn(builder);
    PowerMockito.when(builder.withRegion(anyString())).thenReturn(builder);
    PowerMockito.when(builder.withCredentials(any(AWSCredentialsProvider.class)))
      .thenReturn(builder);
    PowerMockito.when(builder.build()).thenReturn(client);
  }
  
  @Test
  public void initialize() throws Exception {
    AWSSecretsProvider provider = new AWSSecretsProvider();
    provider.registerConfigs(config);
    
    config.override(provider.getConfigKey(
        AWSSecretsProvider.REGION_KEY), "us-west-1");
    config.override(provider.getConfigKey(
        AWSSecretsProvider.PROFILE_KEY), "Test");
    provider.initialize(mock(ProviderFactory.class), config, 
        mock(HashedWheelTimer.class), null);
    
    assertSame(client, provider.client);
    
    // bad configs
    config.override(provider.getConfigKey(
        AWSSecretsProvider.PROFILE_KEY), "");
    provider = new AWSSecretsProvider();
    try {
      provider.initialize(mock(ProviderFactory.class), config, 
          mock(HashedWheelTimer.class), null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {}
    
    provider.close();
  }
  
  @Test
  public void getSecretString() throws Exception {
    AWSSecretsProvider provider = new AWSSecretsProvider();
    provider.initialize(mock(ProviderFactory.class), config, 
        mock(HashedWheelTimer.class), null);
    
    GetSecretValueResult result = mock(GetSecretValueResult.class);
    when(client.getSecretValue(any(GetSecretValueRequest.class)))
      .thenReturn(result);
    when(result.getSecretString()).thenReturn("Value!");
    assertEquals("Value!", provider.getSecretString("key"));
    
    when(result.getSecretString()).thenReturn(null);
    when(result.getSecretBinary()).thenReturn(ByteBuffer.wrap(
        Base64.encode("ByteValue!".getBytes())));
    assertEquals("ByteValue!", provider.getSecretString("key"));
    
    try {
      when(client.getSecretValue(any(GetSecretValueRequest.class)))
        .thenThrow(new ResourceNotFoundException("Boo!"));
      provider.getSecretString("key");
      fail("Expected ConfigurationException");
    } catch (ConfigurationException e) {}
    
    provider.close();
  }
  
  @Test
  public void getSecretBytes() throws Exception {
    AWSSecretsProvider provider = new AWSSecretsProvider();
    provider.initialize(mock(ProviderFactory.class), config, 
        mock(HashedWheelTimer.class), null);
    
    GetSecretValueResult result = mock(GetSecretValueResult.class);
    when(client.getSecretValue(any(GetSecretValueRequest.class)))
      .thenReturn(result);
    when(result.getSecretString()).thenReturn("Value!");
    assertArrayEquals("Value!".getBytes(Const.UTF8_CHARSET), 
        provider.getSecretBytes("key"));
    
    when(result.getSecretString()).thenReturn(null);
    when(result.getSecretBinary()).thenReturn(ByteBuffer.wrap(
        Base64.encode("ByteValue!".getBytes())));
    assertArrayEquals("ByteValue!".getBytes(Const.UTF8_CHARSET), 
        provider.getSecretBytes("key"));
    
    try {
      when(client.getSecretValue(any(GetSecretValueRequest.class)))
        .thenThrow(new ResourceNotFoundException("Boo!"));
      provider.getSecretBytes("key");
      fail("Expected ConfigurationException");
    } catch (ConfigurationException e) {}
    
    provider.close();
  }
}

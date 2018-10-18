// This file is part of OpenTSDB.
// Copyright (C) 2015-2018  The OpenTSDB Authors.
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
package net.opentsdb.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.api.gax.rpc.StatusCode.Code;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;

import net.opentsdb.core.MockTSDB;
import net.opentsdb.data.TimeSeriesDatum;
import net.opentsdb.data.TimeSeriesSharedTagsAndTimeData;
import net.opentsdb.query.serdes.SerdesOptions;
import net.opentsdb.query.serdes.TimeSeriesDataSerdes;
import net.opentsdb.stats.Span;
import net.opentsdb.storage.WriteStatus.WriteState;
import net.opentsdb.utils.UnitTestException;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ PubSubWriter.class, GoogleCredentials.class, 
  FileInputStream.class, Publisher.Builder.class, Publisher.class })
public class TestPubSubWriter {

  private MockTSDB tsdb;
  private TimeSeriesDataSerdes serdes;
  private Publisher.Builder pub_builder;
  private Publisher publisher;
  
  @Before
  public void before() throws Exception {
    PowerMockito.mockStatic(FileInputStream.class);
    PowerMockito.whenNew(FileInputStream.class).withAnyArguments()
      .thenReturn(mock(FileInputStream.class));
    PowerMockito.mockStatic(GoogleCredentials.class);
    PowerMockito.when(GoogleCredentials.fromStream(any(InputStream.class)))
      .thenReturn(mock(GoogleCredentials.class));
    PowerMockito.mockStatic(Publisher.Builder.class);
    PowerMockito.mockStatic(Publisher.class);
    
    tsdb = new MockTSDB();
    serdes = mock(TimeSeriesDataSerdes.class);
    when(tsdb.registry.getDefaultPlugin(TimeSeriesDataSerdes.class))
      .thenReturn(serdes);
    
    pub_builder = mock(Publisher.Builder.class);
    publisher = mock(Publisher.class);
    
    when(Publisher.newBuilder(any(TopicName.class))).thenReturn(pub_builder);
    when(pub_builder.setCredentialsProvider(any(CredentialsProvider.class)))
      .thenReturn(pub_builder);
    when(pub_builder.build()).thenReturn(publisher);
  }
  
  @Test
  public void initialize() throws Exception {
    PubSubWriter writer = new PubSubWriter();
    writer.registerConfigs(tsdb);
    
    tsdb.config.override(PubSubWriter.PROJECT_NAME_KEY, "MyProject");
    tsdb.config.override(PubSubWriter.TOPIC_KEY, "Test");
    tsdb.config.override(PubSubWriter.JSON_KEYFILE_KEY, "MyKey");
    
    assertNull(writer.initialize(tsdb, null).join());
    assertSame(publisher, writer.publisher);
    assertSame(serdes, writer.serdes);
    assertEquals("Test", writer.topic.getTopic());
    assertEquals("MyProject", writer.topic.getProject());
    
    // bad settings
    try {
      tsdb.config.override(PubSubWriter.PROJECT_NAME_KEY, null);
      writer = new PubSubWriter();
      writer.initialize(tsdb, null).join();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      tsdb.config.override(PubSubWriter.PROJECT_NAME_KEY, "");
      writer = new PubSubWriter();
      writer.initialize(tsdb, null).join();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    tsdb.config.override(PubSubWriter.PROJECT_NAME_KEY, "MyProject");
    try {
      tsdb.config.override(PubSubWriter.TOPIC_KEY, null);
      writer = new PubSubWriter();
      writer.initialize(tsdb, null).join();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      tsdb.config.override(PubSubWriter.TOPIC_KEY, "");
      writer = new PubSubWriter();
      writer.initialize(tsdb, null).join();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    tsdb.config.override(PubSubWriter.TOPIC_KEY, "Test");
    try {
      tsdb.config.override(PubSubWriter.JSON_KEYFILE_KEY, null);
      writer = new PubSubWriter();
      writer.initialize(tsdb, null).join();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      tsdb.config.override(PubSubWriter.JSON_KEYFILE_KEY, "");
      writer = new PubSubWriter();
      writer.initialize(tsdb, null).join();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    tsdb.config.override(PubSubWriter.JSON_KEYFILE_KEY, "MyKey");
    
    // no serdes
    when(tsdb.registry.getDefaultPlugin(TimeSeriesDataSerdes.class))
      .thenReturn(null);
    try {
      writer = new PubSubWriter();
      writer.initialize(tsdb, null).join();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void writeDatum() throws Exception {
    PubSubWriter writer = new PubSubWriter();
    writer.registerConfigs(tsdb);
    tsdb.config.override(PubSubWriter.PROJECT_NAME_KEY, "MyProject");
    tsdb.config.override(PubSubWriter.TOPIC_KEY, "Test");
    tsdb.config.override(PubSubWriter.JSON_KEYFILE_KEY, "MyKey");
    writer.initialize(tsdb, null);
    
    TimeSeriesDatum datum = mock(TimeSeriesDatum.class);
    doAnswer(new Answer<ApiFuture<String>>() {
      @Override
      public ApiFuture<String> answer(InvocationOnMock invocation)
          throws Throwable {
        return ApiFutures.immediateFuture("1");
      }
    }).when(publisher).publish(any(PubsubMessage.class));
    assertEquals(WriteState.OK, 
        writer.write(null, datum, null).join().state());
    
    // API error
    doAnswer(new Answer<ApiFuture<String>>() {
      @Override
      public ApiFuture<String> answer(InvocationOnMock invocation)
          throws Throwable {
        StatusCode status = mock(StatusCode.class);
        when(status.getCode()).thenReturn(Code.ABORTED);
        return ApiFutures.immediateFailedFuture(
            new ApiException("Boo!", null, status, false));
      }
    }).when(publisher).publish(any(PubsubMessage.class));
    assertEquals(WriteState.ERROR, 
        writer.write(null, datum, null).join().state());
    
    // API retryable
    doAnswer(new Answer<ApiFuture<String>>() {
      @Override
      public ApiFuture<String> answer(InvocationOnMock invocation)
          throws Throwable {
        StatusCode status = mock(StatusCode.class);
        when(status.getCode()).thenReturn(Code.ABORTED);
        return ApiFutures.immediateFailedFuture(
            new ApiException("Boo!", null, status, true));
      }
    }).when(publisher).publish(any(PubsubMessage.class));
    assertEquals(WriteState.RETRY, 
        writer.write(null, datum, null).join().state());
    
    // other exception
    doAnswer(new Answer<ApiFuture<String>>() {
      @Override
      public ApiFuture<String> answer(InvocationOnMock invocation)
          throws Throwable {
        return ApiFutures.immediateFailedFuture(
            new UnitTestException());
      }
    }).when(publisher).publish(any(PubsubMessage.class));
    assertEquals(WriteState.ERROR, 
        writer.write(null, datum, null).join().state());
    
    doThrow(new UnitTestException()).when(serdes)
    .serialize(any(SerdesOptions.class), any(TimeSeriesDatum.class), 
        any(OutputStream.class), any(Span.class));
    assertEquals(WriteState.ERROR, 
        writer.write(null, datum, null).join().state());
  }
  
  @Test
  public void writeSharedData() throws Exception {
    PubSubWriter writer = new PubSubWriter();
    writer.registerConfigs(tsdb);
    tsdb.config.override(PubSubWriter.PROJECT_NAME_KEY, "MyProject");
    tsdb.config.override(PubSubWriter.TOPIC_KEY, "Test");
    tsdb.config.override(PubSubWriter.JSON_KEYFILE_KEY, "MyKey");
    writer.initialize(tsdb, null);
    
    TimeSeriesSharedTagsAndTimeData data = mock(TimeSeriesSharedTagsAndTimeData.class);
    when(data.size()).thenReturn(4);
    doAnswer(new Answer<ApiFuture<String>>() {
      @Override
      public ApiFuture<String> answer(InvocationOnMock invocation)
          throws Throwable {
        return ApiFutures.immediateFuture("1");
      }
    }).when(publisher).publish(any(PubsubMessage.class));
    List<WriteStatus> result = writer.write(null, data, null).join();
    assertEquals(4, result.size());
    assertEquals(WriteState.OK, result.get(0).state());
    
    // API error
    doAnswer(new Answer<ApiFuture<String>>() {
      @Override
      public ApiFuture<String> answer(InvocationOnMock invocation)
          throws Throwable {
        StatusCode status = mock(StatusCode.class);
        when(status.getCode()).thenReturn(Code.ABORTED);
        return ApiFutures.immediateFailedFuture(
            new ApiException("Boo!", null, status, false));
      }
    }).when(publisher).publish(any(PubsubMessage.class));
    result = writer.write(null, data, null).join();
    assertEquals(4, result.size());
    assertEquals(WriteState.ERROR, result.get(0).state());
    
    // API retryable
    doAnswer(new Answer<ApiFuture<String>>() {
      @Override
      public ApiFuture<String> answer(InvocationOnMock invocation)
          throws Throwable {
        StatusCode status = mock(StatusCode.class);
        when(status.getCode()).thenReturn(Code.ABORTED);
        return ApiFutures.immediateFailedFuture(
            new ApiException("Boo!", null, status, true));
      }
    }).when(publisher).publish(any(PubsubMessage.class));
    result = writer.write(null, data, null).join();
    assertEquals(4, result.size());
    assertEquals(WriteState.RETRY, result.get(0).state());
    
    // other exception
    doAnswer(new Answer<ApiFuture<String>>() {
      @Override
      public ApiFuture<String> answer(InvocationOnMock invocation)
          throws Throwable {
        return ApiFutures.immediateFailedFuture(
            new UnitTestException());
      }
    }).when(publisher).publish(any(PubsubMessage.class));
    result = writer.write(null, data, null).join();
    assertEquals(4, result.size());
    assertEquals(WriteState.ERROR, result.get(0).state());
    
    doThrow(new UnitTestException()).when(serdes)
    .serialize(any(SerdesOptions.class), any(TimeSeriesDatum.class), 
        any(OutputStream.class), any(Span.class));
    result = writer.write(null, data, null).join();
    assertEquals(4, result.size());
    assertEquals(WriteState.ERROR, result.get(0).state());
  }
}

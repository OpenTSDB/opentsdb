package net.opentsdb.auth;
/**
 * Copyright 2015 The opentsdb Authors
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public class EmbeddedAccessKeyPair {
  private String accessKey;
  private String accessSecretKey;

  public EmbeddedAccessKeyPair(String accessKey, String accessSecretKey) {
    this.accessKey = accessKey;
    this.accessSecretKey = accessSecretKey;
  }

  public String getAccessKey() {
    return accessKey;
  }

  public String getAccessSecretKey() {
    return accessSecretKey;
  }

  public String toString() {
    return getAccessKey() + ":" + getAccessSecretKey();
  }
}

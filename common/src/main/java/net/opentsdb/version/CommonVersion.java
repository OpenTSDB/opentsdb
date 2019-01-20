// This file is part of OpenTSDB.
// Copyright (C) 2019  The OpenTSDB Authors.
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
package net.opentsdb.version;

import java.io.IOException;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * A utility that pulls Version and Git information from the build using the
 * pl.project13.maven:git-commit-id-plugin plugin. Make sure that's included
 * in the pom.xml.
 * 
 * @since 3.0
 */
public class CommonVersion {
  private static Logger LOG = LoggerFactory.getLogger(CommonVersion.class);
  
  public static final String RESOURCE = "opentsdb-common-version.json";
  public static JsonNode JSON;
  static {
    final InputStream stream = CommonVersion.class.getResourceAsStream(RESOURCE);
    if (stream == null) {
      LOG.error("No such resource: " + RESOURCE);
    } else {
      try {
        JSON = new ObjectMapper().readTree(stream);
      } catch (IOException e) {
        LOG.error("Unable to parse resource: " + RESOURCE, e);
      }
    }
  }
  
  private CommonVersion() { }
  
  /** @return The full JSON node. */
  public static JsonNode json() {
    return JSON;
  }
  
  /** @return The Maven version of this component. */
  public static String version() {
    return JSON == null ? null : JSON.get("git.build.version").asText();
  }
  
  /** @return The Git branch this was built from. */
  public static String gitBranch() {
    return JSON == null ? null : JSON.get("git.branch").asText();
  }
  
  /** @return The hostname where this package was built. */
  public static String gitHost() {
    return JSON == null ? null : JSON.get("git.build.host").asText();
  }
  
  /** @return The build timestamp. */
  public static String gitBuildTime() {
    return JSON == null ? null : JSON.get("git.build.time").asText();
  }
  
  /** @return The email of the user who built this package (picked up from Git). */
  public static String gitBuildUserEmail() {
    return JSON == null ? null : JSON.get("ggit.build.user.email").asText();
  }
  
  /** @return The name of the user who built this package (picked up from Git). */
  public static String gitBuildUserName() {
    return JSON == null ? null : JSON.get("git.build.user.name").asText();
  }
  
  /** @return The Git commit ID when this was built. */
  public static String gitCommitId() {
    return JSON == null ? null : JSON.get("git.commit.id").asText();
  }
  
  /** @return The short ID of the Git commit when this was built. */
  public static String gitCommitIdShort() {
    return JSON == null ? null : JSON.get("git.commit.id.abbrev").asText();
  }
  
  /** @return The full Git commit message. */
  public static String gitCommitMessageFull() {
    return JSON == null ? null : JSON.get("git.commit.message.full").asText();
  }
  
  /** @return The short Git commit message. */
  public static String gitCommitMessageShort() {
    return JSON == null ? null : JSON.get("git.commit.message.short").asText();
  }
  
  /** @return Whether or not uncommitted changes were present when building. */
  public static boolean gitDirty() {
    return JSON == null ? false : JSON.get("git.dirty").asBoolean();
  }
  
  /** @return The original repository URL this Git clone came from. */
  public static String gitRemoteOriginUrl() {
    return JSON == null ? null : JSON.get("git.remote.origin.url").asText();
  }
  
  /** @return Tags associated with this Git release. */
  public static String gitTags() {
    return JSON == null ? null : JSON.get("git.tags").asText();
  }
}

package net.opentsdb.auth;
// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.


import com.stumbleupon.async.Deferred;
import net.opentsdb.core.TSDB;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.utils.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

/**
 * @since 2.3
 */
public class EmbeddedAuthenticationPlugin extends AuthenticationPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(EmbeddedAuthenticationPlugin.class);
  private Map<String, String> authDB = new HashMap();
  private String adminAccessKey = null;
  private String adminSecretKey = null;


  public void storeCredentials(EmbeddedAccessKeyPair keyPair) {
    authDB.put(keyPair.getAccessKey(), keyPair.getAccessSecretKey());
  }

  public void storeCredentials(String accessKey, String accessSecretKey) {
    authDB.put(accessKey, accessSecretKey);
  }

  public void removeCredentials(String adminAccessKey, String adminSecretKey, EmbeddedAccessKeyPair keyPair) {
    if (authenticateAdmin(adminAccessKey, adminSecretKey)) {
      authDB.remove(keyPair.getAccessKey());
    }
  }

  public void removeCredentials(String adminAccessKey, String adminSecretKey, String accessKey) {
    if (authenticateAdmin(adminAccessKey, adminSecretKey)) {
      authDB.remove(accessKey);
    }
  }

  @Override
  public void initialize(TSDB tsdb) {
    LOG.debug("Initialized Authentication Plugin");
    this.adminAccessKey = tsdb.getConfig().getString("tsd.core.authentication.admin_access_key");
    this.adminSecretKey = tsdb.getConfig().getString("tsd.core.authentication.admin_access_secret");
    storeCredentials(this.adminAccessKey, this.adminSecretKey);
    LOG.debug("Created keyPair:" + generateAccessToken(this.adminAccessKey, this.adminSecretKey).toString());
  }

  @Override
  public Deferred<Object> shutdown() {
    return null;
  }

  @Override
  public String version() {
    return "2.3.0";
  }

  @Override
  public void collectStats(StatsCollector collector) {

  }

  @Override
  public Boolean authenticate(String accessKey, Map fields) {
    try {
      AuthenticationUtil.validateFields(fields);
      String providedDigest = (String) fields.get("digest");
      LOG.debug("Authenticating " + accessKey + " " + providedDigest);
      Long providedTimestamp = DateTime.parseDateTimeString((String) fields.get("date"), "UTC");
      Long minimumTimestamp = DateTime.parseDateTimeString("20m-ago", "UTC");
      if (providedTimestamp < minimumTimestamp) {
        throw new IllegalArgumentException("Provided timestamp: " + (String) fields.get("date") + " is too old.");
      } else {
        String calculatedDigest = AuthenticationUtil.createDigest(new EmbeddedAccessKeyPair(accessKey, authDB.get(accessKey)), fields);
        LOG.debug("Calc: " + calculatedDigest);
        LOG.debug("Prov: " + providedDigest);
        return AuthenticationUtil.validateCredentials(accessKey, calculatedDigest, accessKey, providedDigest);
      }
    } catch (Exception e) {
      LOG.error("Exception: " + e);
      return false;
    }
  }

  @Override
  public Boolean authenticate(String providedAccessKey, String providedSecretKey) {
    String correctSecretKey = (String) authDB.get(providedAccessKey);
    return AuthenticationUtil.validateCredentials(providedAccessKey, correctSecretKey, providedAccessKey, providedSecretKey);
  }

  @Override
  public Boolean authenticateAdmin(String providedAdminAccessKey, String providedAdminSecretKey) {
    return AuthenticationUtil.validateCredentials(this.adminAccessKey, this.adminSecretKey, providedAdminAccessKey,providedAdminSecretKey);
  }

  @Override
  public EmbeddedAccessKeyPair generateAccessToken(String adminAccessKey, String adminSecretKey) {
    if (authenticateAdmin(adminAccessKey, adminSecretKey)) {
      try {
        EmbeddedAccessKeyPair keyPair = AuthenticationUtil.generateKeyPair();
        storeCredentials(keyPair);
        return keyPair;
      } catch (NoSuchAlgorithmException e) {
        LOG.error("NoSuchAlgorithmException: " + e);
      }
    }
    return null;
  }
}

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.DatatypeConverter;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by jcreasy on 3/18/16.
 */
public class Util {
  private static final Logger LOG = LoggerFactory.getLogger(Util.class);
  public static final String algo = "HmacSHA256";

  public static final String hmacDigest(String access_key, String access_key_secret) {
    LOG.trace("Generating HASH for " + access_key + " " + access_key_secret.getClass().toString());
    LOG.debug("Generating HASH for " + access_key);
    String digest = null;
    try {
      SecretKeySpec key = new SecretKeySpec((access_key_secret).getBytes("UTF-8"), algo);
      Mac mac = Mac.getInstance(algo);
      mac.init(key);

      byte[] bytes = mac.doFinal(access_key.getBytes("ASCII"));

      StringBuffer hash = new StringBuffer();
      for (int i = 0; i < bytes.length; i++) {
        String hex = Integer.toHexString(0xFF & bytes[i]);
        if (hex.length() == 1) {
          hash.append('0');
        }
        hash.append(hex);
      }
      digest = hash.toString();
    } catch (UnsupportedEncodingException e) {
      LOG.error("UnsupportedEncodingException: " + e);
    } catch (InvalidKeyException e) {
      LOG.error("InvalidKeyException: " + e);
    } catch (NoSuchAlgorithmException e) {
      LOG.error("NoSuchAlgorithmException: " + e);
    }
    return digest;
  }

  public static final String createDigest(EmbeddedAccessKeyPair keyPair, Map fields) {
    String digest = hmacDigest(keyPair.getAccessKey(), keyPair.getAccessSecretKey() + (String) fields.get("date") + (String) fields.get("noonce"));
    LOG.trace("got hash: " + digest);
    return digest;
  }

  public static final Map<String, String> createFields(final String input) {
    final Map<String, String> map = new HashMap<String, String>();
    for (String pair : input.split("&")) {
      String[] kv = pair.split("=");
      map.put(kv[0], kv[1]);
    }
    return map;
  }

  public static final EmbeddedAccessKeyPair generateKeyPair() throws NoSuchAlgorithmException {
    javax.crypto.KeyGenerator generator = javax.crypto.KeyGenerator.getInstance("HMACSHA1");
    generator.init(120);
    byte[] accessKeyRaw = generator.generateKey().getEncoded();
    generator.init(240);
    byte[] accessSecretKeyRaw = generator.generateKey().getEncoded();
    String accessKey = DatatypeConverter.printBase64Binary(accessKeyRaw).replaceAll("[\\r\\n]", "");
    String accessSecretKey = DatatypeConverter.printBase64Binary(accessSecretKeyRaw).replaceAll("[\\r\\n]", "");
    LOG.debug(accessKey);
    LOG.debug(accessSecretKey);
    return new EmbeddedAccessKeyPair(accessKey, accessSecretKey);
  }

  public static Boolean validateCredentials(String correctAccess, String correctSecret, String providedAccess, String providedSecret){
    Boolean secretMatched = correctAccess.equals(providedAccess);
    Boolean keyMatched = correctSecret.equals(providedSecret);
    if (keyMatched && secretMatched) {
      LOG.debug("Authentication Succeeded for: " + providedAccess);
      return true;
    } else {
      LOG.debug("Authentication Failed for: " + providedAccess);
      return false;
    }
  }
}

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
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

public class AuthenticationUtil {
  private static final Logger LOG = LoggerFactory.getLogger(AuthenticationUtil.class);
  public static final String algo = "HmacSHA256";

  private static Mac generateMAC(String accessKeySecret) throws NoSuchAlgorithmException, InvalidKeyException {
    SecretKeySpec signingKey = new SecretKeySpec(accessKeySecret.getBytes(), algo);
    Mac mac = Mac.getInstance(algo);
    mac.init(signingKey);
    return mac;
  }

  private static String generateSaltedHMAC(String accessKey, String userSalt, Mac mac) throws UnsupportedEncodingException {
    byte[] bytes = mac.doFinal((accessKey + ":" + userSalt).getBytes("ASCII"));
    return DatatypeConverter.printBase64Binary(bytes);
  }

  private static String generateDigestHMAC(String saltedHMAC, String nonce) throws NoSuchAlgorithmException {
    MessageDigest md = MessageDigest.getInstance("SHA-256");
    md.update((saltedHMAC + ":" + nonce).getBytes());
    byte[] bytes = md.digest();
    return DatatypeConverter.printBase64Binary(bytes);
  }

  private static String hmacDigest(String accessKey, String accessKeySecret, String userSalt, String nonce) {
    String digest = null;
    try {
      Mac mac = generateMAC(accessKeySecret);
      String saltedHMAC = generateSaltedHMAC(accessKey, userSalt, mac);
      digest = generateDigestHMAC(saltedHMAC, nonce);
    } catch (UnsupportedEncodingException e) {
      LOG.error("UnsupportedEncodingException: " + e);
    } catch (InvalidKeyException e) {
      LOG.error("InvalidKeyException: " + e);
    } catch (NoSuchAlgorithmException e) {
      LOG.error("NoSuchAlgorithmException: " + e);
    }
    return digest;
  }

  public static void validateFields(Map<String, String> fields) throws IllegalArgumentException {
    if (!fields.containsKey("digest")) {
      throw new IllegalArgumentException(("digest is a required field"));
    }
    if (fields.containsKey("date") && !fields.containsKey("salt")) {
      fields.put("salt", fields.get("date"));
    }
    if (!fields.containsKey("salt")) {
      throw new IllegalArgumentException(("salt is a required field"));
    }
    if (!fields.containsKey("nonce")) {
      throw new IllegalArgumentException(("nonce is a required field"));
    }
  }

  public static String createDigest(EmbeddedAccessKeyPair keyPair, Map fields) throws IllegalArgumentException {
    String digest = null;
    if (fields.containsKey("date") && !fields.containsKey("salt")) {
      fields.put("salt", fields.get("date"));
    }
    if ((fields.containsKey("salt") && fields.containsKey("nonce"))) {
      digest = hmacDigest(keyPair.getAccessKey(), keyPair.getAccessSecretKey(), (String) fields.get("salt"), (String) fields.get("nonce"));
      LOG.trace("got hash: " + digest);
    } else {
      throw new IllegalArgumentException("(date or salt) and nonce are required fields");
    }
    return digest;
  }

  public static Map<String, String> createFields(final String input) {
    final Map<String, String> map = new HashMap<String, String>();
    for (String pair : input.split("&")) {
      String[] kv = pair.split("=");
      map.put(kv[0], kv[1]);
    }
    return map;
  }

  public static Map<String, String> stringToMap(String source, String delimiter) throws IllegalArgumentException {
    String[] fieldsArray = source.split(delimiter);
    Map<String, String> fields = new HashMap();
    if (fieldsArray.length == 4) {
      fields.put("accessKey", fieldsArray[0]);
      fields.put("digest", fieldsArray[1]);
      fields.put("date", fieldsArray[2]);
      fields.put("nonce", fieldsArray[3]);
    } else {
      throw new IllegalArgumentException("Improperly formatted Authorization Header: " + source);
    }
    return fields;
  }

  public static EmbeddedAccessKeyPair generateKeyPair() throws NoSuchAlgorithmException {
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

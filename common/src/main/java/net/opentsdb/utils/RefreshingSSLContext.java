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
package net.opentsdb.utils;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringReader;
import java.math.BigInteger;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.RSAPrivateKeySpec;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.xml.bind.DatatypeConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.io.Files;

import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import net.opentsdb.common.Const;
import net.opentsdb.core.TSDB;
import sun.security.provider.certpath.X509CertPath;
import sun.security.util.DerInputStream;
import sun.security.util.DerValue;

/**
 * First stab at a refreshing SSL context that watches the given params for
 * updates and reloads the context on changes. Note that if the interval is set
 * to 0 then refreshing is disabled. Makes it useful for servers that load once.
 * 
 * @since 3.0
 */
public class RefreshingSSLContext implements TimerTask {
  private static Logger LOG = LoggerFactory.getLogger(RefreshingSSLContext.class);
  
  private static CertificateFactory CA_FACTORY;
  
  public static enum SourceType {
    FILES,
    KEYSTORE,
    SECRET
  }
  
  private final Builder builder;
  private long last_hash;
  private volatile SSLContext context;
  
  private RefreshingSSLContext(final Builder builder) {
    if (builder.type == null) {
      throw new IllegalArgumentException("Type cannot be null.");
    }
    
    try {
      CA_FACTORY = CertificateFactory.getInstance("X.509");
    } catch (CertificateException e) {
      throw new IllegalStateException(e);
    }
    
    switch (builder.type) {
    case FILES:
      if (Strings.isNullOrEmpty(builder.cert)) {
        throw new IllegalArgumentException("The certificate path cannot be "
            + "null or empty.");
      }
      break;
    case KEYSTORE:
      if (Strings.isNullOrEmpty(builder.keystore)) {
        throw new IllegalArgumentException("The keystore path cannot be "
            + "null or empty.");
      }
      if (Strings.isNullOrEmpty(builder.keystore_pass)) {
        throw new IllegalArgumentException("The keystor password cannot be "
            + "null or empty.");
      }
      break;
    case SECRET:
      if (Strings.isNullOrEmpty(builder.secret_cert)) {
        throw new IllegalArgumentException("The secret certificate key cannot be "
            + "null or empty.");
      }
      break;
    }
    this.builder = builder;
    
    try {
      run(null);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }
  
  public synchronized SSLContext context() {
    return context;
  }
  
  public synchronized long hash() {
    return last_hash;
  }

  public long forceRefresh() {
    try {
      switch (builder.type) {
      case FILES:
        runKeyAndCert();
        break;
      case KEYSTORE:
        runKeystore();
        break;
      case SECRET:
        runSecrets();
        break;
      }
    } catch (Throwable t) {
      throw new RuntimeException("Failed to refresh SSLContext", t);
    }
    return last_hash;
  }
  
  @Override
  public void run(final Timeout timeout) throws Exception {
    try {
      switch (builder.type) {
      case FILES:
        runKeyAndCert();
        break;
      case KEYSTORE:
        runKeystore();
        break;
      case SECRET:
        runSecrets();
        break;
      }
    } catch (Throwable t) {
      LOG.error("Failed to refresh SSLContext: " + this, t);
    }
    
    if (builder.interval < 1) {
      return; // not scheduling it.
    }
    
    builder.tsdb.getMaintenanceTimer().newTimeout(this, 
        builder.interval, 
        TimeUnit.MILLISECONDS);
  }
  
  void runKeystore() throws FileNotFoundException, IOException, 
      KeyStoreException, NoSuchAlgorithmException, CertificateException, 
      UnrecoverableKeyException, KeyManagementException {
    final File file = new File(builder.keystore);
    if (!file.exists()) {
      throw new IllegalArgumentException("No keystore file found at " 
          + builder.keystore);
    }
    final long hash;
    try {
      hash = Files.asByteSource(file).hash(Const.HASH_FUNCTION()).asLong();
    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to open keystore found at " 
          + builder.keystore, e);
    }
    
    // if the hash is the same, we're done.
    if (context != null && hash == last_hash) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Keystore hash was the same as the last load: " + builder.keystore);
      }
      return;
    }
    
    LOG.info("Attempting to load keystore: " + builder.keystore);
    
    // load an initialize the keystore.
    try(final FileInputStream stream = new FileInputStream(file)) {
      final KeyStore keystore = KeyStore.getInstance(KeyStore.getDefaultType());
      keystore.load(stream, builder.keystore_pass.toCharArray());
      
      // initialize a key manager to pass to the SSL context using the keystore.
      final KeyManagerFactory key_factory = KeyManagerFactory.getInstance(
          KeyManagerFactory.getDefaultAlgorithm());
      key_factory.init(keystore, builder.keystore_pass.toCharArray());
  
      // init a trust manager so we can use the public cert.
      final TrustManagerFactory trust_factory = 
          TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trust_factory.init(keystore);
      final TrustManager[] trustManagers = trust_factory.getTrustManagers();
      
      final SSLContext sslContext = SSLContext.getInstance("TLS"); 
      sslContext.init(key_factory.getKeyManagers(), trustManagers, null);
      synchronized (this) {
        context =  sslContext;
        last_hash = hash;
      }
      if (builder.callback != null) {
        builder.callback.refresh(context);
      }
      LOG.info("Successfully loaded keystore: " + builder.keystore);
    }
  }

  void runKeyAndCert() throws IOException, GeneralSecurityException {
    
    List<HashCode> hashes = Lists.newArrayListWithExpectedSize(3);
    if (!Strings.isNullOrEmpty(builder.key)) {
      final File key = new File(builder.key);
      hashes.add(Files.asByteSource(key).hash(Const.HASH_FUNCTION()));
    }
    final File pub_cert = new File(builder.cert);
    hashes.add(Files.asByteSource(pub_cert).hash(Const.HASH_FUNCTION()));
    if (!Strings.isNullOrEmpty(builder.ca)) {
      final File ca = new File(builder.ca);
      hashes.add(Files.asByteSource(ca).hash(Const.HASH_FUNCTION()));
    }
    final long hash = Hashing.combineOrdered(hashes).asLong();
    // if the hash is the same, we're done.
    if (context != null && hash == last_hash) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Keystore hash was the same as the last load: " + builder.keystore);
      }
      return;
    }
    
    RSAPrivateKey key = null;
    if (!Strings.isNullOrEmpty(builder.key)) {
      String raw_key = Files.asCharSource(new File(builder.key), Charsets.UTF_8).read();
      if (Strings.isNullOrEmpty(raw_key)) {
        throw new IllegalStateException("The key at location " 
            + builder.key + " was null or empty.");
      }
      if (raw_key.contains("BEGIN PRIVATE KEY")) {
        key = parsePKCS8Key(trimPrivateKey(raw_key));
      } else if (raw_key.contains("BEGIN RSA PRIVATE KEY")) {
        key = parsePKCS1Key(trimPrivateKey(raw_key));
      } else {
        throw new IllegalStateException("Unrecognized key at location " 
            + builder.key);
      }
      LOG.info("Successfully loaded private key from file: " + builder.key);
    }
    
    List<String> parts;
    final KeyStore keystore;
    final List<Certificate> certificates;
    final String keystore_pass = Long.toString(System.currentTimeMillis());
    int cert_id = 0;
    
    final String raw_certs = Files.toString(
        new File(builder.cert), Charsets.UTF_8);
    parts = splitPem(raw_certs);
    if (parts.size() < 1) {
      throw new IllegalStateException("No certificates found in path: " 
          + builder.cert);
    }
    
    keystore = KeyStore.getInstance("JKS");
    keystore.load(null);
    certificates = Lists.newArrayList();
    boolean have_server_cert = false;
    for (final String part : parts) {
      if (part.contains("BEGIN PRIVATE KEY")) {
        if (key != null) {
          throw new RuntimeException("Already loaded a key but " 
              + builder.cert + " also had a key.");
        }
        key = parsePKCS8Key(trimPrivateKey(part));
      } else if (part.contains("BEGIN RSA PRIVATE KEY")) {
        if (key != null) {
          throw new RuntimeException("Already loaded a key but " 
              + builder.cert + " also had a key.");
        }
        key = parsePKCS1Key(trimPrivateKey(part));
      } else {
        X509Certificate cert = parseCert(trimCertificate(part));
        certificates.add(cert);
        String cn = getCN(cert);
        boolean is_server_cert = isServerCert(cert);
        if (have_server_cert && is_server_cert) {
          throw new IllegalStateException("Multiple server certs in "
              + "the PEM are not allowed.");
        } else if (is_server_cert) {
          have_server_cert = is_server_cert;
          LOG.info("Successfully loaded server certificate with CN:: " + cn);
        } else {
          LOG.info("Successfully loaded intermediate or CA cert with CN: " + cn);
        }
        keystore.setCertificateEntry(Integer.toString(cert_id++), cert);
      }
    }
    
    if (certificates.isEmpty()) {
      throw new IllegalStateException("No certificates loaded from: " 
          + builder.cert);
    }
    Certificate[] cert_array = new Certificate[certificates.size()];
    certificates.toArray(cert_array);
    keystore.setKeyEntry("key-alias", 
                         key, 
                         keystore_pass.toCharArray(),
                         cert_array);
    
    if (!Strings.isNullOrEmpty(builder.ca)) {
      if (new File(builder.ca).isDirectory()) {
        // TODO - load all the certs in the dir
        throw new UnsupportedOperationException("We don't support "
            + "loading directories yet.");
      } else {
        final String raw_cas = Files.toString(new File(builder.ca), 
            Charsets.UTF_8);
        parts = splitPem(raw_cas);
        for (final String part : parts) {
          X509Certificate cert = parseCert(trimCertificate(part));
          certificates.add(cert);
          String cn = getCN(cert);
          keystore.setCertificateEntry(Integer.toString(cert_id++), cert);
          LOG.info("Successfully loaded CA cert with CN: " + cn);
        }
      }
    }
    
    // initialize a key manager to pass to the SSL context using the keystore.
    final KeyManagerFactory key_factory = KeyManagerFactory.getInstance(
        KeyManagerFactory.getDefaultAlgorithm());
    key_factory.init(keystore, keystore_pass.toCharArray());

    // init a trust manager so we can use the public cert.
    final TrustManagerFactory trust_factory = 
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    trust_factory.init(keystore);
    final TrustManager[] trustManagers = trust_factory.getTrustManagers();
    
    final SSLContext ssl_context = SSLContext.getInstance("TLS"); 
    ssl_context.init(key_factory.getKeyManagers(), trustManagers, null);
    synchronized (this) {
      context = ssl_context;
      last_hash = hash;
    }
    if (builder.callback != null) {
      builder.callback.refresh(context);
    }
    LOG.info("Successfully loaded certificate from: " + builder.cert);
  }
  
  void runSecrets() throws IOException, GeneralSecurityException {
    final Object cert_secret = builder.tsdb.getConfig().getSecretObject(
        builder.secret_cert);
    if (cert_secret == null) {
      throw new IllegalStateException("The secret object cannot be null: " 
          + builder.secret_cert);
    }
    
    final Hasher hasher = Const.HASH_FUNCTION().newHasher();
    
    RSAPrivateKey key = null;
    if (!Strings.isNullOrEmpty(builder.secret_key)) {
      final Object key_secret = builder.tsdb.getConfig().getSecretObject(
          builder.secret_key);
      if (key_secret != null) {
        if (key_secret instanceof RSAPrivateKey) {
          key = (RSAPrivateKey) key_secret;
          hasher.putBytes(key.getEncoded());
        } else if (key_secret instanceof String || key_secret instanceof byte[]) {
          final String raw_key;
          if (key_secret instanceof String) {
            raw_key = (String) key_secret;
          } else {
            raw_key = new String((byte[]) key_secret, Const.UTF8_CHARSET);
          }
          if (raw_key.contains("BEGIN PRIVATE KEY")) {
            key = parsePKCS8Key(trimPrivateKey(raw_key));
          } else if (raw_key.contains("BEGIN RSA PRIVATE KEY")) {
            key = parsePKCS1Key(trimPrivateKey(raw_key));
          } else {
            throw new IllegalStateException("Unrecognized key: " 
                + builder.secret_key);
          }
          hasher.putString(raw_key, Const.UTF8_CHARSET);
        } else {
          throw new IllegalArgumentException("Unknown private key object type: " 
              + key_secret.getClass().getCanonicalName().toString());
        }
        LOG.info("Successfully loaded private key from secret: "+  
            builder.secret_key);
      } else {
        throw new IllegalArgumentException("Private key from secret was null: " 
            + builder.secret_key);
      }
    }
    
    List<String> parts;
    KeyStore keystore = null;
    List<Certificate> certificates = null;
    final String keystore_pass = Long.toString(System.currentTimeMillis());
    int cert_id = 0;
    
    final String raw_certs;
    if (cert_secret instanceof String) {
      raw_certs = (String) cert_secret;
      hasher.putString(raw_certs, Const.UTF8_CHARSET);
    } else if (cert_secret instanceof byte[]) {
      raw_certs = new String((byte[]) cert_secret, Const.UTF8_CHARSET);
      hasher.putString(raw_certs, Const.UTF8_CHARSET);
    } else if (cert_secret instanceof X509CertPath) {
      raw_certs = null;
      certificates = Lists.newArrayList();
      keystore = KeyStore.getInstance("JKS");
      keystore.load(null);
      X509CertPath p = (X509CertPath) cert_secret;
      hasher.putBytes(p.getEncoded());
      boolean have_server_cert = false;
      for (final X509Certificate c : p.getCertificates()) {
        certificates.add(c);
        String cn = getCN(c);
        boolean is_server_cert = isServerCert(c);
        if (have_server_cert && is_server_cert) {
          throw new IllegalStateException("Multiple server certs in "
              + "the PEM are not allowed.");
        } else if (is_server_cert) {
          have_server_cert = is_server_cert;
          LOG.info("Successfully loaded server certificate with CN:: " + cn);
        } else {
          LOG.info("Successfully loaded intermediate or CA cert with CN: " + cn);
        }
        keystore.setCertificateEntry(Integer.toString(cert_id++), c);
      }
      
    } else {
      throw new IllegalArgumentException("Unrecognized secret class: " 
          + cert_secret.getClass());
    }
    
    if (raw_certs != null) {
      parts = splitPem(raw_certs);
      if (parts.size() < 1) {
        throw new IllegalStateException("No certificates found in secret: " 
            + builder.secret_cert);
      }

      keystore = KeyStore.getInstance("JKS");
      keystore.load(null);
      certificates = Lists.newArrayList();
      boolean have_server_cert = false;
      for (final String part : parts) {
        if (part.contains("BEGIN PRIVATE KEY")) {
          if (key != null) {
            throw new RuntimeException("Already loaded a key but " 
                + builder.secret_cert + " also had a key.");
          }
          key = parsePKCS8Key(trimPrivateKey(part));
        } else if (part.contains("BEGIN RSA PRIVATE KEY")) {
          if (key != null) {
            throw new RuntimeException("Already loaded a key but " 
                + builder.secret_cert + " also had a key.");
          }
          key = parsePKCS1Key(trimPrivateKey(part));
        } else {
          X509Certificate cert = parseCert(trimCertificate(part));
          certificates.add(cert);
          String cn = getCN(cert);
          boolean is_server_cert = isServerCert(cert);
          if (have_server_cert && is_server_cert) {
            throw new IllegalStateException("Multiple server certs in "
                + "the PEM are not allowed.");
          } else if (is_server_cert) {
            have_server_cert = is_server_cert;
            LOG.info("Successfully loaded server certificate with CN:: " + cn);
          } else {
            LOG.info("Successfully loaded intermediate or CA cert with CN: " + cn);
          }
          keystore.setCertificateEntry(Integer.toString(cert_id++), cert);
        }
      }
    }
    
    if (certificates.isEmpty()) {
      throw new IllegalStateException("No certificates loaded from: " 
          + builder.secret_cert);
    }
    Certificate[] cert_array = new Certificate[certificates.size()];
    certificates.toArray(cert_array);
    keystore.setKeyEntry("key-alias", 
                         key, 
                         keystore_pass.toCharArray(),
                         cert_array);
    
    if (!Strings.isNullOrEmpty(builder.ca)) {
      if (new File(builder.ca).isDirectory()) {
        // TODO - load all the certs in the dir
        throw new UnsupportedOperationException("We don't support "
            + "loading directories yet.");
      } else {
        final String raw_cas = Files.asCharSource(new File(builder.ca), 
            Charsets.UTF_8).read();
        hasher.putString(raw_cas, Const.UTF8_CHARSET);
        parts = splitPem(raw_cas);
        for (final String part : parts) {
          X509Certificate cert = parseCert(trimCertificate(part));
          certificates.add(cert);
          String cn = getCN(cert);
          keystore.setCertificateEntry(Integer.toString(cert_id++), cert);
          LOG.info("Successfully loaded CA cert with CN: " + cn);
        }
      }
    }
    
    final long hash = hasher.hash().asLong();
    // if the hash is the same, we're done.
    if (context != null && hash == last_hash) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Keystore hash was the same as the last load: " + builder.keystore);
      }
      return;
    }
    
    // initialize a key manager to pass to the SSL context using the keystore.
    final KeyManagerFactory key_factory = KeyManagerFactory.getInstance(
        KeyManagerFactory.getDefaultAlgorithm());
    key_factory.init(keystore, keystore_pass.toCharArray());

    // init a trust manager so we can use the public cert.
    final TrustManagerFactory trust_factory = 
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    trust_factory.init(keystore);
    final TrustManager[] trustManagers = trust_factory.getTrustManagers();
    
    final SSLContext ssl_context = SSLContext.getInstance("TLS");
    ssl_context.init(key_factory.getKeyManagers(), trustManagers, null);
   
    synchronized (this) {
      context = ssl_context;
      last_hash = hash;
    }
    if (builder.callback != null) {
      builder.callback.refresh(context);
    }
    LOG.info("Successfully loaded certificate from: " + builder.secret_cert);
  }
  
  /**
   * Strips the header and footer from the key.
   * @param key A non-null key.
   * @return The clean key.
   */
  private static String trimPrivateKey(final String key) {
    return key.replace("-----BEGIN PRIVATE KEY-----", "")
              .replace("-----END PRIVATE KEY-----", "")
              .replace("-----BEGIN RSA PRIVATE KEY-----", "")
              .replace("-----END RSA PRIVATE KEY-----", "")
              .trim();
  }
  
  /**
   * Strips the header and footer from the certificate.
   * @param cert A non-null certificate.
   * @return The clean certificate.
   */
  private static String trimCertificate(final String cert) {
    return cert.replace("-----BEGIN CERTIFICATE-----", "")
               .replace("-----END CERTIFICATE-----", "")
               .trim();
  }
  
  /**
   * Parses a PKCS8 formatted private key.
   * @param key A non-null key as a B64 encoded string.
   * @return A private key if parsing was successful.
   * @throws NoSuchAlgorithmException If the JVM is hosed.
   * @throws InvalidKeySpecException If the key wasn't PKCS8 formatted.
   */
  private static RSAPrivateKey parsePKCS8Key(final String key) throws 
      NoSuchAlgorithmException, InvalidKeySpecException {
    final PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(
        DatatypeConverter.parseBase64Binary(key));
    final KeyFactory factory = KeyFactory.getInstance("RSA");
    return (RSAPrivateKey) factory.generatePrivate(spec);
  }
  
  /**
   * Parses a PKCS1 formatted private key using unsafe classes (to avoid
   * pulling in another jar just for this).
   * @param key A non-null key as a B64 encoded string.
   * @return A private key if parsing was successful.
   * @throws IOException If the input was corrupted.
   * @throws GeneralSecurityException If parsing failed because the key
   * appeared to be in the wrong format.
   */
  private static RSAPrivateKey parsePKCS1Key(final String key) throws 
      IOException, GeneralSecurityException {
    final DerInputStream stream = new DerInputStream(
        DatatypeConverter.parseBase64Binary(key));
    final DerValue[] seq = stream.getSequence(0);

    if (seq.length < 9) {
      throw new GeneralSecurityException("Failed parsing the PKCS1 "
          + "formatted key as it didn't have the right number of sequences.");
    }

    final BigInteger modulus = seq[1].getBigInteger();
    final BigInteger private_exponent = seq[3].getBigInteger();
    final RSAPrivateKeySpec spec = 
        new RSAPrivateKeySpec(modulus, private_exponent);
    final KeyFactory factory = KeyFactory.getInstance("RSA");
    return (RSAPrivateKey) factory.generatePrivate(spec);
  }
  
  /**
   * Splits a PEM formatted file into individual certificates and keys.
   * Just looks for the '-----BEGIN' header and appends to a buffer until
   * the next header or the end of file is reached. New-lines are removed.
   * @param pem A non-null pem file.
   * @return A list of individual objects in the file.
   */
  private static List<String> splitPem(final String pem) {
    final List<String> parts = Lists.newArrayList();
    final StringBuilder buf = new StringBuilder();
    try (BufferedReader br = new BufferedReader(new StringReader(pem))) {
      for (String line = br.readLine(); line != null; line = br.readLine()) {
        if (line.isEmpty()) {
          continue;
        }
        if (line.startsWith("-----BEGIN")) {
          if (buf.length() > 0) {
            parts.add(buf.toString());
            buf.setLength(0);
          }
        }
        buf.append(line);
      }
      if (buf.length() > 0) {
        parts.add(buf.toString());
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to parse the pem!", e);
    }
    return parts;
  }
  
  /**
   * Parses a B64 encoded certificate into an X509 representation.
   * @param cert The non-null and non-empty certificate.
   * @return A parsed certificate.
   * @throws CertificateException If something goes pear shaped.
   */
  private static X509Certificate parseCert(final String cert) throws 
      CertificateException {
    return (X509Certificate) CA_FACTORY.generateCertificate(
        new ByteArrayInputStream(
            DatatypeConverter.parseBase64Binary((cert))));
  }
  
  /**
   * A helper to extract the CN from a certificate.
   * @param cert A non-null certificate.
   * @return The CN.
   */
  private static String getCN(final X509Certificate cert) {
    String cn = cert.getSubjectX500Principal().getName()
                    .replace("CN=", "");
    cn = cn.substring(0, cn.indexOf(","));
    return cn;
  }
  
  /**
   * A helper to determine if the cert could be used for TLS termination.
   * @param cert A non-null cert.
   * @return True if it could be, false if it was an intermediate or CA 
   * certificate.
   */
  private static boolean isServerCert(final X509Certificate cert) {
    boolean[] uses = cert.getKeyUsage();
    if (uses == null) {
      throw new IllegalStateException("The given certificate didn't "
          + "have any usage extensions. This shouldn't happen with certs "
          + "used for TLS: " + getCN(cert));
    }
    return (uses[0] && uses[2]);
  }

  public static interface RefreshCallback {
    public void refresh(final SSLContext context);
  }
  
  public static Builder newBuilder() {
    return new Builder();
  }
  
  public static class Builder {
    private SourceType type;
    private String keystore;
    private String keystore_pass;
    private String cert;
    private String key;
    private String ca;
    private String secret_cert;
    private String secret_key;
    private int interval;
    private TSDB tsdb;
    private RefreshCallback callback;
    
    public Builder setType(final SourceType type) {
      this.type = type;
      return this;
    }
    
    public Builder setKeystore(final String keystore) {
      this.keystore = keystore;
      return this;
    }
    
    public Builder setKeystorePass(final String keystore_pass) {
      this.keystore_pass = keystore_pass;
      return this;
    }
    
    public Builder setCert(final String cert) {
      this.cert = cert;
      return this;
    }
    
    public Builder setKey(final String key) {
      this.key = key;
      return this;
    }
    
    public Builder setCa(final String ca) {
      this.ca = ca;
      return this;
    }
    
    public Builder setSecret_cert(final String secret_cert) {
      this.secret_cert = secret_cert;
      return this;
    }
    
    public Builder setSecret_key(final String secret_key) {
      this.secret_key = secret_key;
      return this;
    }
    
    public Builder setInterval(final int interval) {
      this.interval = interval;
      return this;
    }
    
    public Builder setTsdb(final TSDB tsdb) {
      this.tsdb = tsdb;
      return this;
    }
    
    public Builder setCallback(final RefreshCallback callback) {
      this.callback = callback;
      return this;
    }
    
    public RefreshingSSLContext build() {
      return new RefreshingSSLContext(this);
    }
  }
}

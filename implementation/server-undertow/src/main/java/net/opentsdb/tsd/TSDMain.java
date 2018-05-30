// This file is part of OpenTSDB.
// Copyright (C) 2017-2018  The OpenTSDB Authors.
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
package net.opentsdb.tsd;

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
import java.util.Map;
import java.util.Map.Entry;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.servlet.DispatcherType;
import javax.servlet.ServletException;
import javax.xml.bind.DatatypeConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.Undertow.Builder;
import io.undertow.server.handlers.PathHandler;
import io.undertow.servlet.Servlets;
import io.undertow.servlet.api.DeploymentInfo;
import io.undertow.servlet.api.DeploymentManager;
import io.undertow.servlet.api.FilterInfo;
import net.opentsdb.auth.Authentication;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.configuration.ConfigurationEntrySchema;
import net.opentsdb.core.DefaultTSDB;
import net.opentsdb.servlet.applications.OpenTSDBApplication;
import net.opentsdb.servlet.filter.AuthFilter;
import net.opentsdb.utils.ArgP;
import sun.security.util.DerInputStream;
import sun.security.util.DerValue;

/**
 * A simple main method that instantiates a TSD and the TSD servlet package
 * and serves them with the Undertow HTTP server.
 * 
 * @since 3.0
 */
@SuppressWarnings("restriction")
public class TSDMain {
  private static Logger LOG = LoggerFactory.getLogger(TSDMain.class);
  
  /** Property keys */
  public static final String HTTP_PORT_KEY = "tsd.network.port";
  public static final String TLS_PORT_KEY = "tsd.network.ssl_port";
  public static final String BIND_KEY = "tsd.network.bind";
  public static final String ROOT_KEY = "tsd.http.root";
  public static final String LOAD_PLUGINS_KEY = "tsd.core.load_plugins";
  public static final String KEYSTORE_KEY = "tsd.network.keystore.location";
  public static final String KEYSTORE_PASS_KEY = "tsd.network.keystore.password";
  public static final String TLS_CERT_KEY = "tsd.network.tls.certificate";
  public static final String TLS_KEY_KEY = "tsd.network.tls.key";
  public static final String TLS_CA_KEY = "tsd.network.tls.ca";
  
  private static CertificateFactory factory;
  
  /** Defaults */
  public static final String DEFAULT_PATH = "/";
  
  /** The TSD reference. Static so we can shutdown gracefully. */
  private static DefaultTSDB tsdb = null;
  
  /** The Undertwo server reference. Static so we can shutdown gracefully. */
  private static Undertow server = null;
  
  /**
   * The main function that does all the fun stuff of instantiating the TSD
   * and passing it to the servlet.
   * @param args CLI args.
   */
  public static void main(final String[] args) {
    try {
      System.in.close();  // Release a FD we don't need.
    } catch (Exception e) {
      LOG.warn("Failed to close stdin", e);
    }
    
    final Configuration config = new Configuration(args);
    
    config.register(HTTP_PORT_KEY, 0, false, 
        "A port to listen on for HTTP requests.");
    config.register(TLS_PORT_KEY, 0, false, 
        "A port to listen on for HTTP over TLS requests");
    config.register(BIND_KEY, "0.0.0.0", false, 
        "The IP to bind listeners to.");
    config.register(ROOT_KEY, DEFAULT_PATH, false, 
        "The root path for HTTP requests.");
    config.register(LOAD_PLUGINS_KEY, true, false, 
        "Whether or not to load plugins on startup.");
    config.register(KEYSTORE_KEY, null, false,
        "The location to a Java keystore file containing the public "
        + "certificate, optional intermediaries and private key for TLS "
        + "server authentication. If set, takes precedence over '" 
        + TLS_CERT_KEY + "' and '" + TLS_KEY_KEY + "'");
    config.register(ConfigurationEntrySchema.newBuilder()
        .isNullable()
        .setSource(TSDMain.class.getCanonicalName())
        .setKey(KEYSTORE_PASS_KEY)
        .setDefaultValue(null)
        .setDescription("The secret used to unlock the keystore set in '" 
            + KEYSTORE_KEY + "'")
        .setType(String.class)
        .isSecret()
        .build());
    config.register(TLS_CERT_KEY, null, false,
        "The location to a PEM formatted file containing the public "
        + "certificate, optional intermediaries and optionally the "
        + "private key. If the private key is not in this file, make "
        + "sure that '" + TLS_KEY_KEY + "' is set.");
    config.register(TLS_KEY_KEY, null, false,
        "The location to a file containing the PKCS#1 or PKCS#8 encoded "
        + "private key. Make sure that '" + TLS_CERT_KEY 
        + "' is also configured.");
    config.register(TLS_CA_KEY, null, false,
        "An optional location to a PEM formatted file containing CA "
        + "certificates.");
    
    int port = config.getInt(HTTP_PORT_KEY);
    int ssl_port = config.getInt(TLS_PORT_KEY);
    if (port < 1 && ssl_port < 1) {
      System.err.println("Must provide an HTTP or SSL port.");
    }
  
    String bind = config.getString(BIND_KEY);
    String root = config.getString(ROOT_KEY);
    boolean load_plugins = config.getBoolean(LOAD_PLUGINS_KEY);
    
    tsdb = new DefaultTSDB(config);
    if (load_plugins) {
      try {
        // if the plugins don't load within 5 minutes, something is TERRIBLY
        // wrong.
        tsdb.initializeRegistry(true).join(300000);
      } catch (Exception e) {
        LOG.error("Failed to initialize TSDB registry", e);
        System.exit(1);
      }
    }
    
    // make sure to shutdown gracefully.
    registerShutdownHook();
    
    DeploymentInfo servletBuilder = Servlets.deployment()
        .setClassLoader(TSDMain.class.getClassLoader())
        .setContextPath(root)
        .setDeploymentName("tsd.war") // just a name
        .addServletContextAttribute(OpenTSDBApplication.TSD_ATTRIBUTE, tsdb)
        .addServlets(
          Servlets.servlet("OpenTSDB", 
              org.glassfish.jersey.servlet.ServletContainer.class)
                  .setAsyncSupported(true)
                  .setLoadOnStartup(1)
                  .addInitParam("javax.ws.rs.Application", 
                      OpenTSDBApplication.class.getName())
                  .addMapping("/*"));

    // Load an authentication filter if so configured.
    if (tsdb.getConfig().getBoolean(Authentication.AUTH_ENABLED_KEY)) {
      LOG.info("Authentication is enabled, searching for an auth filter.");
      AuthFilter auth_filter = tsdb.getRegistry()
          .getDefaultPlugin(AuthFilter.class);
      if (auth_filter != null) {
        final FilterInfo filter_info = new FilterInfo("tsdbAuthFilter", 
            auth_filter.getClass());
        filter_info.setAsyncSupported(true);
        final  Map<String, String> conf = tsdb.getConfig().asUnsecuredMap();
        for (final Entry<String, String> entry : conf.entrySet()) {
          filter_info.addInitParam(entry.getKey(), entry.getValue());
        }
        servletBuilder = servletBuilder.addFilter(filter_info)
            .addFilterUrlMapping("tsdbAuthFilter", "/*", DispatcherType.REQUEST);
        LOG.info("Successfully loaded auth filter: " + auth_filter);
      } else {
        LOG.error("Failed to find an authentication filter. Not starting.");
        System.exit(1);
        return;
      }
    }
    
    final DeploymentManager manager = Servlets.defaultContainer()
        .addDeployment(servletBuilder);
    manager.deploy();
    
    try {
      final PathHandler path = Handlers.path(Handlers.redirect(root))
              .addPrefixPath(root, manager.start());
      
      final Builder builder = Undertow.builder()
          .setHandler(path);
      if (port > 0) {
        builder.addHttpListener(port, bind);
      }
      
      // SSL/TLS setup
      if (ssl_port > 0) {
        builder.addHttpsListener(4443, bind, buildSSLContext(config));
      }
      
      server = builder.build();
      server.start();
      LOG.info("Undertow server successfully started.");
      return;
    } catch (ServletException e) {
      LOG.error("Unable to start due to servlet exception", e);
    } catch (IllegalArgumentException e) {
      LOG.error("Invalid configuration", e);
    } catch (Exception e) {
      LOG.error("WTF! Unexpected exception starting server", e);
    }
  }

  /**
   * Walks the config to figure out how to load either a key store or 
   * certificates from files for the server.
   * @param config A non-null config.
   * @return An SSL context. If a method threw an exception then we exit.
   */
  private static SSLContext buildSSLContext(final Configuration config) {
    try {
      factory = CertificateFactory.getInstance("X.509");
      final String keystore_location = config.getString(KEYSTORE_KEY);
      final SSLContext sslContext;
      if (!Strings.isNullOrEmpty(keystore_location)) {
        sslContext = contextFromKeystore(config);
      } else {
        sslContext = contextFromKeyAndCerts(config);
      }
      if (sslContext == null) {
        // Already threw an exception.
        System.exit(1);
      }
      return sslContext;
    } catch (CertificateException e) {
      LOG.error("Failed to insantiate factory", e);
    } catch (Exception e) {
      LOG.error("Unexpected exception initializing SSL Context", e);
    }
    System.exit(1);
    return null;
  }
  
  /**
   * Attempts to load a keystore from disk and initialize an SSL Context 
   * with the data therin. The keystore should contain all of the certs,
   * intermediates, CAs and the private key.
   * @param config A non-null config.
   * @return An instantiated context or null if something went wrong. The
   * exceptions will be logged.
   */
  private static SSLContext contextFromKeystore(final Configuration config) {
    final String keystore_location = config.getString(KEYSTORE_KEY);
    final String keystore_key = config.getString(KEYSTORE_PASS_KEY);
    if (Strings.isNullOrEmpty(keystore_key)) {
      throw new IllegalArgumentException("Cannot enable SSL without a "
          + "keystore password. Set '" + KEYSTORE_PASS_KEY +"'");
    }
    try {
      // load an initialize the keystore.
      final FileInputStream file = new FileInputStream(keystore_location);
      final KeyStore keystore = KeyStore.getInstance(KeyStore.getDefaultType());
      keystore.load(file, keystore_key.toCharArray());
      
      // initialize a key manager to pass to the SSL context using the keystore.
      final KeyManagerFactory key_factory = KeyManagerFactory.getInstance(
          KeyManagerFactory.getDefaultAlgorithm());
      key_factory.init(keystore, keystore_key.toCharArray());
  
      // init a trust manager so we can use the public cert.
      final TrustManagerFactory trust_factory = 
          TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trust_factory.init(keystore);
      final TrustManager[] trustManagers = trust_factory.getTrustManagers();
      
      final SSLContext sslContext = SSLContext.getInstance("TLS"); 
      sslContext.init(key_factory.getKeyManagers(), trustManagers, null);
      return sslContext;
    } catch (FileNotFoundException e) {
      LOG.error("Unable to open keystore file: " + keystore_location, e);
    } catch (NoSuchAlgorithmException e) {
      LOG.error("Missing a required algorithm for TLS?", e);
    } catch (CertificateException e) {
      LOG.error("Invalid certificate in keystore: " + keystore_location, e);
    } catch (IOException e) {
      LOG.error("WTF? Something went pear shaped unexpectedly", e);
    } catch (KeyManagementException e) {
      LOG.error("Something was wrong with the key in the keystore: " 
          + keystore_location, e);
    } catch (UnrecoverableKeyException e) {
      LOG.error("Possibly corrupted key in file: " + keystore_location, e);
    } catch (KeyStoreException e) {
      LOG.error("WTF! Unexpected exception in keystore: " + keystore_location, e);
    }
    return null;
  }
  
  /**
   * Attempts to load the key from a combination of private key file, 
   * certificate file(s) and Certificate Authority pem.
   * @param config A non-null config.
   * @return An instantiated context or null if something went wrong. The
   * exceptions will be logged.
   */
  private static SSLContext contextFromKeyAndCerts(final Configuration config) {
    RSAPrivateKey key = null;
    final String key_location = config.getString(TLS_KEY_KEY);
    try {
      if (!Strings.isNullOrEmpty(key_location)) {
        String raw_key = Files.toString(new File(key_location), Charsets.UTF_8);
        if (Strings.isNullOrEmpty(raw_key)) {
          throw new IllegalStateException("The key at location " 
              + key_location + " was null or empty.");
        }
        if (raw_key.contains("BEGIN PRIVATE KEY")) {
          key = parsePKCS8Key(trimPrivateKey(raw_key));
        } else if (raw_key.contains("BEGIN RSA PRIVATE KEY")) {
          key = parsePKCS1Key(trimPrivateKey(raw_key));
        } else {
          throw new IllegalStateException("Unrecognized key at location " 
              + key_location);
        }
        LOG.info("Successfully loaded private key from file: "+  key_location);
      }
    } catch (IOException | GeneralSecurityException e) {
      LOG.error("Failed to parse private key at: " + key_location);
      return null;
    }
    
    final String cert_file_location = config.getString(TLS_CERT_KEY);
    if (Strings.isNullOrEmpty(cert_file_location)) {
      throw new IllegalStateException("A certificate file, in PEM format, "
          + "must be present at a location defined in: " + TLS_CERT_KEY);
    }
    
    List<String> parts;
    final KeyStore keystore;
    final List<Certificate> certificates;
    final String keystore_pass = Long.toString(System.currentTimeMillis());
    int cert_id = 0;
    
    try {
      final String raw_certs = Files.toString(
          new File(cert_file_location), Charsets.UTF_8);
      parts = splitPem(raw_certs);
      if (parts.size() < 1) {
        throw new IllegalStateException("No certificates found in path: " 
      + cert_file_location);
      }
      
      keystore = KeyStore.getInstance("JKS");
      keystore.load(null);
      certificates = Lists.newArrayList();
      boolean have_server_cert = false;
      for (final String part : parts) {
        if (part.contains("BEGIN PRIVATE KEY")) {
          if (key != null) {
            throw new RuntimeException("Already loaded a key but " 
                + cert_file_location + " also had a key.");
          }
          key = parsePKCS8Key(trimPrivateKey(part));
        } else if (part.contains("BEGIN RSA PRIVATE KEY")) {
          if (key != null) {
            throw new RuntimeException("Already loaded a key but " 
                + cert_file_location + " also had a key.");
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
            + cert_file_location);
      }
      Certificate[] cert_array = new Certificate[certificates.size()];
      certificates.toArray(cert_array);
      keystore.setKeyEntry("key-alias", 
                           key, 
                           keystore_pass.toCharArray(),
                           cert_array);
    } catch (IOException | GeneralSecurityException e) {
      LOG.error("Failed to load certificate(s) from " 
          + cert_file_location, e);
      return null;
    }
    
    final String ca_certs_location = config.getString(TLS_CA_KEY);
    if (!Strings.isNullOrEmpty(ca_certs_location)) {
      try {
        if (new File(ca_certs_location).isDirectory()) {
          // TODO - load all the certs in the dir
          throw new UnsupportedOperationException("We don't support "
              + "loading directories yet.");
        } else {
          final String raw_cas = Files.toString(new File(ca_certs_location), 
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
      } catch (IOException | GeneralSecurityException e) {
        LOG.error("Failed to load CA certificates from: " + ca_certs_location, e);
        return null;
      }
    }
    
    try {
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
      return ssl_context;
    } catch (GeneralSecurityException e) {
      LOG.error("Failed to initialize SSLContext", e);
      return null;
    }
  }
  
  /** Prints usage and exits with the given retval. */
  static void usage(final ArgP argp, final String errmsg, final int retval) {
    System.err.println(errmsg);
    System.err.println("Usage: tsd --config=PATH\n"
      + "Starts the TSD, the Time Series Daemon");
    if (argp != null) {
      System.err.print(argp.usage());
    }
    System.exit(retval);
  }
  
  /**
   * Helper method that will attach a callback to the runtime shutdown so that
   * if we receive a SIGTERM then we can gracefully stop the web server and
   * the TSD with it's associated plugins.
   */
  private static void registerShutdownHook() {
    final class TSDBShutdown extends Thread {
      public TSDBShutdown() {
        super("TSDBShutdown");
      }
      public void run() {
        try {
          if (server != null) {
            LOG.info("Stopping Undertow server");
            server.stop();
          }
          if (tsdb != null) {
            LOG.info("Shuttingdown TSD");
            tsdb.shutdown().join();
          }
          
          LOG.info("Shutdown complete.");
        } catch (Exception e) {
          LoggerFactory.getLogger(TSDBShutdown.class)
            .error("Uncaught exception during shutdown", e);
        }
      }
    }
    Runtime.getRuntime().addShutdownHook(new TSDBShutdown());
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
   * appared to be in the wrong format.
   */
  static RSAPrivateKey parsePKCS1Key(final String key) throws 
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
    return (X509Certificate) factory.generateCertificate(
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
}

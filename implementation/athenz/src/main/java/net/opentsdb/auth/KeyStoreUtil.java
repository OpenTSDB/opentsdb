// This file is part of OpenTSDB.
// Copyright (C) 2021  The OpenTSDB Authors.
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
package net.opentsdb.auth;

import com.google.common.io.Resources;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility for opening Java JKS files.
 */
public class KeyStoreUtil {

  private static final Logger LOG = LoggerFactory.getLogger(KeyStoreUtil.class);

  private static final String TLSV1_2 = "TLSv1.2";

  /**
   * @param certLocation the location on the cert file
   * @param privateKeyLocation the location of hte private key file
   * @param password keystore password
   * @return a KeyStore with loaded certificate
   * @throws Exception KeyStore generation can throw Exception for many reasons
   */
  public static KeyStore createKeyStore(final String certLocation,
                                        final String privateKeyLocation,
                                        final String password) throws Exception {

    final CertificateFactory cf = CertificateFactory.getInstance("X.509");
    final JcaPEMKeyConverter pemConverter = new JcaPEMKeyConverter();

    X509Certificate certificate;
    PrivateKey privateKey;

    final InputStream publicCertStream;
    final InputStream privateKeyStream;

    try {
      if (new File(certLocation).isAbsolute() && new File(privateKeyLocation)
              .isAbsolute()) {
        File certFile = new File(certLocation);
        File keyFile = new File(privateKeyLocation);

        if (!certFile.exists() || !keyFile.exists()) {
          LOG.error("Missing cert or private key files");
          throw new IllegalArgumentException("Missing cert or private key files");
        }
        publicCertStream = new FileInputStream(certFile);
        privateKeyStream = new FileInputStream(keyFile);
      } else {
        publicCertStream = Resources.getResource(certLocation).openStream();
        privateKeyStream = Resources.getResource(privateKeyLocation).openStream();
      }
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }

    try (PEMParser pemParser = new PEMParser(new InputStreamReader(privateKeyStream))) {
      Object key = pemParser.readObject();
      PrivateKeyInfo pKeyInfo = ((PEMKeyPair) key).getPrivateKeyInfo();
      privateKey = pemConverter.getPrivateKey(pKeyInfo);
    } catch (IOException e) {
      throw new IllegalStateException("Unable to parse private key", e);
    }

    certificate = (X509Certificate) cf.generateCertificate(publicCertStream);
    KeyStore keyStore = KeyStore.getInstance("JKS");
    String alias = certificate.getSubjectX500Principal().getName();
    keyStore.load(null);
    keyStore.setKeyEntry(alias, privateKey, password.toCharArray(),
            new X509Certificate[]{certificate});
    return keyStore;
  }

  /**
   * Attempts to load the key store.
   * @param jksFilePath The path to the JKS store.
   * @param password The password for the store.
   * @return The opened store if successful.
   * @throws Exception If something goes pear shaped.
   */
  public static KeyStore loadKeyStore(final String jksFilePath,
                                      final String password) throws Exception {

    final KeyStore keyStore = KeyStore.getInstance("JKS");
    if (new File(jksFilePath).isAbsolute()) {
      try (InputStream jksFileInputStream = new FileInputStream(jksFilePath)) {
        keyStore.load(jksFileInputStream, password.toCharArray());
        return keyStore;
      }
    }

    try (InputStream jksFileInputStream =
                 Resources.getResource(jksFilePath).openStream()) {
      keyStore.load(jksFileInputStream, password.toCharArray());
      return keyStore;
    }
  }

}

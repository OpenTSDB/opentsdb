package net.opentsdb;

import java.io.IOException;
import java.net.URL;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

/** Build data for {@code net.opentsdb} */
public final class BuildData {
  private static Attributes sharedInstance;

  private static Attributes attributes() {
    if (sharedInstance == null) {
      try {
        final URL manifestUrl = BuildData.class.getClassLoader()
            .getResource("META-INF/MANIFEST.MF");
        sharedInstance = new Manifest(manifestUrl.openStream())
            .getMainAttributes();
      } catch (IOException e) {
        throw new RuntimeException("", e);
      }
    }

    return sharedInstance;
  }

  public static String name() {
    return attributes().getValue("Implementation-Title");
  }

  /** Version string MAJOR.MINOR.MAINT */
  public static String version() {
    return attributes().getValue("Implementation-Version");
  }

  /** UTC date at which this package was built. */
  public static String date() {
    return attributes().getValue("Build-Date");
  }

  /** Username of the user who built this package. */
  public static String user() {
    return attributes().getValue("Build-User");
  }

  /** Host on which this package was built. */
  public static String host() {
    return attributes().getValue("Build-Host");
  }
}

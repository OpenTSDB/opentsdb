package tsd.client;

public class ClientConstants {
  public static final String TSDB_ID_CLASS = "[-_./a-zA-Z0-9]";
  public static final String TSDB_ID_RE = "^" + TSDB_ID_CLASS + "*$";
  public static final String TSDB_TAGVALUE_RE =
    "^(\\*?"                                       // a `*' wildcard or nothing
    + "|" + TSDB_ID_CLASS + "+(\\|" + TSDB_ID_CLASS + "+)*)$"; // `foo|bar|...'
}

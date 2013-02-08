package net.opentsdb.core;

import org.apache.zookeeper.KeeperException;

/**
 * @author Andrey Stepachev
 */
public class ZkException extends IllegalStateException {

  static final long serialVersionUID = -1848913673093119416L;

  private KeeperException.Code code = KeeperException.Code.SYSTEMERROR;

  public ZkException() {
  }

  public ZkException(KeeperException ke) {
    this(ke.getMessage(), ke, ke.code());
  }

  public ZkException(String message, Throwable cause) {
    super(message, cause);
  }

  public ZkException(String s, KeeperException.Code code) {
    super(s);
    this.code = code;
  }

  public ZkException(String s) {
    super(s);
  }

  public ZkException(String message, Throwable cause, KeeperException.Code code) {
    super(message, cause);
    this.code = code;
  }
}

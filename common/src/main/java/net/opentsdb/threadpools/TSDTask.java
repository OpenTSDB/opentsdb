package net.opentsdb.threadpools;

/**
 * Defines the TSD task types.
 * 
 * Used by the {@code TSDBThreadPoolExecutor} Task scheduler
 *
 */
public enum TSDTask {

  /** When the task to start processing a query */
  QUERY,

  /** When the task is a query completion */
  QUERY_CLOSE,

  /** When the task is to start processing a result */
  RESULT,

  /** When the task is a result completion */
  RESULT_CLOSE;

  /**
   * Returns the TSD task counterpart
   * 
   * @param tsdTask
   * @return
   */
  public TSDTask getCounterPart() {
    TSDTask tt = null;
    switch (this) {
      case QUERY_CLOSE:
        tt = QUERY;
        break;

      case RESULT_CLOSE:
        tt = RESULT;
        break;

      default:
        tt = this;
        break;
    }

    return tt;

  }

}

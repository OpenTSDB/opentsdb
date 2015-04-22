package net.opentsdb.storage.hbase;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;

import java.util.ArrayList;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A row processor receives individual rows from a scanner for processing
 * without having to deal with any of the state of the scanner. Implement this
 * abstract class and then call {@link #processRows} with a {@link
 * org.hbase.async.Scanner} and an instance of your implementation of this
 * class. {@link #processRows} will then close the scanner once there are no
 * more rows and it will return the result of processing all the rows. It will
 * also handle closing the scanner in case of an error.
 *
 * @param <R> The type of result produced by the row processor
 */
abstract class RowProcessor<R> {
  /**
   * Called once when all rows have been processed to build the result of the
   * {@link com.stumbleupon.async.Deferred} returned from {@link #processRows}.
   *
   * @return The result of processing all rows
   */
  protected abstract R getResult();

  /**
   * Called once for each row returned by the scanner.
   *
   * @param row A row as returned by the scanner
   */
  protected abstract void processRow(final ArrayList<KeyValue> row);

  /**
   * Start processing all the rows returned by the provided scanner using the
   * provided row processor.
   *
   * @param scanner      A scanner that provides the rows to process
   * @param rowProcessor The row processor to use to process each individual
   *                     row
   * @return A {@link com.stumbleupon.async.Deferred} with the result as
   * returned by {@link #getResult}.
   */
  public static <R> Deferred<R> processRows(final Scanner scanner,
                                            final RowProcessor<R> rowProcessor) {
    checkNotNull(scanner);
    checkNotNull(rowProcessor);

    return scanner.nextRows()
        .addCallbackDeferring(new ScannerCB<>(scanner, rowProcessor))
        .addErrback(new ScannerErrback(scanner));
  }

  /**
   * A callback that is added recursively between calls to {@link
   * org.hbase.async.Scanner#nextRows}. When the {@link com.stumbleupon.async.Deferred}
   * yields a result and the callback is called it will call {@link #processRow}
   * for each row returned. Once there are no more rows a deferred that contains
   * the result of {@link #getResult} will be called and the scanner will be
   * closed.
   */
  private static final class ScannerCB<T>
      implements Callback<Deferred<T>, ArrayList<ArrayList<KeyValue>>> {

    private final Scanner scanner;
    private final RowProcessor<T> rowProcessor;

    public ScannerCB(final Scanner scanner,
                     final RowProcessor<T> rowProcessor) {
      this.scanner = scanner;
      this.rowProcessor = rowProcessor;
    }

    @Override
    public Deferred<T> call(final ArrayList<ArrayList<KeyValue>> rows) {
      // Once the scanner returns null there are no more rows that matches the
      // filters on the scanner so we are done and should return the result of
      // this row processor.
      if (rows == null) {
        scanner.close();
        return Deferred.fromResult(rowProcessor.getResult());
      }

      // Iterate over all the rows and let the implementation process each row
      // in turn.
      for (final ArrayList<KeyValue> row : rows) {
        rowProcessor.processRow(row);
      }

      // Attempt to get more rows
      return scanner.nextRows().addCallbackDeferring(this);
    }
  }

  /**
   * A callback that will be called if anything in the callback chain cause an
   * error. This callback ensures that the scanner will be closed.
   */
  private static class ScannerErrback implements Callback<Exception, Exception> {
    private final Scanner scanner;

    public ScannerErrback(final Scanner scanner) {
      this.scanner = scanner;
    }

    @Override
    public Exception call(final Exception e) throws Exception {
      scanner.close();
      return e;
    }
  }
}

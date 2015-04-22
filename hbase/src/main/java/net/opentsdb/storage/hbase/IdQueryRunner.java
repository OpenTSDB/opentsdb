package net.opentsdb.storage.hbase;

import com.google.common.base.MoreObjects;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import net.opentsdb.uid.IdQuery;
import net.opentsdb.uid.IdentifierDecorator;
import net.opentsdb.uid.UniqueIdType;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * A delegate that scans the HBase uid table for UIDs that match a provided
 * {@link net.opentsdb.uid.IdQuery}.
 */
class IdQueryRunner {
  /**
   * The start row to scan on empty search strings.  `!' = first ASCII char.
   */
  private static final byte[] START_ROW = new byte[] { '!' };

  /**
   * The end row to scan on empty search strings.  `~' = last ASCII char.
   */
  private static final byte[] END_ROW = new byte[] { '~' };

  /**
   * The high limit of number of IDs to attempt to fetch.
   */
  private static final int MAX_NUM_RESULTS = 4096;

  private final Scanner scanner;
  private final int maxResults;

  IdQueryRunner(final HBaseClient client,
                final byte[] uidTableName,
                final IdQuery query) {
    scanner = buildScanner(client, uidTableName);
    configureQualifier(scanner, query.getType());
    configureRowKeys(scanner, query.getQuery());
    configureMaxNumRows(scanner, query.getMaxResults());

    this.maxResults = query.getMaxResults();
  }

  /**
   * Build the scanner and select the correct column family to search in.
   */
  private Scanner buildScanner(final HBaseClient client,
                               final byte[] uidTableName) {
    Scanner scanner = client.newScanner(uidTableName);
    scanner.setFamily(HBaseConst.UniqueId.ID_FAMILY);

    return scanner;
  }

  /**
   * Configure the qualifier to look in based on the provided type.
   */
  private void configureQualifier(final Scanner scanner,
                                  final UniqueIdType type) {
    if (type != null) {
      byte[] qualifier = type.toValue().getBytes(HBaseConst.CHARSET);
      scanner.setQualifier(qualifier);
    }
  }

  /**
   * Configure the scanner to search between certain rows based on the provided
   * string query.
   */
  private void configureRowKeys(final Scanner scanner,
                                final String query) {
    if (query == null) {
      scanner.setStartKey(START_ROW);
      scanner.setStopKey(END_ROW);
    } else {
      final byte[] start_row = query.getBytes(HBaseConst.CHARSET);
      final byte[] end_row = Arrays.copyOf(start_row, start_row.length);
      end_row[start_row.length - 1]++;

      scanner.setStartKey(start_row);
      scanner.setStopKey(end_row);
    }
  }

  /**
   * Configure the maximum number of rows to fetch.
   */
  private void configureMaxNumRows(final Scanner scanner,
                                   final int maxResults) {
    if (maxResults != IdQuery.NO_LIMIT) {
      scanner.setMaxNumRows(Math.min(maxResults, MAX_NUM_RESULTS));
    }
  }

  /**
   * Once an instance of this class has been instantiated through the constructor
   * this method should be called which will start the scan.
   */
  Deferred<List<IdentifierDecorator>> search() {
    return scanner.nextRows()
            .addCallbackDeferring(new ScannerCB(scanner, maxResults));
  }

  /**
   * Callback to scan a HBase database for matching IDs.
   */
  private static final class ScannerCB
          implements Callback<Deferred<List<IdentifierDecorator>>, ArrayList<ArrayList<KeyValue>>> {
    private final List<IdentifierDecorator> suggestions = new LinkedList<>();
    private final Scanner scanner;
    private final int max_results;

    ScannerCB(final Scanner scanner, final int max_results) {
      this.max_results = max_results;
      this.scanner = scanner;
    }

    Deferred<List<IdentifierDecorator>> search() {
      return scanner.nextRows().addCallbackDeferring(this);
    }

    @Override
    public Deferred<List<IdentifierDecorator>> call(final ArrayList<ArrayList<KeyValue>> rows) {
      // Once the scanner returns null there is nothing more for us so we are
      // done.
      if (rows == null) {
        return Deferred.fromResult(suggestions);
      }

      // Iterate over all the rows and columns and extract the identifiers with
      // their names and types.
      for (final ArrayList<KeyValue> row : rows) {
        for (final KeyValue kv : row) {
          suggestions.add(new HBaseIdentifierDecorator(kv));

          if (suggestions.size() >= max_results) {  // We have enough.
            return Deferred.fromResult(suggestions);
          }
        }
      }

      // Attempt to get more results
      return search();
    }
  }

  /**
   * An implementation of the label interface that accepts a raw
   * {@link org.hbase.async.KeyValue} and calculates the values based on that
   * when requested.
   */
  private static class HBaseIdentifierDecorator implements IdentifierDecorator {
    private final KeyValue keyValue;

    public HBaseIdentifierDecorator(final KeyValue keyValue) {
      this.keyValue = keyValue;
    }

    @Override
    public byte[] getId() {
      return keyValue.value();
    }

    @Override
    public UniqueIdType getType() {
      String stringType = new String(keyValue.qualifier(), HBaseConst.CHARSET);
      return UniqueIdType.fromValue(stringType);
    }

    @Override
    public String getName() {
      return new String(keyValue.key(), HBaseConst.CHARSET);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
              .add("id", getId())
              .add("type", getType())
              .add("name", getName())
              .toString();
    }
  }
}

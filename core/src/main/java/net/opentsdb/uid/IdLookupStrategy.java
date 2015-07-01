package net.opentsdb.uid;

import com.google.common.base.Strings;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * An IdLookupStrategy defines some custom behavior to use when attempting to lookup the ID behind a
 * name.
 */
public interface IdLookupStrategy {
  /**
   * Fetch the ID behind the provided name using the provided {@link LabelClientTypeContext}
   * instance.
   *
   * @param labelClientTypeContext The LabelClientTypeContext instance to use for looking up the ID
   * @param name The name to find the ID behind
   * @return A future that on completion will contains the ID behind the name
   */
  @Nonnull
  ListenableFuture<LabelId> getId(final LabelClientTypeContext labelClientTypeContext,
                                  final String name);

  /**
   * The most basic id lookup strategy that just fetches the ID behind the provided name without
   * providing any special behavior.
   */
  class SimpleIdLookupStrategy implements IdLookupStrategy {
    public static final IdLookupStrategy instance = new SimpleIdLookupStrategy();

    @Nonnull
    @Override
    public ListenableFuture<LabelId> getId(final LabelClientTypeContext labelClientTypeContext,
                                           final String name) {
      return labelClientTypeContext.getId(name);
    }
  }

  /**
   * An ID lookup strategy that will create an ID for the provided name if it does not already
   * exist.
   */
  class CreatingIdLookupStrategy implements IdLookupStrategy {
    public static final IdLookupStrategy instance = new CreatingIdLookupStrategy();

    @Nonnull
    @Override
    public ListenableFuture<LabelId> getId(final LabelClientTypeContext labelClientTypeContext,
                                           final String name) {
      final SettableFuture<LabelId> id = SettableFuture.create();

      Futures.addCallback(labelClientTypeContext.getId(name), new FutureCallback<LabelId>() {
        @Override
        public void onSuccess(@Nullable final LabelId result) {
          id.set(result);
        }

        @Override
        public void onFailure(final Throwable throwable) {
          Futures.addCallback(labelClientTypeContext.createId(name), new FutureCallback<LabelId>() {
            @Override
            public void onSuccess(@Nullable final LabelId result) {
              id.set(result);
            }

            @Override
            public void onFailure(final Throwable throwable) {
              id.setException(throwable);
            }
          });
        }
      });

      return id;
    }
  }

  /**
   * An ID lookup strategy that supports wildcards.
   *
   * <p>If the provided name is {@code null}, empty or equal to "*" it will be interpreted as a
   * wildcard and it will return immediately with a future that contains {@code null}.
   *
   * <p>If the provided name is not interpreted as a wildcard as described above then a regular
   * lookup will be done.
   */
  class WildcardIdLookupStrategy implements IdLookupStrategy {
    public static final IdLookupStrategy instance = new WildcardIdLookupStrategy();

    @Nonnull
    @Override
    public ListenableFuture<LabelId> getId(final LabelClientTypeContext labelClientTypeContext,
                                           final String name) {
      if (Strings.isNullOrEmpty(name) || "*".equals(name)) {
        return Futures.immediateFuture(null);
      }

      return labelClientTypeContext.getId(name);
    }
  }
}

package net.opentsdb.uid;

import com.google.common.base.Strings;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import javax.annotation.Nonnull;

/**
 * An IdLookupStrategy defines some custom behavior to use when attempting to lookup the ID behind a
 * name.
 */
public interface IdLookupStrategy {
  /**
   * Fetch the ID behind the provided name using the provided {@link net.opentsdb.uid.UniqueId}
   * instance.
   *
   * @param uniqueId The UniqueId instance to use for looking up the ID
   * @param name The name to find the ID behind
   * @return A deferred that contains the byte representation of the ID behind the name
   */
  @Nonnull
  Deferred<LabelId> getId(@Nonnull final UniqueId uniqueId,
                          @Nonnull final String name);

  /**
   * The most basic id lookup strategy that just fetches the ID behind the provided name without
   * providing any special behavior.
   */
  class SimpleIdLookupStrategy implements IdLookupStrategy {
    public static final IdLookupStrategy instance = new SimpleIdLookupStrategy();

    @Nonnull
    @Override
    public Deferred<LabelId> getId(@Nonnull final UniqueId uniqueId,
                                   @Nonnull final String name) {
      return uniqueId.getId(name);
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
    public Deferred<LabelId> getId(@Nonnull final UniqueId uniqueId,
                                   @Nonnull final String name) {
      return uniqueId.getId(name).addErrback(new Callback<Object, Exception>() {
        @Override
        public Object call(final Exception e) throws Exception {
          if (e instanceof NoSuchUniqueName) {
            return uniqueId.createId(name);
          }

          return e;
        }
      });
    }
  }

  /**
   * An ID lookup strategy that supports wildcards.
   * <p/>
   * If the provided name is {@code null}, empty or equal to "*" it will be interpreted as a
   * wildcard and a {@link com.stumbleupon.async.Deferred} with the result {@code null} will be
   * returned.
   * <p/>
   * If the provided name is not {@code null} then a regular lookup will be done.
   */
  class WildcardIdLookupStrategy implements IdLookupStrategy {
    public static final IdLookupStrategy instance = new WildcardIdLookupStrategy();

    @Nonnull
    @Override
    public Deferred<LabelId> getId(@Nonnull final UniqueId uniqueId,
                                   @Nonnull final String name) {
      if (Strings.isNullOrEmpty(name) || "*".equals(name)) {
        return Deferred.fromResult(null);
      }

      return uniqueId.getId(name);
    }
  }
}

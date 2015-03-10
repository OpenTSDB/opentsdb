package net.opentsdb.uid;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

public class CreatingIdLookupStrategy implements IdLookupStrategy {
  @Override
  public Deferred<byte[]> getId(final UniqueId uniqueId, final String name) {
    return uniqueId.getId(name)
        .addErrback(new Callback<Object, Exception>() {
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

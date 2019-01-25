package net.opentsdb.utils;

public class UniqueKeyPair<K, V> extends Pair<K,V> {



  /**
   * Ctor that stores references to the objects
   * @param key The key or left hand value to store
   * @param value The value or right hand value to store
   */
  public UniqueKeyPair(final K key, final V value) {
   super(key,value);
  }


  /**
   * Calculates the hash by ORing the key and value hash codes
   * @return a hash code for this pair
   */
  @Override
  public int hashCode() {
    return (key == null ? 0 : key.hashCode());
  }

  /**
   * Compares the Keys of two pairs for equality. If the incoming object
   * reference is
   * the same, the result is true. Then {@code .equals} is called on both
   * objects (if they are not null)
   * @return true if the objects refer to the same address or both objects are
   * equal
   */
  @Override
  public boolean equals(final Object object) {
    if (object == this) {
      return true;
    }
    if (object instanceof Pair<?, ?>) {
      final Pair<?, ?> other_pair = (Pair<?, ?>)object;
      return
              (key == null ? other_pair.getKey() == null :
                      key.equals(other_pair.key));
    }
    return false;
  }
}

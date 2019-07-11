// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.utils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class Comparators {

  /**
   * A simple comparator for two maps with the same types.
   *
   * @param <K> The type of the key.
   * @param <V> The type of the value.
   */
  public static class MapComparator<K, V extends Comparable<V>> 
      implements Comparator<Map<K, V>> {

    @Override
    public int compare(final Map<K, V> a, final Map<K, V> b) {
      if (a == b || a == null && b == null) {
        return 0;
      }
      if (a == null && b != null) {
        return -1;
      }
      if (b == null && a != null) {
        return 1;
      }
      if (a.size() > b.size()) {
        return -1;
      }
      if (b.size() > a.size()) {
        return 1;
      }
      for (final Entry<K, V> entry : a.entrySet()) {
        final V b_value = b.get(entry.getKey());
        if (b_value == null && entry.getValue() != null) {
          return 1;
        }
        final int cmp = entry.getValue().compareTo(b_value);
        if (cmp != 0) {
          return cmp;
        }
      }
      return 0;
    }
    
  }

  /**
   * A method that determines whether two lists contain the same elements
   * (not necessarily in the same order).
   *
   * @param List<T> one First list.
   * @param List<T> two Second list.
   */
  public static class ListComparison {
    public static <T> boolean equalLists(List<T> one, List<T> two){
      if (one == null && two == null){
        return true;
      }

      if((one == null && two != null)
              || one != null && two == null
              || one.size() != two.size()){
        return false;
      }

      //to avoid messing the order of the lists we will use a copy
      one = new ArrayList<>(one);
      two = new ArrayList<>(two);

      for (T element: one) {
        boolean exists = false;
        for (int i = 0; i < two.size(); i++) {
          if (two.get(i).equals(element)) {
            exists = true;
            two.remove(i);
            break;
          }
        }
        if (!exists)
          return false;
      }
      return true;
    }

  }
}

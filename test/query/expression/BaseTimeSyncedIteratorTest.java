// This file is part of OpenTSDB.
// Copyright (C) 2015  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.query.expression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import net.opentsdb.core.BaseTsdbTest;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.FillPolicy;
import net.opentsdb.core.Query;
import net.opentsdb.core.TSQuery;
import net.opentsdb.core.TSSubQuery;
import net.opentsdb.utils.Pair;

/**
 * A class for setting up a number of time series and queries for expression
 * testing.
 */
public class BaseTimeSyncedIteratorTest extends BaseTsdbTest {

  /** Start time across all queries */
  protected static final long START_TS = 1388534400;
  
  /** The query object compiled after calling {@link #runQueries(ArrayList)} */
  protected TSQuery query;
  
  /** The results of our queries after calling {@link #runQueries(ArrayList)} */
  protected Map<String, Pair<TSSubQuery, DataPoints[]>> results;

  /** List of iterators */
  protected Map<String, ITimeSyncedIterator> iterators;
  
  /**
   * Queries for metrics A and B with a group by all on the D tag
   */
  protected void queryAB_Dstar() throws Exception {
    final ArrayList<TSSubQuery> subs = new ArrayList<TSSubQuery>(2);
    TSSubQuery sub = new TSSubQuery();
    
    HashMap<String, String> query_tags = new HashMap<String, String>(1);
    query_tags.put("D", "*");
    
    sub = new TSSubQuery();
    sub.setMetric("A");
    sub.setTags(query_tags);
    sub.setAggregator("sum");
    subs.add(sub);
    
    sub = new TSSubQuery();
    sub.setMetric("B");
    query_tags = new HashMap<String, String>(1);
    query_tags.put("D", "*");
    sub.setTags(query_tags);
    sub.setAggregator("sum");
    subs.add(sub);
    
    runQueries(subs);
  }
  
  /**
   * Queries for A and B but without a tag specifier, thus agging em all
   */
  protected void queryAB_AggAll() throws Exception {
    final ArrayList<TSSubQuery> subs = new ArrayList<TSSubQuery>(2);
    TSSubQuery sub = new TSSubQuery();
    
    HashMap<String, String> query_tags = new HashMap<String, String>(1);
    
    sub = new TSSubQuery();
    sub.setMetric("A");
    sub.setTags(query_tags);
    sub.setAggregator("sum");
    subs.add(sub);
    
    sub = new TSSubQuery();
    sub.setMetric("B");
    query_tags = new HashMap<String, String>(1);
    sub.setTags(query_tags);
    sub.setAggregator("sum");
    subs.add(sub);
    
    runQueries(subs);
  }
  
  /**
   * Queries for A only with a filter of tag value "D" for tag key "D"
   */
  protected void queryA_DD() throws Exception {
    final ArrayList<TSSubQuery> subs = new ArrayList<TSSubQuery>(2);
    TSSubQuery sub = new TSSubQuery();
    final HashMap<String, String> query_tags = new HashMap<String, String>(1);
    query_tags.put("D", "D");
    sub = new TSSubQuery();
    sub.setMetric("A");
    sub.setTags(query_tags);
    sub.setAggregator("sum");
    subs.add(sub);
    
    runQueries(subs);
  }
  
  /**
   * Executes the queries against MockBase through the regular pipeline and stores
   * the results in {@linke #results}
   * @param subs The queries to execute
   */
  protected void runQueries(final ArrayList<TSSubQuery> subs) throws Exception {
    query = new TSQuery();
    query.setStart(Long.toString(START_TS));
    query.setQueries(subs);
    query.validateAndSetQuery();

    final Query[] compiled = query.buildQueries(tsdb);
    results = new HashMap<String, Pair<TSSubQuery, DataPoints[]>>(
        compiled.length);
    iterators = new HashMap<String, ITimeSyncedIterator>(compiled.length);
    
    int index = 0;
    for (final Query q : compiled) {
      final DataPoints[] dps = q.runAsync().join();
      results.put(Integer.toString(index), 
          new Pair<TSSubQuery, DataPoints[]>(
              query.getQueries().get(index), dps));
      final ITimeSyncedIterator it = new TimeSyncedIterator(Integer.toString(index), 
          query.getQueries().get(index).getFilterTagKs(), dps);
      it.setFillPolicy(new NumericFillPolicy(FillPolicy.NOT_A_NUMBER));
      iterators.put(Integer.toString(index), it);
      index++;
    }
  }

  /**
   * A and B, each with two series. Common D values, different E values
   */
  protected void twoSeriesAggedE() throws Exception {
    setDataPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(2);
    tags.put("D", "D");
    tags.put("E", "E");
    
    tsdb.addPoint("A", 1431561600, 1, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561660, 2, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561720, 3, tags).joinUninterruptibly();

    tags = new HashMap<String, String>(2);
    tags.put("D", "D");
    tags.put("E", "F");
    
    tsdb.addPoint("A", 1431561600, 1, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561660, 2, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561720, 3, tags).joinUninterruptibly();
    
    tags = new HashMap<String, String>(2);
    tags.put("D", "D");
    tags.put("E", "E");
    tsdb.addPoint("B", 1431561600, 11, tags).joinUninterruptibly();
    tsdb.addPoint("B", 1431561660, 12, tags).joinUninterruptibly();
    tsdb.addPoint("B", 1431561720, 13, tags).joinUninterruptibly();
    
    tags = new HashMap<String, String>(2);
    tags.put("D", "D");
    tags.put("E", "F");
    tsdb.addPoint("B", 1431561600, 11, tags).joinUninterruptibly();
    tsdb.addPoint("B", 1431561660, 12, tags).joinUninterruptibly();
    tsdb.addPoint("B", 1431561720, 13, tags).joinUninterruptibly();
  }
  
  /**
   * A and B, each with two series. Common D, different E values and the B metric
   * series have an extra Z tag with different values. 
   */
  protected void twoSeriesAggedEandExtraTagK() throws Exception {
    setDataPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(2);
    tags.put("D", "D");
    tags.put("E", "E");
    
    tsdb.addPoint("A", 1431561600, 1, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561660, 2, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561720, 3, tags).joinUninterruptibly();

    tags = new HashMap<String, String>(2);
    tags.put("D", "D");
    tags.put("E", "F");
    
    tsdb.addPoint("A", 1431561600, 1, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561660, 2, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561720, 3, tags).joinUninterruptibly();
    
    tags = new HashMap<String, String>(2);
    tags.put("D", "D");
    tags.put("E", "E");
    tags.put("Z", "A");
    tsdb.addPoint("B", 1431561600, 11, tags).joinUninterruptibly();
    tsdb.addPoint("B", 1431561660, 12, tags).joinUninterruptibly();
    tsdb.addPoint("B", 1431561720, 13, tags).joinUninterruptibly();
    
    tags = new HashMap<String, String>(2);
    tags.put("D", "D");
    tags.put("E", "F");
    tags.put("Z", "B");
    tsdb.addPoint("B", 1431561600, 11, tags).joinUninterruptibly();
    tsdb.addPoint("B", 1431561660, 12, tags).joinUninterruptibly();
    tsdb.addPoint("B", 1431561720, 13, tags).joinUninterruptibly();
  }
  
  /**
   * A and B where A has two series and B only has one. Series A has two D 
   * values that will be agged. Different D and E values.
   */
  protected void oneAggedTheOtherTagged() throws Exception {
    setDataPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(2);
    tags.put("D", "D");
    tags.put("E", "E");
    
    tsdb.addPoint("A", 1431561600, 1, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561660, 2, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561720, 3, tags).joinUninterruptibly();

    tags = new HashMap<String, String>(2);
    tags.put("D", "E");
    tags.put("E", "F");
    
    tsdb.addPoint("A", 1431561600, 1, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561660, 2, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561720, 3, tags).joinUninterruptibly();
    
    tags = new HashMap<String, String>(2);
    tags.put("D", "D");
    tags.put("E", "E");
    
    tsdb.addPoint("B", 1431561600, 11, tags).joinUninterruptibly();
    tsdb.addPoint("B", 1431561660, 12, tags).joinUninterruptibly();
    tsdb.addPoint("B", 1431561720, 13, tags).joinUninterruptibly();
  }
  
  /**
   * A only with three series. Different D values, commong E values.
   */
  protected void threeSameENoB() throws Exception {
    setDataPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(2);
    tags.put("D", "D");
    tags.put("E", "E");
    
    tsdb.addPoint("A", 1431561600, 1, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561660, 2, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561720, 3, tags).joinUninterruptibly();
    
    tags = new HashMap<String, String>(2);
    tags.put("D", "F");
    tags.put("E", "E");
    tsdb.addPoint("A", 1431561600, 4, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561660, 5, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561720, 6, tags).joinUninterruptibly();
    
    tags = new HashMap<String, String>(2);
    tags.put("D", "G");
    tags.put("E", "E");
    tsdb.addPoint("A", 1431561600, 7, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561660, 8, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561720, 9, tags).joinUninterruptibly();
  }
  
  /**
   * A and B where A has two series, B has three. Different D values, common E.
   */
  protected void oneExtraSameE() throws Exception {
    setDataPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(2);
    tags.put("D", "D");
    tags.put("E", "E");
    
    tsdb.addPoint("A", 1431561600, 1, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561660, 2, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561720, 3, tags).joinUninterruptibly();
    
    tags = new HashMap<String, String>(2);
    tags.put("D", "F");
    tags.put("E", "E");
    tsdb.addPoint("A", 1431561600, 4, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561660, 5, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561720, 6, tags).joinUninterruptibly();
    
    tags = new HashMap<String, String>(2);
    tags.put("D", "D");
    tags.put("E", "E");
    tsdb.addPoint("B", 1431561600, 11, tags).joinUninterruptibly();
    tsdb.addPoint("B", 1431561660, 12, tags).joinUninterruptibly();
    tsdb.addPoint("B", 1431561720, 13, tags).joinUninterruptibly();
    
    tags = new HashMap<String, String>(2);
    tags.put("D", "F");
    tags.put("E", "E");
    tsdb.addPoint("B", 1431561600, 14, tags).joinUninterruptibly();
    tsdb.addPoint("B", 1431561660, 15, tags).joinUninterruptibly();
    tsdb.addPoint("B", 1431561720, 16, tags).joinUninterruptibly();
    
    // all by myself......
    tags = new HashMap<String, String>(2);
    tags.put("D", "G");
    tags.put("E", "E");
    tsdb.addPoint("B", 1431561600, 17, tags).joinUninterruptibly();
    tsdb.addPoint("B", 1431561660, 18, tags).joinUninterruptibly();
    tsdb.addPoint("B", 1431561720, 19, tags).joinUninterruptibly();
  }
  
  /**
   * A and B with two series each. Different D values, common E. 
   * A has values at T0 and T1, but then B has values at T2 and T3. Should
   * throw NaNs after the intersection.
   */
  protected void timeOffset() throws Exception {
    setDataPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(2);
    tags.put("D", "D");
    tags.put("E", "E");
    
    tsdb.addPoint("A", 1431561600, 1, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561660, 2, tags).joinUninterruptibly();
    
    tags = new HashMap<String, String>(2);
    tags.put("D", "F");
    tags.put("E", "E");
    tsdb.addPoint("A", 1431561600, 4, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561660, 5, tags).joinUninterruptibly();
    
    tags = new HashMap<String, String>(2);
    tags.put("D", "D");
    tags.put("E", "E");
    tsdb.addPoint("B", 1431561720, 13, tags).joinUninterruptibly();
    tsdb.addPoint("B", 1431561780, 14, tags).joinUninterruptibly();
    
    tags = new HashMap<String, String>(2);
    tags.put("D", "F");
    tags.put("E", "E");
    tsdb.addPoint("B", 1431561720, 16, tags).joinUninterruptibly();
    tsdb.addPoint("B", 1431561780, 17, tags).joinUninterruptibly();
  }
  
  /**
   * A and B, each with three series. Different D values, commong E.
   */
  protected void threeSameE() throws Exception {
    setDataPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(2);
    tags.put("D", "D");
    tags.put("E", "E");
    
    tsdb.addPoint("A", 1431561600, 1, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561660, 2, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561720, 3, tags).joinUninterruptibly();
    
    tags = new HashMap<String, String>(2);
    tags.put("D", "F");
    tags.put("E", "E");
    tsdb.addPoint("A", 1431561600, 4, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561660, 5, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561720, 6, tags).joinUninterruptibly();
    
    tags = new HashMap<String, String>(2);
    tags.put("D", "G");
    tags.put("E", "E");
    tsdb.addPoint("A", 1431561600, 7, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561660, 8, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561720, 9, tags).joinUninterruptibly();
    
    tags = new HashMap<String, String>(2);
    tags.put("D", "D");
    tags.put("E", "E");
    tsdb.addPoint("B", 1431561600, 11, tags).joinUninterruptibly();
    tsdb.addPoint("B", 1431561660, 12, tags).joinUninterruptibly();
    tsdb.addPoint("B", 1431561720, 13, tags).joinUninterruptibly();
    
    tags = new HashMap<String, String>(2);
    tags.put("D", "F");
    tags.put("E", "E");
    tsdb.addPoint("B", 1431561600, 14, tags).joinUninterruptibly();
    tsdb.addPoint("B", 1431561660, 15, tags).joinUninterruptibly();
    tsdb.addPoint("B", 1431561720, 16, tags).joinUninterruptibly();

    tags = new HashMap<String, String>(2);
    tags.put("D", "G");
    tags.put("E", "E");
    tsdb.addPoint("B", 1431561600, 17, tags).joinUninterruptibly();
    tsdb.addPoint("B", 1431561660, 18, tags).joinUninterruptibly();
    tsdb.addPoint("B", 1431561720, 19, tags).joinUninterruptibly();
  }
  
  /**
   * A and B, each with three series. Different D values. Series in A are missing
   * the E tag. Common values in E for B.
   */
  protected void threeAMissingE() throws Exception {
    setDataPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(2);
    tags.put("D", "D");
    tsdb.addPoint("A", 1431561600, 1, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561660, 2, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561720, 3, tags).joinUninterruptibly();
    
    tags = new HashMap<String, String>(2);
    tags.put("D", "F");
    tsdb.addPoint("A", 1431561600, 4, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561660, 5, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561720, 6, tags).joinUninterruptibly();
    
    tags = new HashMap<String, String>(2);
    tags.put("D", "G");
    tsdb.addPoint("A", 1431561600, 7, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561660, 8, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561720, 9, tags).joinUninterruptibly();
    
    tags = new HashMap<String, String>(2);
    tags.put("D", "D");
    tags.put("E", "E");
    tsdb.addPoint("B", 1431561600, 11, tags).joinUninterruptibly();
    tsdb.addPoint("B", 1431561660, 12, tags).joinUninterruptibly();
    tsdb.addPoint("B", 1431561720, 13, tags).joinUninterruptibly();
    
    tags = new HashMap<String, String>(2);
    tags.put("D", "F");
    tags.put("E", "E");
    tsdb.addPoint("B", 1431561600, 14, tags).joinUninterruptibly();
    tsdb.addPoint("B", 1431561660, 15, tags).joinUninterruptibly();
    tsdb.addPoint("B", 1431561720, 16, tags).joinUninterruptibly();

    tags = new HashMap<String, String>(2);
    tags.put("D", "G");
    tags.put("E", "E");
    tsdb.addPoint("B", 1431561600, 17, tags).joinUninterruptibly();
    tsdb.addPoint("B", 1431561660, 18, tags).joinUninterruptibly();
    tsdb.addPoint("B", 1431561720, 19, tags).joinUninterruptibly();
  }
  
  /**
   * A and B, each with three series. Different D and E values
   */
  protected void threeDifE() throws Exception {
    setDataPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(2);
    tags.put("D", "D");
    tags.put("E", "A");
    tsdb.addPoint("A", 1431561600, 1, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561660, 2, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561720, 3, tags).joinUninterruptibly();
    
    tags = new HashMap<String, String>(2);
    tags.put("D", "F");
    tags.put("E", "B");
    tsdb.addPoint("A", 1431561600, 4, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561660, 5, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561720, 6, tags).joinUninterruptibly();
    
    tags = new HashMap<String, String>(2);
    tags.put("D", "G");
    tags.put("E", "C");
    tsdb.addPoint("A", 1431561600, 7, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561660, 8, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561720, 9, tags).joinUninterruptibly();
    
    tags = new HashMap<String, String>(2);
    tags.put("D", "D");
    tags.put("E", "D");
    tsdb.addPoint("B", 1431561600, 11, tags).joinUninterruptibly();
    tsdb.addPoint("B", 1431561660, 12, tags).joinUninterruptibly();
    tsdb.addPoint("B", 1431561720, 13, tags).joinUninterruptibly();
    
    tags = new HashMap<String, String>(2);
    tags.put("D", "F");
    tags.put("E", "F");
    tsdb.addPoint("B", 1431561600, 14, tags).joinUninterruptibly();
    tsdb.addPoint("B", 1431561660, 15, tags).joinUninterruptibly();
    tsdb.addPoint("B", 1431561720, 16, tags).joinUninterruptibly();

    tags = new HashMap<String, String>(2);
    tags.put("D", "G");
    tags.put("E", "G");
    tsdb.addPoint("B", 1431561600, 17, tags).joinUninterruptibly();
    tsdb.addPoint("B", 1431561660, 18, tags).joinUninterruptibly();
    tsdb.addPoint("B", 1431561720, 19, tags).joinUninterruptibly();
  }

  /**
   * A and B, each with 3 series. Different D values, common E. 
   * Each set has one series that isn't in the other. D=G in A and D=Q in B.
   */
  protected void threeDisjointSameE() throws Exception {
    setDataPointStorage();
    
    HashMap<String, String> tags = new HashMap<String, String>(2);
    tags.put("D", "D");
    tags.put("E", "E");
    tsdb.addPoint("A", 1431561600, 1, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561660, 2, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561720, 3, tags).joinUninterruptibly();
    
    // not in set 2
    tags = new HashMap<String, String>(2);
    tags.put("D", "F");
    tags.put("E", "E");
    tsdb.addPoint("A", 1431561600, 4, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561660, 5, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561720, 6, tags).joinUninterruptibly();
    
    tags = new HashMap<String, String>(2);
    tags.put("D", "G");
    tags.put("E", "E");
    tsdb.addPoint("A", 1431561600, 7, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561660, 8, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561720, 9, tags).joinUninterruptibly();
    
    tags = new HashMap<String, String>(2);
    tags.put("D", "D");
    tags.put("E", "E");
    tsdb.addPoint("B", 1431561600, 11, tags).joinUninterruptibly();
    tsdb.addPoint("B", 1431561660, 12, tags).joinUninterruptibly();
    tsdb.addPoint("B", 1431561720, 13, tags).joinUninterruptibly();
    
    // not in set 1
    tags = new HashMap<String, String>(2);
    tags.put("D", "Q");
    tags.put("E", "E");
    tsdb.addPoint("B", 1431561600, 14, tags).joinUninterruptibly();
    tsdb.addPoint("B", 1431561660, 15, tags).joinUninterruptibly();
    tsdb.addPoint("B", 1431561720, 16, tags).joinUninterruptibly();

    tags = new HashMap<String, String>(2);
    tags.put("D", "G");
    tags.put("E", "E");
    tsdb.addPoint("B", 1431561600, 17, tags).joinUninterruptibly();
    tsdb.addPoint("B", 1431561660, 18, tags).joinUninterruptibly();
    tsdb.addPoint("B", 1431561720, 19, tags).joinUninterruptibly();
  }
  
  /**
   * A and B, each with three series. Different D values, common E values.
   * D=G is the only common series between the sets
   */
  protected void reduceToOne() throws Exception {
    setDataPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(2);
    tags.put("D", "D");
    tags.put("E", "E");
    
    tsdb.addPoint("A", 1431561600, 1, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561660, 2, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561720, 3, tags).joinUninterruptibly();
    
    // not in set 2
    tags = new HashMap<String, String>(2);
    tags.put("D", "F");
    tags.put("E", "E");
    tsdb.addPoint("A", 1431561600, 4, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561660, 5, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561720, 6, tags).joinUninterruptibly();
    
    tags = new HashMap<String, String>(2);
    tags.put("D", "G");
    tags.put("E", "E");
    tsdb.addPoint("A", 1431561600, 7, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561660, 8, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561720, 9, tags).joinUninterruptibly();
    
    tags = new HashMap<String, String>(2);
    tags.put("D", "P");
    tags.put("E", "E");
    tsdb.addPoint("B", 1431561600, 11, tags).joinUninterruptibly();
    tsdb.addPoint("B", 1431561660, 12, tags).joinUninterruptibly();
    tsdb.addPoint("B", 1431561720, 13, tags).joinUninterruptibly();
    
    // not in set 1
    tags = new HashMap<String, String>(2);
    tags.put("D", "Q");
    tags.put("E", "E");
    tsdb.addPoint("B", 1431561600, 14, tags).joinUninterruptibly();
    tsdb.addPoint("B", 1431561660, 15, tags).joinUninterruptibly();
    tsdb.addPoint("B", 1431561720, 16, tags).joinUninterruptibly();

    tags = new HashMap<String, String>(2);
    tags.put("D", "G");
    tags.put("E", "E");
    tsdb.addPoint("B", 1431561600, 17, tags).joinUninterruptibly();
    tsdb.addPoint("B", 1431561660, 18, tags).joinUninterruptibly();
    tsdb.addPoint("B", 1431561720, 19, tags).joinUninterruptibly();
  }
 
  /**
   * A and B, each with three series. Different D values, common E values.
   * Each series is "missing" a data point so that we can test time sync.  
   */
  protected void threeSameEGaps() throws Exception {
    setDataPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(2);
    tags.put("D", "D");
    tags.put("E", "E");
    
    tsdb.addPoint("A", 1431561600, 1, tags).joinUninterruptibly();
    //tsdb.addPoint("A", 1431561660, 2, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561720, 3, tags).joinUninterruptibly();
    
    tags = new HashMap<String, String>(2);
    tags.put("D", "F");
    tags.put("E", "E");
    tsdb.addPoint("A", 1431561600, 4, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561660, 5, tags).joinUninterruptibly();
    //tsdb.addPoint("A", 1431561720, 6, tags).joinUninterruptibly();
    
    tags = new HashMap<String, String>(2);
    tags.put("D", "G");
    tags.put("E", "E");
    //tsdb.addPoint("A", 1431561600, 7, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561660, 8, tags).joinUninterruptibly();
    tsdb.addPoint("A", 1431561720, 9, tags).joinUninterruptibly();
    
    tags = new HashMap<String, String>(2);
    tags.put("D", "D");
    tags.put("E", "E");
    //tsdb.addPoint("B", 1431561600, 11, tags).joinUninterruptibly();
    //tsdb.addPoint("B", 1431561660, 12, tags).joinUninterruptibly();
    tsdb.addPoint("B", 1431561720, 13, tags).joinUninterruptibly();
    
    tags = new HashMap<String, String>(2);
    tags.put("D", "F");
    tags.put("E", "E");
    //tsdb.addPoint("B", 1431561600, 14, tags).joinUninterruptibly();
    tsdb.addPoint("B", 1431561660, 15, tags).joinUninterruptibly();
    //tsdb.addPoint("B", 1431561720, 16, tags).joinUninterruptibly();

    tags = new HashMap<String, String>(2);
    tags.put("D", "G");
    tags.put("E", "E");
    //tsdb.addPoint("B", 1431561600, 17, tags).joinUninterruptibly();
    //tsdb.addPoint("B", 1431561660, 18, tags).joinUninterruptibly();
    tsdb.addPoint("B", 1431561720, 19, tags).joinUninterruptibly();
  }
 
}

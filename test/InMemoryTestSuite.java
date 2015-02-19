package net.opentsdb;

import net.opentsdb.core.DataPointsClientExecuteQueryTest;
import net.opentsdb.core.DataPointsClientTest;
import net.opentsdb.core.MetaClientAnnotationTest;
import net.opentsdb.core.MetaClientTSMetaTest;
import net.opentsdb.core.MetaClientUIDMetaTest;
import net.opentsdb.core.RowKeyTest;
import net.opentsdb.core.SeekableViewsForTest;
import net.opentsdb.core.TestAggregationIterator;
import net.opentsdb.core.TestAggregators;
import net.opentsdb.core.TestDownsampler;
import net.opentsdb.core.TestInternal;
import net.opentsdb.core.TestMutableDataPoint;
import net.opentsdb.core.TestQueryBuilder;
import net.opentsdb.core.TestRateSpan;
import net.opentsdb.core.TestSpan;
import net.opentsdb.core.TestTSQuery;
import net.opentsdb.core.TestTSSubQuery;
import net.opentsdb.core.TestTags;
import net.opentsdb.core.TestTsdbQueryDownsample;
import net.opentsdb.core.TreeClientTest;
import net.opentsdb.core.UniqueIdClientTest;
import net.opentsdb.meta.TestTSMeta;
import net.opentsdb.meta.TestTSUIDQuery;
import net.opentsdb.meta.TestUIDMeta;
import net.opentsdb.search.TestSearchPlugin;
import net.opentsdb.search.TestSearchQuery;
import net.opentsdb.search.TestTimeSeriesLookup;
import net.opentsdb.storage.DatabaseTests;
import net.opentsdb.storage.StoreModuleTest;
import net.opentsdb.storage.TestMemoryStore;
import net.opentsdb.storage.TestTsdbStore;
import net.opentsdb.storage.cassandra.TestCassandraStore;
import net.opentsdb.storage.hbase.AvailableIdsGaugeTest;
import net.opentsdb.storage.hbase.IdQueryRunnerTest;
import net.opentsdb.storage.hbase.TestCompactedRow;
import net.opentsdb.storage.hbase.TestCompactionQueue;
import net.opentsdb.storage.hbase.TestHBaseStore;
import net.opentsdb.storage.hbase.UsedIdsGaugeTest;
import net.opentsdb.storage.json.AnnotationMixInTest;
import net.opentsdb.storage.json.BranchMixInTest;
import net.opentsdb.storage.json.LeafMixInTest;
import net.opentsdb.storage.json.TSMetaMixInTest;
import net.opentsdb.storage.json.TreeMixInTest;
import net.opentsdb.storage.json.TreeRuleMixInTest;
import net.opentsdb.storage.json.UIDMetaMixInTest;
import net.opentsdb.tools.TestDumpSeries;
import net.opentsdb.tools.TestFsck;
import net.opentsdb.tools.TestTextImporter;
import net.opentsdb.tools.TestUID;
import net.opentsdb.tree.TestBranch;
import net.opentsdb.tree.TestLeaf;
import net.opentsdb.tree.TestTree;
import net.opentsdb.tree.TestTreeBuilder;
import net.opentsdb.tree.TestTreeRule;
import net.opentsdb.tsd.TestAnnotationRpc;
import net.opentsdb.tsd.TestGraphHandler;
import net.opentsdb.tsd.TestHttpJsonSerializer;
import net.opentsdb.tsd.TestHttpQuery;
import net.opentsdb.tsd.TestPutRpc;
import net.opentsdb.tsd.TestQueryRpc;
import net.opentsdb.tsd.TestRTPublisher;
import net.opentsdb.tsd.TestRpcHandler;
import net.opentsdb.tsd.TestSearchRpc;
import net.opentsdb.tsd.TestSuggestRpc;
import net.opentsdb.tsd.TestTreeRpc;
import net.opentsdb.tsd.TestUniqueIdRpc;
import net.opentsdb.uid.IdCreatedEventTest;
import net.opentsdb.uid.IdQueryTest;
import net.opentsdb.uid.TestNoSuchUniqueId;
import net.opentsdb.uid.TestUidFormatter;
import net.opentsdb.uid.TestUidResolver;
import net.opentsdb.uid.TestUniqueId;
import net.opentsdb.uid.UniqueIdTypeTest;
import net.opentsdb.utils.TestByteArrayPair;
import net.opentsdb.utils.TestConfig;
import net.opentsdb.utils.TestDateTime;
import net.opentsdb.utils.TestJSON;
import net.opentsdb.utils.TestPair;
import net.opentsdb.utils.TestPluginLoader;
import org.junit.experimental.categories.Categories;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Categories.class)
@Categories.ExcludeCategory(DatabaseTests.class)
@Suite.SuiteClasses({
        AnnotationMixInTest.class,
        AvailableIdsGaugeTest.class,
        BranchMixInTest.class,
        DataPointsClientExecuteQueryTest.class,
        DataPointsClientTest.class,
        IdCreatedEventTest.class,
        IdQueryRunnerTest.class,
        IdQueryTest.class,
        LeafMixInTest.class,
        MetaClientAnnotationTest.class,
        MetaClientTSMetaTest.class,
        MetaClientUIDMetaTest.class,
        RowKeyTest.class,
        SeekableViewsForTest.class,
        StoreModuleTest.class,
        TestAggregationIterator.class,
        TestAggregators.class,
        TestAnnotationRpc.class,
        TestBranch.class,
        TestByteArrayPair.class,
        TestCassandraStore.class,
        TestCompactedRow.class,
        TestCompactionQueue.class,
        TestConfig.class,
        TestDateTime.class,
        TestDownsampler.class,
        TestDumpSeries.class,
        TestFsck.class,
        TestGraphHandler.class,
        TestHBaseStore.class,
        TestHttpJsonSerializer.class,
        TestHttpQuery.class,
        TestInternal.class,
        TestJSON.class,
        TestLeaf.class,
        TestMemoryStore.class,
        TestMutableDataPoint.class,
        TestNoSuchUniqueId.class,
        TestPair.class,
        TestPluginLoader.class,
        TestPutRpc.class,
        TestQueryBuilder.class,
        TestQueryRpc.class,
        TestRateSpan.class,
        TestRpcHandler.class,
        TestRTPublisher.class,
        TestSearchPlugin.class,
        TestSearchQuery.class,
        TestSearchRpc.class,
        TestSpan.class,
        TestSuggestRpc.class,
        TestTags.class,
        TestTextImporter.class,
        TestTimeSeriesLookup.class,
        TestTreeBuilder.class,
        TestTree.class,
        TestTreeRpc.class,
        TestTreeRule.class,
        TestTsdbQueryDownsample.class,
        TestTsdbStore.class,
        TestTSMeta.class,
        TestTSQuery.class,
        TestTSSubQuery.class,
        TestTSUIDQuery.class,
        TestUID.class,
        TestUidFormatter.class,
        TestUIDMeta.class,
        TestUidResolver.class,
        TestUniqueId.class,
        TestUniqueIdRpc.class,
        TreeClientTest.class,
        TreeMixInTest.class,
        TreeRuleMixInTest.class,
        TSMetaMixInTest.class,
        UIDMetaMixInTest.class,
        UniqueIdClientTest.class,
        UniqueIdTypeTest.class,
        UsedIdsGaugeTest.class
})
public class InMemoryTestSuite {
}

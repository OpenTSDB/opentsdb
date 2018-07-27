package net.opentsdb.query.processor.dedup;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.reflect.TypeToken;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class TestDedupNode {

    @Mock
    private QueryPipelineContext pipelineContext;

    @Mock
    private QueryResult queryResult;

    @Mock
    private TimeSeries timeSeries;

    @Mock
    private QueryNode upStream;

    @Captor
    private ArgumentCaptor<QueryResult> upStreamCaptor;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testRemovesDuplicateTimeseries() {

        List<TimeSeriesValue<? extends TimeSeriesDataType>> tsValues = new ArrayList() {{
            add(new MutableNumericValue(new SecondTimeStamp(1532455200), 43363892));
            add(new MutableNumericValue(new SecondTimeStamp(1532455200), 44421636.8));
            add(new MutableNumericValue(new SecondTimeStamp(1532483800), 49255974.4));
            add(new MutableNumericValue(new SecondTimeStamp(1532483500), 43338914.45831));
            add(new MutableNumericValue(new SecondTimeStamp(1532482900), 44013684.8));
            add(new MutableNumericValue(new SecondTimeStamp(1532483500), 43338914.4));
        }};

        List<TimeSeriesValue<? extends TimeSeriesDataType>> expectedValues = new ArrayList() {{
            add(new MutableNumericValue(new SecondTimeStamp(1532455200), 44421636.8));
            add(new MutableNumericValue(new SecondTimeStamp(1532483800), 49255974.4));
            add(new MutableNumericValue(new SecondTimeStamp(1532482900), 44013684.8));
            add(new MutableNumericValue(new SecondTimeStamp(1532483500), 43338914.4));
        }};

        Optional<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterator = Optional.of(tsValues.iterator());
        when(timeSeries.iterator(NumericType.TYPE)).thenReturn(iterator);
        when(pipelineContext.upstream(any(DedupNode.class))).thenReturn(new ArrayList<QueryNode>(){{add(upStream);}});
        when(queryResult.timeSeries()).thenReturn(new ArrayList<TimeSeries>() {{
            add(timeSeries);
        }});

        DedupNode dedupNode = new DedupNode(null, pipelineContext, null, DedupConfig.newBuilder().build());
        dedupNode.initialize(null);

        dedupNode.onNext(queryResult);

        verify(upStream).onNext(upStreamCaptor.capture());
        QueryResult dedupedResult = upStreamCaptor.getValue();

        Collection<TimeSeries> dedupedTSCollection = dedupedResult.timeSeries();
        assertEquals(1, dedupedTSCollection.size());
        TimeSeries dedupedTs = dedupedTSCollection.iterator().next();
        Optional<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> optional = dedupedTs.iterator(NumericType.TYPE);
        assertTrue(optional.isPresent());
        Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> dedupedIterator = optional.get();
        List<TimeSeriesValue<? extends TimeSeriesDataType>> dedupedValues = new ArrayList<>();
        dedupedIterator.forEachRemaining(dedupedValues::add);

        assertTrue(expectedValues.containsAll(dedupedValues));
    }

}

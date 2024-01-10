package net.opentsdb.kubernetes.discovery.impl;

import com.google.common.base.Strings;
import io.kubernetes.client.openapi.models.V1StatefulSet;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.kubernetes.discovery.ServiceContext;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.TimeSeriesDataSourceConfig;

import java.util.Map;
import java.util.Objects;

//TODO: Make this guy a plugin so that its config will be independent.
public class ShardedTSDBContext implements ServiceContext<V1StatefulSet> {

    private final String timeLabel;
    private final String namespaceLabel;
    private final QueryPipelineContext queryPipelineContext;
    private final TimeSeriesDataSourceConfig timeSeriesDataSourceConfig;

    //TODO: Can we avoid giving this in constructor.
    public ShardedTSDBContext(String timeLabel,
                              String namespaceLabel,
                              QueryPipelineContext queryPipelineContext,
                              TimeSeriesDataSourceConfig timeSeriesDataSourceConfig) {
        this.timeLabel = timeLabel;
        this.namespaceLabel = namespaceLabel;
        this.queryPipelineContext = queryPipelineContext;
        this.timeSeriesDataSourceConfig = timeSeriesDataSourceConfig;
    }

    @Override
    public boolean match(V1StatefulSet statefulSet) {


        final TimeStamp start = timeSeriesDataSourceConfig.startTimestamp();
        final TimeStamp end = timeSeriesDataSourceConfig.endTimestamp();
        final String namespace = timeSeriesDataSourceConfig.getNamespace();
        //Lets not get into name format.
        //Lets rely on labels to give us the info.

        if( !Objects.nonNull(statefulSet.getMetadata())) {
            return false;
        }

        Map<String, String> labels = statefulSet.getMetadata().getLabels();

        final String nsLabel = labels.get(namespaceLabel);
        final String epochLabel = labels.get(timeLabel);
        if(!Strings.isNullOrEmpty(nsLabel) && nsLabel.equals(namespace)) {
            if(!Strings.isNullOrEmpty(epochLabel)) {
                try {
                    //Assumes that the epoch is end.
                    //TODO: Make epoch start and epoch end a range and read from two labels.
                    //OR we can use epoch start and duration.
                    final String[] epochRange = epochLabel.split("-");
                    final int epochStart = Integer.parseInt(epochRange[0]);
                    final int epochEnd = Integer.parseInt(epochRange[1]);
                    final long epochStartSeconds = toSeconds(epochStart);
                    final long epochEndSeconds = toSeconds(epochEnd == 0 ? 24 : epochEnd);

                    if(end.epoch() < epochEndSeconds && end.epoch() > epochStartSeconds ) {
                        return true;
                    }
                } catch (NumberFormatException e) {
                    return false;
                }
            }
        }
        return false;
    }

    private long toSeconds(int hour_epoch) {
        final long epochSeconds = System.currentTimeMillis() / 1000;
        final long dayStart = epochSeconds - epochSeconds % 86400;
        if ( hour_epoch == 0) {
            return dayStart;
        }
        return dayStart + hour_epoch * 3600L;
    }

}

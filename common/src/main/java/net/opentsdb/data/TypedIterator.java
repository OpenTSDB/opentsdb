package net.opentsdb.data;

import com.google.common.reflect.TypeToken;
import java.util.Iterator;
import java.util.function.Consumer;

public class TypedIterator<T>  implements Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> {

    protected Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> iterator;
    protected TypeToken<?> type;

    public TypedIterator(Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> iterator,
        TypeToken<? extends TimeSeriesDataType> type) {
        this.iterator = iterator;
        this.type = type;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public TimeSeriesValue<? extends TimeSeriesDataType> next() {
        return iterator.next();
    }

    @Override
    public void remove() {
        iterator.remove();
    }

    @Override
    public void forEachRemaining(
        Consumer<? super TimeSeriesValue<? extends TimeSeriesDataType>> action) {
        iterator.forEachRemaining(action);
    }

    public TypeToken<?> getType() {
        return type;
    }

    public Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> getIterator() {
        return iterator;
    }
}

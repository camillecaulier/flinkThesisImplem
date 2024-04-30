package CompleteOperators;

import org.apache.flink.streaming.api.datastream.DataStream;

public interface CompleteOperator<T> {
    public DataStream<T> execute();
}

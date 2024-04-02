package keygrouping;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.io.network.api.writer.SubtaskStateMapper;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.runtime.partitioner.ConfigurableStreamPartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;

import java.util.Objects;


public class CustomStreamPartitioner extends StreamPartitioner<Tuple2<String,Integer>> implements ConfigurableStreamPartitioner {

    private static final long serialVersionUID = 1L;

    private final KeySelector<Tuple2<String,Integer>,String> keySelector;

    private int maxParallelism;

    Partitioner<String> partitioner;

    public CustomStreamPartitioner(Partitioner<String> partitioner, KeySelector<Tuple2<String, Integer>, String> keySelector, int maxParallelism) {
        Preconditions.checkArgument(maxParallelism > 0, "Number of key-groups must be > 0!");
        this.keySelector = Preconditions.checkNotNull(keySelector);
        this.maxParallelism = maxParallelism;

        this.partitioner = partitioner;
    }

    @Override
    public StreamPartitioner<Tuple2<String,Integer>> copy() {
        return this;
    }

    @Override
    public SubtaskStateMapper getDownstreamSubtaskStateMapper() {
        return SubtaskStateMapper.RANGE;
    }

    @Override
    public boolean isPointwise() {
        return false;
    }



    @Override
    public void configure(int maxParallelism) {
        this.maxParallelism = maxParallelism;
    }
    @Override
    public int hashCode() {
        return 0;
//        return Objects.hash(super.hashCode(), keySelector, maxParallelism);
    }

    @Override
    public int selectChannel(SerializationDelegate<StreamRecord<Tuple2<String, Integer>>> record) {
        String key;
        try {
            key = keySelector.getKey(record.getInstance().getValue());
        } catch (Exception e) {
            throw new RuntimeException("Could not extract key from " + record.getInstance(), e);
        }

        return partitioner.partition(key, numberOfChannels);
    }
}

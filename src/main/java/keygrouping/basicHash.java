package keygrouping;

import org.apache.flink.api.common.functions.Partitioner;

public class basicHash implements Partitioner<String> {
    @Override
    public int partition(String key, int numPartitions) {
        int hash = key.hashCode();
        return Math.abs(hash % numPartitions);
    }
}

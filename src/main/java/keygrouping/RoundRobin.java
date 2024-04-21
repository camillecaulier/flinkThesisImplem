package keygrouping;

import org.apache.flink.api.common.functions.Partitioner;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

public class RoundRobin implements Partitioner<String>, Serializable {
    private int index;
    public RoundRobin() {
        this.index =0;
    }


    @Override
    public int partition(String key, int numPartitions) {
        index ++;
        index = index % numPartitions;
        return index;
    }
}
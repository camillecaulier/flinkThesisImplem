package keygrouping;

import org.apache.flink.api.common.functions.Partitioner;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

public class RoundRobin extends keyGroupingBasic{
    private int index;
    public RoundRobin(int parallelism) {
        super(parallelism);
        this.index =-1;
    }


    @Override
    public int customPartition(String key, int numPartitions) {
        index ++;
        index = index % numPartitions;
//        System.out.println("index: " + index + " numPartitions: " + numPartitions);
        return index;
    }

//    private final AtomicInteger counter = new AtomicInteger(-1);
//
//    public RoundRobin(int parallelism) {
//        super(parallelism);
//    }
//
//    @Override
//    public int customPartition(String key, int numPartitions) {
//        int count = counter.incrementAndGet();
//        System.out.println("count: " + count + " numPartitions: " + numPartitions);
//        return Math.abs(count % numPartitions);
//    }
}
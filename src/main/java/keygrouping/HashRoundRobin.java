package keygrouping;

import eventTypes.EventBasic;
import org.apache.commons.math3.util.FastMath;
import org.apache.flink.shaded.guava31.com.google.common.hash.HashFunction;
import org.apache.flink.shaded.guava31.com.google.common.hash.Hashing;

public class HashRoundRobin extends keyGroupingBasic {
    int index = 0;

    private HashFunction h1 = Hashing.murmur3_128(13);
    public HashRoundRobin(int parallelism) {
        super(parallelism);
    }
    @Override
    public int customPartition(String key, int numPartitions) {
        if(EventBasic.isPopularKey(key)){
            return roundRobin(numPartitions);
        }

        return (int) (FastMath.abs(h1.hashBytes(key.getBytes()).asLong()) % numPartitions);
    }

    public int roundRobin(int numPartitions){
        index++;
        return Math.abs(index % numPartitions);
    }
}

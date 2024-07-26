package keygrouping;

import TopK.StreamSummaryHelper;
import com.clearspring.analytics.stream.StreamSummary;
import org.apache.commons.math3.util.FastMath;
import org.apache.flink.shaded.guava31.com.google.common.hash.HashFunction;
import org.apache.flink.shaded.guava31.com.google.common.hash.Hashing;

import java.util.HashMap;

public class HashRoundRobinTopK extends keyGroupingBasic {
    int index = 0;

    StreamSummary<String> streamSummary;

//    public HashFunction[] hashFunctions;
    Long totalItems;

    private HashFunction h1 = Hashing.murmur3_128(13);


    public HashRoundRobinTopK(int parallelism) {
        super(parallelism);
        streamSummary = new StreamSummary<String>(StreamSummaryHelper.capacity);
        totalItems = (long) 0;

    }
    @Override
    public int customPartition(String key, int numPartitions) {
        StreamSummaryHelper ssHelper = new StreamSummaryHelper();

        streamSummary.offer(key);
//        float probability = 2/(float)(this.parallelism  *10);
//        float probability = 2/(float)(10); // 2/(10*5 workers)
        HashMap<String,Long> freqList = ssHelper.getTopK(streamSummary,thresholdForTopK,totalItems);
        if(freqList.containsKey(key)) {
            totalItems++;
            return roundRobin(numPartitions);
        }

        return (int) (FastMath.abs(h1.hashBytes(key.getBytes()).asLong()) % numPartitions);
    }

    public int roundRobin(int numPartitions){
        index++;
        return Math.abs(index % numPartitions);
    }
}

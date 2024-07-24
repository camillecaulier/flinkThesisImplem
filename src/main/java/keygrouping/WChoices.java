package keygrouping;

import com.clearspring.analytics.stream.StreamSummary;
import org.apache.commons.math3.util.FastMath;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.shaded.guava31.com.google.common.hash.HashFunction;
import org.apache.flink.shaded.guava31.com.google.common.hash.Hashing;
import TopK.StreamSummaryHelper;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class WChoices extends keyGroupingBasic {
    /*
     * method based on the WChoice algorithm see https://github.com/anisnasir/SLBStorm
     */

    private  long[] targetTaskStats;

    private HashFunction h1 = Hashing.murmur3_128(13);
    private HashFunction h2 = Hashing.murmur3_128(17);
    StreamSummary<String> streamSummary;
    Long totalItems;

    int parallelism;

    public WChoices(int parallelism) {
        super(parallelism);
        this.parallelism = parallelism;
        targetTaskStats = new long[parallelism];
        streamSummary = new StreamSummary<String>(StreamSummaryHelper.capacity);
        totalItems = (long) 0;
    }

    @Override
    public int customPartition(String key, int numPartitions) {

        StreamSummaryHelper ssHelper = new StreamSummaryHelper();


        streamSummary.offer(key);
//        float probability = 2/(float)(this.parallelism*10); // 2/(10*5 workers)
        float probability = 1/(float)(parallelism); // 2/(2*5 workers)
        //  float value that represents a threshold for determining whether an item should be included in the returned list of top items
        HashMap<String,Long> freqList = ssHelper.getTopK(streamSummary,probability,totalItems);

        if(freqList.containsKey(key)) {

            int selected = selectMinChoice(targetTaskStats);

            targetTaskStats[selected]++;
            totalItems++;
            return selected;

        }else {
            int firstChoice = (int) (FastMath.abs(h1.hashBytes(key.getBytes()).asLong()) % numPartitions);
            int secondChoice = (int) (FastMath.abs(h2.hashBytes(key.getBytes()).asLong()) % numPartitions);
            int selected = targetTaskStats[firstChoice]>targetTaskStats[secondChoice]?secondChoice:firstChoice;
            targetTaskStats[selected]++;
            totalItems++;
            return selected;
        }

    }

    int selectMinChoice(long loadVector[]) {
        int index =0;
        for(int i = 0; i< this.parallelism; i++) {
            if (loadVector[i]<loadVector[index])
                index = i;
        }
        return index;
    }
}
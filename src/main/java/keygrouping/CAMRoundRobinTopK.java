package keygrouping;

import TopK.StreamSummaryHelper;
import com.clearspring.analytics.stream.StreamSummary;
import org.apache.commons.math3.util.FastMath;
import org.apache.flink.shaded.guava31.com.google.common.hash.HashFunction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class CAMRoundRobinTopK extends keyGroupingBasic{
    public int n;
    private ConcurrentHashMap<Integer, HashSet<String>> cardinality;

    private ConcurrentHashMap<Integer, AtomicInteger> tupleCount;
    int parallelism;

    int index = 0;
    StreamSummary<String> streamSummary;

    public HashFunction[] hashFunctions;
    Long totalItems;

    public CAMRoundRobinTopK(int n_choices, int numPartitions) {
        super(numPartitions);
        //n being the number of choices eg two choices etc...
        this.parallelism = numPartitions;
        this.n = n_choices;
        this.cardinality = new ConcurrentHashMap<Integer, HashSet<String>>(numPartitions);
        this.tupleCount = new ConcurrentHashMap<Integer, AtomicInteger>(numPartitions);
        streamSummary = new StreamSummary<String>(StreamSummaryHelper.capacity);
        totalItems = (long) 0;
        this.hashFunctions = createHashFunctions(n_choices);
    }

    @Override
    public int customPartition(String key, int numPartitions) {
        //special keygrouping for popular keys
        StreamSummaryHelper ssHelper = new StreamSummaryHelper();


        streamSummary.offer(key);
//        float probability = 2/(float)(this.parallelism  *10);
        float probability = 2/(float)(10); // 2/(10*5 workers)
        HashMap<String,Long> freqList = ssHelper.getTopK(streamSummary,probability,totalItems);
        if(freqList.containsKey(key)) {
            totalItems++;
            return roundRobin(numPartitions);
        }


        int[] hashes = generateHashes(key);
        for (int i = 0; i < n; i++) {
            int partition = hashes[i];
            cardinality.putIfAbsent(partition, new HashSet<>());
            tupleCount.putIfAbsent(partition, new AtomicInteger(0));

            synchronized (cardinality.get(partition)) { // Synchronize on the HashSet object for the partition
                HashSet<String> set = cardinality.get(partition);
                if (set.contains(key)) {
                    tupleCount.get(partition).getAndIncrement();
                    return partition;
                }
            }

        }

        int minCardinality = Integer.MAX_VALUE;
        int choice = 0;


        for (int i = 0; i < n; i++) {
            int partition = hashes[i];
            cardinality.putIfAbsent(partition, new HashSet<>());
            tupleCount.putIfAbsent(partition, new AtomicInteger(0));

            AtomicInteger count = tupleCount.get(partition);
            synchronized (count){
                if (count.get() < minCardinality) {
                    minCardinality = count.get();
                    choice = partition;
                }
            }

        }

        cardinality.putIfAbsent(choice, new HashSet<>());
        tupleCount.putIfAbsent(choice, new AtomicInteger(0));

        synchronized (cardinality.get(choice)) {
            HashSet<String> set = cardinality.get(choice);
            set.add(key);
            tupleCount.get(choice).getAndIncrement();
        }


        return choice;
    }

    public int[] generateHashes(String input) {
        int[] hashes = new int[n];

        for (int i = 0; i < n; i++) {
            hashes[i] = (int) (FastMath.abs(hashFunctions[i].hashBytes(input.getBytes()).asLong()) % this.parallelism); // Using a linear probing approach 31 helps distribute well
        }

        return hashes;
    }

    public int roundRobin(int numPartitions){
        index++;
        return Math.abs(index % numPartitions);
    }
}

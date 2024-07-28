package keygrouping;

import TopK.StreamSummaryHelper;
import com.clearspring.analytics.stream.StreamSummary;
import org.apache.commons.math3.util.FastMath;
import org.apache.flink.shaded.guava31.com.google.common.hash.HashFunction;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class CAMRoundRobinTopK extends keyGroupingBasic{
    public int n;
    private HashMap<Integer, Set<String>> cardinality;
    private HashMap<Integer, Integer> tupleCount;
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
        this.cardinality = new HashMap<>(numPartitions);
        this.tupleCount = new HashMap<>(numPartitions);
        streamSummary = new StreamSummary<String>(StreamSummaryHelper.capacity);
        totalItems = (long) 0;
        this.hashFunctions = createHashFunctions(n_choices);
    }

    @Override
    public int customPartition(String key, int numPartitions) {
        //special keygrouping for popular keys
        StreamSummaryHelper ssHelper = new StreamSummaryHelper();


        streamSummary.offer(key);
        HashMap<String,Long> freqList = ssHelper.getTopK(streamSummary,thresholdForTopK ,totalItems);
        if(freqList.containsKey(key)) {
            totalItems++;
            return roundRobin(numPartitions);
        }


        int[] hashes = generateHashes(key);

        for (int i = 0; i < n; i++) {
            int partition = hashes[i];
            cardinality.putIfAbsent(partition,new HashSet<>());
            tupleCount.putIfAbsent(partition, 0);

            Set<String> set = cardinality.get(partition);
            if (set.contains(key)) {
                tupleCount.put(partition, tupleCount.get(partition) + 1);
//                System.out.println("Choice: " + partition + " Key: " + key + " Partition: " + numPartitions + " TupleCount: " + tupleCount.get(partition).get() + " Cardinality: " + cardinality.get(partition).size());

                return partition;
            }
        }

        int minCount = Integer.MAX_VALUE;
        int choice = 0;

        for (int i = 0; i < n; i++) {
            int partition = hashes[i];
            cardinality.putIfAbsent(partition, new HashSet<>());
            tupleCount.putIfAbsent(partition, 0);

            int count = tupleCount.get(partition);
//            int currentCount = count.get();
            if (count < minCount) {
                minCount = count;
                choice = partition;
            }
        }

        cardinality.putIfAbsent(choice, Collections.newSetFromMap(new ConcurrentHashMap<>()));
        tupleCount.putIfAbsent(choice, 0);

        Set<String> set = cardinality.get(choice);

        set.add(key);
        tupleCount.put(choice, tupleCount.get(choice) + 1);
//        System.out.println("Choice: " + choice + " Key: " + key + " Partition: " + numPartitions + " TupleCount: " + tupleCount.get(choice).get() + " Cardinality: " + cardinality.get(choice).size());

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

package keygrouping;

import org.apache.flink.api.common.functions.Partitioner;

import java.util.HashSet;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class cam_roundRobin implements Partitioner<String> {
    public int n;
    private ConcurrentHashMap<Integer, HashSet<String>> cardinality;

    private ConcurrentHashMap<Integer, AtomicInteger> tupleCount;
    int parallelism;

    int index = 0;

    public cam_roundRobin(int n_choices, int numPartitions) {
//        if(numPartitions == 0){
//            numPartitions = 2;
//        }
        //n being the number of choices eg two choices etc...
        this.parallelism = numPartitions;
        this.n = n_choices;
        this.cardinality = new ConcurrentHashMap<Integer, HashSet<String>>(numPartitions);
        this.tupleCount = new ConcurrentHashMap<Integer, AtomicInteger>(numPartitions);
    }

    @Override
    public int partition(String key, int numPartitions) {
        if(Objects.equals(key, "ENDD")){
            return roundRobin(numPartitions);
        }


        //special keygrouping for popular keys
        if(key.equals("A") || key.equals("B") || key.equals("C")){
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
        int baseHash = input.hashCode();

        for (int i = 0; i < n; i++) {
            hashes[i] = Math.abs((baseHash + i * 31) % this.parallelism); // Using a linear probing approach 31 helps disitribute well
        }

        return hashes;
    }

    public int roundRobin(int numPartitions){
        index++;
        return Math.abs(index % numPartitions);
    }
}

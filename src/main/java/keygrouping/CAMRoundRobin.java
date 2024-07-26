package keygrouping;

import eventTypes.EventBasic;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class CAMRoundRobin extends keyGroupingBasic{
    public int n;
    private HashMap<Integer, Set<String>> cardinality;
    private HashMap<Integer, Integer> tupleCount;
    int parallelism;

    int index = 0;

    public CAMRoundRobin(int n_choices, int numPartitions) {
        super(numPartitions);
        //n being the number of choices eg two choices etc...
        this.parallelism = numPartitions;
        this.n = n_choices;
        this.cardinality = new HashMap<>(numPartitions);
        this.tupleCount = new HashMap<>(numPartitions);
    }

    @Override
    public int customPartition(String key, int numPartitions) {
        //special keygrouping for popular keys
        if(EventBasic.isPopularKey(key)){
            return roundRobin(numPartitions);
        }

        int[] hashes = generateHashes(key);

        for (int i = 0; i < n; i++) {
            int partition = hashes[i];
            cardinality.putIfAbsent(partition, Collections.newSetFromMap(new ConcurrentHashMap<>()));
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
            cardinality.putIfAbsent(partition, Collections.newSetFromMap(new ConcurrentHashMap<>()));
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

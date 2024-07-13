//package keygrouping;
//
//import StringConstants.StringConstants;
//import org.apache.flink.api.common.functions.Partitioner;
//
//import java.util.HashSet;
//import java.util.Objects;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.atomic.AtomicInteger;
//
//public class cam_n extends keyGroupingBasic {
//    public int n;
//    private ConcurrentHashMap<Integer, HashSet<String>> cardinality;
//
//    private ConcurrentHashMap<Integer, AtomicInteger> tupleCount;
//    int parallelism;
//
//
//    public cam_n(int n_choices, int numPartitions) {
//        super(numPartitions);
//
//        //n being the number of choices eg two choices etc...
//        this.parallelism = numPartitions;
//        this.n = n_choices;
//        this.cardinality = new ConcurrentHashMap<Integer, HashSet<String>>(numPartitions);
//        this.tupleCount = new ConcurrentHashMap<Integer, AtomicInteger>(numPartitions);
//    }
//
//    @Override
//    public int customPartition(String key, int numPartitions) {
//
//        int[] hashes = generateHashes(key);
//
//        for (int i = 0; i < n; i++) {
//            int partition = hashes[i];
//            cardinality.putIfAbsent(partition, new HashSet<>());
//            tupleCount.putIfAbsent(partition, new AtomicInteger(0));
//
//            synchronized (cardinality.get(partition)) { // Synchronize on the HashSet object for the partition
//                HashSet<String> set = cardinality.get(partition);
//                if (set.contains(key)) {
//                    tupleCount.get(partition).getAndIncrement();
//                    System.out.println("Choice: " + partition + " Key: " + key + " Partition: " + numPartitions + " TupleCount: " + tupleCount.get(partition).get() + " Cardinality: " + cardinality.get(partition).size());
//
//                    return partition;
//                }
//            }
//
//        }
//
//        int minCount = Integer.MAX_VALUE;
//        int choice = 0;
//
//
//        for (int i = 0; i < n; i++) {
//            int partition = hashes[i];
//            cardinality.putIfAbsent(partition, new HashSet<>());
//            tupleCount.putIfAbsent(partition, new AtomicInteger(0));
//
//            AtomicInteger count = tupleCount.get(partition);
//            synchronized (count){
//                if (count.get() < minCount) {
//                    minCount = count.get();
//                    choice = partition;
//                }
//            }
//
//        }
//
//        cardinality.putIfAbsent(choice, new HashSet<>());
//        tupleCount.putIfAbsent(choice, new AtomicInteger(0));
//
//        synchronized (cardinality.get(choice)) {
//            HashSet<String> set = cardinality.get(choice);
//            set.add(key);
//            tupleCount.get(choice).getAndIncrement();
//        }
//
//        System.out.println("Choice: " + choice + " Key: " + key + " Partition: " + numPartitions + " TupleCount: " + tupleCount.get(choice).get() + " Cardinality: " + cardinality.get(choice).size());
//        return choice;
//    }
//
//    public int[] generateHashes(String input) {
//        int[] hashes = new int[n];
//        int baseHash = input.hashCode();
//
//        for (int i = 0; i < n; i++) {
//            hashes[i] = Math.abs((baseHash + i * 31) % this.parallelism); // Using a linear probing approach 31 helps disitribute well
//        }
//
//        return hashes;
//    }
//
//}

package keygrouping;

import org.apache.commons.math3.util.FastMath;
import org.apache.flink.shaded.guava31.com.google.common.hash.HashFunction;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class cam_n extends keyGroupingBasic {
    public int n;
    private HashMap<Integer, Set<String>> cardinality;
    private HashMap<Integer, AtomicInteger> tupleCount;
    int parallelism;

    public HashFunction[] hashFunctions;
    public cam_n(int n_choices, int numPartitions) {
        super(numPartitions);
        if(n_choices > primeNumbers.length){
            throw new IllegalArgumentException("Number of choices must be less than or equal to " + primeNumbers.length);
        }

        // n being the number of choices eg two choices etc...
        this.parallelism = numPartitions;
        this.n = n_choices;
        this.cardinality = new HashMap<>(numPartitions);
        this.tupleCount = new HashMap<>(numPartitions);
        this.hashFunctions = createHashFunctions(n_choices);
    }

    @Override
    public int customPartition(String key, int numPartitions) {
        int[] hashes = generateHashes(key);

        for (int i = 0; i < n; i++) {
            int partition = hashes[i];
            cardinality.putIfAbsent(partition, Collections.newSetFromMap(new ConcurrentHashMap<>()));
            tupleCount.putIfAbsent(partition, new AtomicInteger(0));

            Set<String> set = cardinality.get(partition);
            if (set.contains(key)) {
                tupleCount.get(partition).incrementAndGet();
//                System.out.println("Choice: " + partition + " Key: " + key + " Partition: " + numPartitions + " TupleCount: " + tupleCount.get(partition).get() + " Cardinality: " + cardinality.get(partition).size());

                return partition;
            }
        }

        int minCount = Integer.MAX_VALUE;
        int choice = 0;

        for (int i = 0; i < n; i++) {
            int partition = hashes[i];
            cardinality.putIfAbsent(partition, Collections.newSetFromMap(new ConcurrentHashMap<>()));
            tupleCount.putIfAbsent(partition, new AtomicInteger(0));

            AtomicInteger count = tupleCount.get(partition);
            int currentCount = count.get();
            if (currentCount < minCount) {
                minCount = currentCount;
                choice = partition;
            }
        }

        cardinality.putIfAbsent(choice, Collections.newSetFromMap(new ConcurrentHashMap<>()));
        tupleCount.putIfAbsent(choice, new AtomicInteger(0));

        Set<String> set = cardinality.get(choice);
        synchronized (set) {
            set.add(key);
            tupleCount.get(choice).incrementAndGet();
        }
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
}


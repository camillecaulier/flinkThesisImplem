package keygrouping;

import StringConstants.StringConstants;
import org.apache.flink.api.common.functions.Partitioner;

import java.io.Serializable;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class cam_n_all_in_one  implements Partitioner<String>, Serializable {
    public int n;
    private ConcurrentHashMap<Integer, Set<String>> cardinality;
    private ConcurrentHashMap<Integer, AtomicInteger> tupleCount;
    int parallelism;

    public EndWindowPropagation endWindowPropagation;

    public cam_n_all_in_one(int n_choices, int numPartitions) {
        endWindowPropagation = new EndWindowPropagation(parallelism);

        // n being the number of choices eg two choices etc...
        this.parallelism = numPartitions;
        this.n = n_choices;
        this.cardinality = new ConcurrentHashMap<>(numPartitions);
        this.tupleCount = new ConcurrentHashMap<>(numPartitions);
    }

    @Override
    public int partition(String key, int numPartitions){
        if(key.equals(StringConstants.WINDOW_END)){
            return endWindowPropagation.endWindowRouting();
        }
        return customPartition(key, numPartitions);
    }

    public int customPartition(String key, int numPartitions) {
        int[] hashes = generateHashes(key);

        for (int i = 0; i < n; i++) {
            int partition = hashes[i];
            cardinality.putIfAbsent(partition, Collections.newSetFromMap(new ConcurrentHashMap<>()));
            tupleCount.putIfAbsent(partition, new AtomicInteger(0));

            Set<String> set = cardinality.get(partition);
            if (set.contains(key)) {
                tupleCount.get(partition).incrementAndGet();
                System.out.println("Choice: " + partition + " Key: " + key + " Partition: " + numPartitions + " TupleCount: " + tupleCount.get(partition).get() + " Cardinality: " + cardinality.get(partition).size());

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
        System.out.println("Choice: " + choice + " Key: " + key + " Partition: " + numPartitions + " TupleCount: " + tupleCount.get(choice).get() + " Cardinality: " + cardinality.get(choice).size());

        return choice;
    }

    public int[] generateHashes(String input) {
        int[] hashes = new int[n];
        int baseHash = input.hashCode();

        for (int i = 0; i < n; i++) {
            hashes[i] = Math.abs((baseHash + i * 31) % this.parallelism); // Using a linear probing approach 31 helps distribute well
        }

        return hashes;
    }
}


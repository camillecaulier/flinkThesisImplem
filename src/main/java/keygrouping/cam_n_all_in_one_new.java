package keygrouping;

import org.apache.flink.api.common.functions.Partitioner;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.collection.ISet;
import com.hazelcast.map.IMap;
import java.io.Serializable;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import StringConstants.StringConstants;

public class cam_n_all_in_one_new implements Partitioner<String>, Serializable {
    public int n;
    private transient HazelcastInstance hazelcastInstance;
    private transient IMap<Integer, ISet<String>> cardinality;
    private transient IMap<Integer, AtomicInteger> tupleCount;
    int parallelism;

    public EndWindowPropagation endWindowPropagation;

    public cam_n_all_in_one_new(int n_choices, int numPartitions) {
        this.hazelcastInstance = Hazelcast.newHazelcastInstance();
        this.parallelism = numPartitions;
        this.n = n_choices;

        this.endWindowPropagation = new EndWindowPropagation(parallelism);
    }

    @Override
    public int partition(String key, int numPartitions) {
        if (key.equals(StringConstants.WINDOW_END)) {
            return endWindowPropagation.endWindowRouting();
        }
        return customPartition(key, numPartitions);
    }

    public int customPartition(String key, int numPartitions) {
        this.cardinality = hazelcastInstance.getMap("CardinalityMap");
        this.tupleCount = hazelcastInstance.getMap("TupleCountMap");

        int[] hashes = generateHashes(key);
        int minCount = Integer.MAX_VALUE;
        int choice = 0;

        for (int i = 0; i < n; i++) {
            int partition = hashes[i];
            ISet<String> set = cardinality.computeIfAbsent(partition, k -> hazelcastInstance.getSet("set:" + k));
            AtomicInteger count = tupleCount.computeIfAbsent(partition, k -> new AtomicInteger(0));

            if (!set.contains(key)) {
                set.add(key);
                int currentCount = count.incrementAndGet();
                if (currentCount < minCount) {
                    minCount = currentCount;
                    choice = partition;
                }
            } else {
                return partition;
            }
        }

        return choice;
    }

    public int[] generateHashes(String input) {
        int[] hashes = new int[n];
        int baseHash = input.hashCode();

        for (int i = 0; i < n; i++) {
            hashes[i] = Math.abs((baseHash + i * 31) % this.parallelism);
        }

        return hashes;
    }

    // Ensure to properly shut down Hazelcast instance
    public void close() {
        hazelcastInstance.shutdown();
    }
}

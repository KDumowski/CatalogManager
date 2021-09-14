package partitions;

import akka.japi.function.Predicate;
import com.here.platform.data.client.javadsl.Partition;

import java.util.Arrays;
import java.util.List;

public class PartitionPredicates {
    public static Predicate<Partition> isPartitionWithinPartitions(String... partitions) {
        return partition -> {
            List<String> partitionIds = Arrays.asList(partitions);
            return partitionIds.isEmpty() || partitionIds.contains(partition.getPartition());
        };
    }
}

package layers;

import com.here.hrn.HRN;

public interface ILayerManager {

    void readLayerObjectByKey(String layer, String key);

    void publishObject(String layer, String key, byte[] blobData);

    void publishMultiplePartitionsToStreamLayer(int numberOfPartitions, HRN catalogHrn, String layerId);

    void publishToVersionedLayer(String layer, String newPartitionId, byte[] blobData);

    void publishToIndexedLayer(String layerId, String partitionId, byte[] data);

    void queryIndexLayer(String layerId, String query);

    void queryVersionedLayerForAllPartitions(String layerId, long version, String... partitions);

    void queryVersionedLayerForSpecificPartitionIds(String layerId, long version, String... partitions);
}

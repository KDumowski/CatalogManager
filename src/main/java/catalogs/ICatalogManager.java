package catalogs;

public interface ICatalogManager {

    void createCatalog(Catalog catalog);

    void catalogExists(String catalogId);

    void queryVersionedLayerForAllPartitions(String layerId, long version, String... partitions);

    void queryVersionedLayerForSpecificPartitionIds(String layerId, long version, String... partitions);

    void readLayerObjectByKey(String layer, String key);

    void publishObject(String layer, String key, byte[] data);

    void publishToVersionedLayer(String layer, String newPartitionId, byte[] blobData);

    void readCatalogs();

    void publishToIndexedLayer(String layerId, String partitionId, byte[] data);

    void queryIndexLayer(String layerId, String query);
}

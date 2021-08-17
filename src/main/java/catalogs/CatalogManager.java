package catalogs;

import akka.actor.ActorSystem;
import com.here.hrn.HRN;
import com.here.platform.data.client.javadsl.AdminApi;
import com.here.platform.data.client.javadsl.DataClient;
import com.here.platform.data.client.model.AutomaticVersionDeletion;
import com.here.platform.data.client.model.WritableCatalogConfiguration;
import layers.ILayerManager;
import layers.LayerManager;

import java.util.HashSet;

public class CatalogManager implements ICatalogManager {

    private final HRN hrn;

    private final ActorSystem actorSystem;

    private final ILayerManager layerManager;

    public CatalogManager(final ActorSystem actorSystem, final String hrnPath) {
        this.actorSystem = actorSystem;
        hrn = HRN.fromString(hrnPath);
        layerManager = new LayerManager(actorSystem, hrn);
    }

    @Override
    public void createCatalog(final Catalog catalog) {
        WritableCatalogConfiguration config =
                new WritableCatalogConfiguration.Builder()
                        .withId(catalog.getId())
                        .withName(catalog.getName())
                        .withSummary(catalog.getSummary())
                        .withDescription(catalog.getDescription())
                        .withTags(new HashSet<>(catalog.getTags()))
                        .withAutomaticVersionDeletion(AutomaticVersionDeletion.builder()
                                .withNumberOfVersionsToKeep(10L)
                                .build())
                        .withLayers(catalog.getWritableLayerBuilders())
                        .build();

        createAdminApi()
                .createCatalog(config)
                .thenApply(hrn -> {
                    System.out.println("Created new catalog `" + hrn + "`");
                    return processCreatedCatalog(hrn);
                });
    }

    @Override
    public void catalogExists(final String catalogId) {
        createAdminApi()
                .listCatalogs()
                .whenComplete(
                        (catalogHRNs, e) -> {
                            if (catalogHRNs != null) {
                                boolean catalogExists = catalogHRNs.stream().anyMatch(cat -> cat.toString().contains(catalogId));
                                if (catalogExists) {
                                    System.out.println("Catalog " + catalogId + " exists.");
                                }
                            } else if (e != null) {
                                e.printStackTrace();
                            }
                        })
                .thenAccept( unbound -> System.out.println("Accepted"));
    }

    @Override
    public void queryVersionedLayerForAllPartitions(final String layerId, long version, String... partitions) {
        layerManager.queryVersionedLayerForAllPartitions(layerId, version, partitions);
    }

    @Override
    public void queryVersionedLayerForSpecificPartitionIds(final String layerId, long version, String... partitions) {
        layerManager.queryVersionedLayerForSpecificPartitionIds(layerId, version, partitions);
    }

    @Override
    public void readLayerObjectByKey(final String layer, final String key) {
        layerManager.readLayerObjectByKey(layer, key);
    }

    @Override
    public void publishObject(final String layer, final String key, byte[] data) {
        layerManager.publishObject(layer, key, data);
    }

    @Override
    public void publishToVersionedLayer(String layer, String newPartitionId, byte[] blobData) {
        layerManager.publishToVersionedLayer(layer, newPartitionId, blobData);
    }

    @Override
    public void readCatalogs() {
        createAdminApi()
                .listCatalogs()
                .whenComplete(
                        (catalogHRNs, e) -> {
                            if (catalogHRNs != null) {
                                catalogHRNs.forEach(System.out::println);
                            } else if (e != null) {
                                e.printStackTrace();
                            }
                        })
                .thenAccept( unbound -> System.out.println("Accepted"));
                // When done, shutdown the Data Client through the ActorSystem
//                .thenAccept(unbound ->
//                        CoordinatedShutdown.get(actorSystem).run(CoordinatedShutdown.unknownReason()));
    }

    @Override
    public void publishToIndexedLayer(final String layerId, final String partitionId, byte[] data) {
        layerManager.publishToIndexedLayer(layerId, partitionId, data);
    }

    @Override
    public void queryIndexLayer(final String layerId, final String query) {
        layerManager.queryIndexLayer(layerId, query);
    }

    private AdminApi createAdminApi() {
        return DataClient.get(actorSystem).adminApi();
    }

    private Object processCreatedCatalog(HRN hrn) {
        return null;
    }
}

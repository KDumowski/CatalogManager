import akka.actor.ActorSystem;
import catalogs.CatalogManager;
import catalogs.CatalogUtils;
import catalogs.ICatalogManager;
import lombok.NoArgsConstructor;

import static catalogs.CatalogUtils.OBJECT_LAYER_ID;
import static catalogs.CatalogUtils.VERSIONED_LAYER_ID;

@NoArgsConstructor
public class Testing {

    public static final String CATALOG_NAME = "kon-dumko";

    public static final String OBJECT_KEY = "some-object";

    public static final String PARTITION_NAME = "some-partition";

    public static final String HRN_PATH = "hrn:here:data::olp-here:" + CATALOG_NAME;

    ICatalogManager catalogManager = new CatalogManager(ActorSystem.create(), HRN_PATH);

    public void testCatalogExists() {
        catalogManager.catalogExists(CATALOG_NAME);
    }

    public void testReadCatalogs() {
        catalogManager.readCatalogs();
    }

    public void testPublishObject() {
        catalogManager.publishObject(OBJECT_LAYER_ID, OBJECT_KEY, createData());
    }

    public void testReadLayerObjectByKey() {
        catalogManager.readLayerObjectByKey(OBJECT_LAYER_ID, OBJECT_KEY);
    }

    public void testCreateCatalog() {
        catalogManager.createCatalog(CatalogUtils.createSampleCatalog(CATALOG_NAME));
    }

    public void testPublishToVersionedLayer() {
        catalogManager.publishToVersionedLayer(VERSIONED_LAYER_ID, PARTITION_NAME, createData());
    }

    public void testPublishToIndexedLayer() {
        catalogManager.publishToIndexedLayer("layer-2", "partition-0-in-indexed-layer", createData());
    }

    public void testQueryIndexedLayer() {
        catalogManager.queryIndexLayer( "layer-2", "someIntKey==42");
    }

    private static byte[] createData() {
        return "Hello World".getBytes();
    }
}

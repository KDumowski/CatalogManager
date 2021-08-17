public class Main {

    private static final Testing testing = new Testing();

    public static void main(String[] args) {
        testing.testCatalogExists();
        testing.testReadCatalogs();
        testing.testPublishObject();
        testing.testReadLayerObjectByKey();
        testing.testCreateCatalog();
        testing.testPublishToVersionedLayer();
        testing.testPublishToIndexedLayer();
        testing.testQueryIndexedLayer();
    }
}

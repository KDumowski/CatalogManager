package catalogs;

import com.here.platform.data.client.model.*;
import scala.Option;
import scala.collection.immutable.HashSet;

import java.time.ZonedDateTime;
import java.util.Arrays;

public class CatalogUtils {

    public static final String VERSIONED_LAYER_ID = "layer-id-0";

    public static final String INDEXED_LAYER_ID = "layer-id-1";

    public static final String OBJECT_LAYER_ID = "layer-id-2";

    public static final String INTERACTIVE_LAYER_ID = "layer-id-3";

    public static Catalog createSampleCatalog(final String catalogId)
    {
        return new Catalog(
                catalogId,
                catalogId + "name",
                "This is a catalog summary.",
                "This is what the catalog is for.",
                Arrays.asList("tag1", "tag2", "tag3"),
                Arrays.asList(
                        new Layer(VERSIONED_LAYER_ID,
                                "VERSIONED",
                                "layer-summary-0",
                                "layer-description-0",
                                LayerTypes.Versioned(),
                                Partitioning.Generic(),
                                Volumes.Durable(),
                                Option.empty(),
                                "application/x-protobuf",
                                Option.empty(),
                                new HashSet<>(),
                                Option.empty(),
                                new HashSet<>(),
                                Option.empty(),
                                ZonedDateTime.now(),
                                Option.empty(),
                                Option.apply(CrcAlgorithm.CRC32C())
                                ),
                        new Layer(INDEXED_LAYER_ID,
                                "INDEXED",
                                "layer-summary-1",
                                "layer-description-1",
                                new IndexLayerType.Builder()
                                        .addIndexDefinition(
                                                new IndexDefinition.Builder()
                                                        .withName("someIntKey")
                                                        .withIndexType(IndexType.Int)
                                                        .build())
                                        .addIndexDefinition(
                                                new IndexDefinition.Builder()
                                                        .withName("someStringKey")
                                                        .withIndexType(IndexType.String)
                                                        .build())
                                        .addIndexDefinition(
                                                new IndexDefinition.Builder()
                                                        .withName("someTimeWindowKey")
                                                        .withIndexType(IndexType.TimeWindow)
                                                        .withDuration(3600000L)
                                                        .build())
                                        .addIndexDefinition(
                                                new IndexDefinition.Builder()
                                                        .withName("someHereTileKey")
                                                        .withIndexType(IndexType.HereTile)
                                                        .withZoomLevel(8)
                                                        .build())
                                        .withTtl(Ttl.OneMonth)
                                        .build(),
                                Partitioning.NoPartition(),
                                Volumes.Durable(),
                                Option.empty(),
                                "application/x-protobuf",
                                Option.empty(),
                                new HashSet<>(),
                                Option.empty(),
                                new HashSet<>(),
                                Option.empty(),
                                ZonedDateTime.now(),
                                Option.empty(),
                                Option.apply(CrcAlgorithm.CRC32C())
                        ),
                        new Layer(OBJECT_LAYER_ID,
                                "OBJECT STORE",
                                "layer-summary-2",
                                "layer-description-2",
                                LayerTypes.ObjectStore(),
                                Partitioning.NoPartition(),
                                Volumes.Durable(),
                                Option.empty(),
                                "application/x-protobuf",
                                Option.empty(),
                                new HashSet<>(),
                                Option.empty(),
                                new HashSet<>(),
                                Option.empty(),
                                ZonedDateTime.now(),
                                Option.empty(),
                                Option.apply(CrcAlgorithm.CRC32C())
                        ),
                        new Layer(INTERACTIVE_LAYER_ID,
                                "INTERACTIVE",
                                "layer-summary-3",
                                "layer-description-3",
                                new InteractiveMapLayerType.Builder()
                                        .withInteractiveMapProperties(
                                                new InteractiveMapProperties.Builder()
                                                        .withSearchableProperties(
                                                                Arrays.asList("some-property1", "some-property-2"))
                                                        .build())
                                        .build(),
                                Partitioning.NoPartition(),
                                Volumes.Durable(),
                                Option.empty(),
                                "application/x-protobuf",
                                Option.empty(),
                                new HashSet<>(),
                                Option.empty(),
                                new HashSet<>(),
                                Option.empty(),
                                ZonedDateTime.now(),
                                Option.empty(),
                                Option.apply(CrcAlgorithm.CRC32C())
                        )
                )
        );
    }
}

package layers;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.actor.CoordinatedShutdown;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.util.ByteString;
import com.here.hrn.HRN;
import com.here.platform.data.client.engine.javadsl.DataEngine;
import com.here.platform.data.client.engine.javadsl.ReadEngine;
import com.here.platform.data.client.engine.javadsl.WriteEngine;
import com.here.platform.data.client.javadsl.*;
import com.here.platform.data.client.model.AdditionalFields;
import com.here.platform.data.client.model.ByteRange;
import com.here.platform.data.client.model.PendingPartition;
import com.here.platform.data.client.model.VersionDependency;
import com.here.platform.data.client.settings.ConsumerSettings;
import com.here.v1.MapScheme;
import lombok.AllArgsConstructor;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@AllArgsConstructor
public class LayerManager implements ILayerManager {

    private final ActorSystem actorSystem;

    private final HRN hrn;

    @Override
    public void readLayerObjectByKey(final String layer, final String key) {
        ReadEngine readEngine = DataEngine.get(actorSystem).readEngine(hrn);

        CompletionStage<Source<ByteString, NotUsed>> dataAsSource =
                readEngine.getObjectDataAsSource(layer, key, ByteRange.all());

        dataAsSource.whenComplete((data, e) -> {
            if (data != null) {
                data.runForeach((byteString -> System.out.println(byteString.utf8String())), Materializer.matFromSystem(actorSystem));
            }
        }).thenAccept( unbound -> System.out.println("Accepted"));
    }

    @Override
    public void publishObject(final String layer, final String key, byte[] blobData) {
        WriteEngine writeEngine = DataEngine.get(actorSystem).writeEngine(hrn);

        CompletableFuture<Void> futureUploadObject =
                writeEngine
                        .uploadObject(
                                layer,
                                key,
                                new com.here.platform.data.client.scaladsl.NewPartition.ByteArrayData(blobData),
                                Optional.empty(),
                                Optional.empty())
                        .toCompletableFuture()
                        .thenAccept( unbound -> System.out.println("Accepted"));
    }

    @Override
    public void publishMultiplePartitionsToStreamLayer(int numberOfPartitions, HRN catalogHrn, String layerId) {
        List<String> partitionIdsList = new ArrayList<>();
        List<byte[]> partitionData = new ArrayList<>();
        for (int i = 0; i < numberOfPartitions ; i++) {
            partitionIdsList.add(String.valueOf(i));
            partitionData.add(createStreamPartitionData(i));
        }
        publishPartitionsToStreamLayer(catalogHrn, layerId, partitionIdsList, partitionData);
    }

    @Override
    public void publishToVersionedLayer(String layer, String newPartitionId, byte[] blobData) {
        // create writeEngine for source catalog
        WriteEngine writeEngine = DataEngine.get(actorSystem).writeEngine(hrn);

        // parallelism defines how many parallel requests would be made to fetch the data
        int parallelism = 10;

        // list of dependencies for this publication
        List<VersionDependency> dependencies = Collections.emptyList();

        NewPartition newPartition =
                new NewPartition.Builder()
                        .withPartition(newPartitionId)
                        .withData(blobData)
                        .withLayer(layer)
                        .build();

        ArrayList<PendingPartition> partitionList = new ArrayList<>();
        partitionList.add(newPartition);

        Source<PendingPartition, NotUsed> partitions = Source.from(partitionList);

        CompletableFuture<Void> futurePublish =
                writeEngine
                        .publishBatch2(parallelism, Optional.of(Arrays.asList(layer)), dependencies, partitions)
                        .toCompletableFuture()
                        .thenAccept(unbound -> System.out.println("Publish to versioned finished"));
    }

    @Override
    public void publishToIndexedLayer(final String layerId, final String partitionId, byte[] data) {

        WriteEngine writeEngine = DataEngine.get(actorSystem).writeEngine(hrn);

        // parallelism defines how many parallel requests would be made to fetch the data
        int parallelism = 10;

        // list of dependencies for this publication
        List<VersionDependency> dependencies = Collections.emptyList();

        NewPartition newPartition =
                new NewPartition.Builder()
                        .withPartition("")
                        .withLayer(layerId)
                        .withData(data)
                        .addIntField("someIntKey", 42)
                        .addStringField("someStringKey", "abc")
                        .addBooleanField("someBooleanKey", true)
                        .addTimeWindowField("someTimeWindowKey", 123456789L)
                        .addHereTileField("someHereTileKey", 91956L)
                        .addMetadata("someKey1", "someValue1")
                        .addMetadata("someKey2", "someValue2")
                        .withChecksum(Optional.empty())
                        .withDataSize(OptionalLong.of(0))
                        .build();

        Iterator<NewPartition> partitions = Arrays.asList(newPartition).iterator();
        CompletionStage<Done> publish = writeEngine.uploadAndIndex(partitions).whenComplete((done, throwable) -> {
            System.out.println("Publish and indexing complete!");
        });
    }

    @Override
    public void queryIndexLayer(final String layerId, final String query) {
        QueryApi queryApi = DataClient.get(actorSystem).queryApi(hrn);
        ReadEngine readEngine = DataEngine.get(actorSystem).readEngine(hrn);
        int numberOfParts = 50;

        IndexParts indexParts =
                queryApi.queryIndexParts(layerId, numberOfParts).toCompletableFuture().join();


        Source<IndexPartition, NotUsed> indexPartitionsSource =
                Source.from(indexParts.getParts())
                        .mapAsync(10, part -> queryApi.queryIndex(layerId, query, part))
                        .flatMapConcat(s -> s);

        ActorMaterializer actorMaterializer = ActorMaterializer.create(actorSystem);

        System.out.println(
                "Download the data corresponding to the index partitions previously found by the queryIndex method");

        int parallelism = 10;
        indexPartitionsSource
                .mapAsyncUnordered(parallelism, readEngine::getDataAsBytes)
                // Replace the method youCanProcessTheDataHere with your own code.
                .runForeach(this::processData, actorMaterializer)
                .toCompletableFuture()
                .join();

        System.out.println(
                "Computation finished. Shutting down the HTTP connections and the actor system.");
        CoordinatedShutdown.get(actorSystem)
                .runAll(CoordinatedShutdown.unknownReason())
                .toCompletableFuture()
                .join();
    }

    @Override
    public void queryVersionedLayerForAllPartitions(final String layerId, long version, String... partitions) {
        QueryApi queryApi = DataClient.get(actorSystem).queryApi(hrn);

        // parallelism defines how many parallel requests would be made to fetch the data
        int parallelism = 10;

        // create readEngine for source catalog
        ReadEngine readEngine = DataEngine.get(actorSystem).readEngine(hrn);

        // stream of tuple of (partition, bytes)
        CompletionStage<Source<Pair<Partition, byte[]>, NotUsed>> dataAsBytes =
                queryApi
                        // Get all partitions filtered if any partitions specified with the exact version - cannot be without
                        .getPartitions(version, layerId, AdditionalFields.AllFields())
                        .thenApply(
                                metadata ->
                                        metadata.filter(partition -> {
                                            List<String> partitionIds = Arrays.asList(partitions);
                                            return partitionIds.isEmpty() || partitionIds.contains(partition.getPartition());
                                        })
                                        .mapAsync(
                                                parallelism,
                                                partition ->
                                                        readEngine
                                                                .getDataAsBytes(partition)
                                                                .thenApply(data -> new Pair<>(partition, data))));
        // Read partitions and their contents
        dataAsBytes.whenComplete((source, notUsed) -> source.runWith(Sink.foreach(
                param -> System.out.println(param.first().getPartition() + ": " + new String(param.second()))),
                Materializer.matFromSystem(actorSystem)));
    }

    @Override
    public void queryVersionedLayerForSpecificPartitionIds(final String layerId, long version, String... partitions) {
        DataClient.get(actorSystem).queryApi(hrn)
             .getPartitionsById(version, layerId, Arrays.asList(partitions), AdditionalFields.AllFields())
             .whenComplete((partitionList, throwable) -> partitionList.forEach(System.out::println));
    }

    private void processData(Object o) {
        System.out.println(o);
    }

    private void publishPartitionsToStreamLayer(HRN catalogHrn, String layer, List<String> newPartitionsIdList,
                                                List<byte[]> blobDataList) {
        // create writeEngine and queryApi for a catalog
        QueryApi queryApi = DataClient.get(actorSystem).queryApi(catalogHrn);
        WriteEngine writeEngine = DataEngine.get(actorSystem).writeEngine(catalogHrn);

        // subscribe to receive new publications from stream layer
        queryApi.subscribe(
                layer,
                new ConsumerSettings.Builder().withGroupName("test-consumer").build(),
                partition -> processPartition(partition));

        ArrayList<PendingPartition> partitionList = new ArrayList<>();
        int i = 0;
        for (String partitionId: newPartitionsIdList) {
            NewPartition newPartition =
                    new NewPartition.Builder()
                            .withPartition(partitionId)
                            .withData(blobDataList.get(i++))
                            .withLayer(layer)
                            .build();
            partitionList.add(newPartition);
        }

        Source<PendingPartition, NotUsed> partitions = Source.from(partitionList);

        writeEngine.publish(partitions);
    }

    private byte[] createStreamPartitionData(int i) {
        return MapScheme.MainProtobufMessage.newBuilder()
                .setAlt(1)
                .setLat(2)
                .setLon(3)
                .build()
                .toByteArray();
    }

    private void processPartition(Partition partition) {
        System.out.println("got data from stream layer: " + partition);
    }
}

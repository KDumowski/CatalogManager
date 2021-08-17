package catalogs;

import com.here.platform.data.client.model.*;
import lombok.AllArgsConstructor;
import lombok.Value;

import java.util.List;
import java.util.stream.Collectors;

@Value
@AllArgsConstructor
public class Catalog {
    String id;
    String name;
    String summary;
    String description;
    List<String> tags;
    List<Layer> layers;

    public List<WritableLayer.Builder> getWritableLayerBuilders() {
        return layers.stream().map(layer ->
                                new WritableLayer.Builder()
                                    .withId(layer.getId())
                                    .withName(layer.getName())
                                    .withSummary(layer.getSummary())
                                    .withDescription(layer.getDescription())
                                    .withLayerType(layer.getLayerType())
                                    .withPartitioning(layer.getPartitioning())
                                    .withVolume(Volumes.Durable())
                                    .withContentType(layer.getContentType())
                                    .withCrc(CrcAlgorithm.CRC32C()))
                            .collect(Collectors.toList());
    }
}

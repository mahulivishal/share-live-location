package vishal.share.live.location.map;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import vishal.share.live.location.model.LocationData;

@Slf4j
public class LocationDataMapper implements MapFunction<String, LocationData> {
    private final ObjectMapper mapper = new ObjectMapper();

    public LocationData map (String event) throws Exception {
        try {
            LocationData locationData = mapper.readValue(event, LocationData.class);
            log.debug("locationData: {}", locationData);
            return locationData;
        } catch (Exception e) {
            log.error("locationData ERROR: {}", event);
            log.error("Error: ", e);
            return null;
        }
    }
}

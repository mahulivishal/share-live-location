package vishal.share.live.location.process;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import vishal.share.live.location.model.LocationData;
import vishal.flink.overspeed.alert.model.DeviceState;

import java.io.IOException;

@Slf4j
public class ShareLiveLocationProcessor extends KeyedProcessFunction<String, LocationData, String> {

    private final ObjectMapper mapper = new ObjectMapper();

    private transient ValueState<DeviceState> deviceStateValueState;

    @Override
    public void open(OpenContext openContext) throws Exception {
        ValueStateDescriptor<DeviceState> deviceStateValueStateDescriptor = new ValueStateDescriptor<>("device_gps_state", DeviceState.class);
        this.deviceStateValueState = getRuntimeContext().getState(deviceStateValueStateDescriptor);
    }

    @Override
    public void processElement(LocationData locationData, KeyedProcessFunction<String, LocationData, String>.Context context, Collector<String> collector) throws Exception {
        DeviceState deviceState = deviceStateValueState.value();
        if(null == deviceState){
            log.info("Initialising deviceState for: {}", locationData.getDeviceId());
            updateState(locationData);
            collector.collect(mapper.writeValueAsString(locationData));
        }
        else {
           if(locationData.getTimestamp() > deviceState.getTimestamp()) {
             if(!locationData.getLatitude().equals(deviceState.getLatitude()))  {
                 if(!locationData.getLongitude().equals(deviceState.getLongitude())){
                     updateState(locationData);
                     collector.collect(mapper.writeValueAsString(locationData));
                 }
             }
           }else {
               log.info("Out of Order Data: {}", locationData);
           }
        }
    }

    public void updateState(LocationData locationData) throws IOException {
        DeviceState deviceState = deviceStateValueState.value();
        deviceState = DeviceState.builder()
                .deviceId(locationData.getDeviceId())
                .latitude(locationData.getLatitude())
                .longitude(locationData.getLongitude())
                .timestamp(locationData.getTimestamp())
                .build();
        deviceStateValueState.update(deviceState);
    }
}

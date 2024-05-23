package vishal.flink.overspeed.alert.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class DeviceState {
    public String deviceId;
    public Double latitude;
    public Double longitude;
    public Long timestamp;
}


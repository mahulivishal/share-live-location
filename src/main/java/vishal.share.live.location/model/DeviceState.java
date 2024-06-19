package vishal.share.live.location.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class DeviceState {
    public String deviceId;
    public Double latitude;
    public Double longitude;
    public Long timestamp;
}


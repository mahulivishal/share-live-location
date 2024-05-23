package vishal.share.live.location;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vishal.flink.overspeed.alert.filters.NullFilters;
import vishal.flink.overspeed.alert.process.OverSpeedProccessor;
import vishal.flink.overspeed.alert.sink.SSESink;
import vishal.share.live.location.map.LocationDataMapper;
import vishal.share.live.location.model.LocationData;

import java.util.Properties;

public class App {
    private static Logger logger = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///Users/g0006686/Desktop/checkpoints/share-live-location");
        env.enableCheckpointing(5000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000L);
        env.getCheckpointConfig().setCheckpointTimeout(2000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig()
                .enableExternalizedCheckpoints(
                        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // Setting state backend with incremental checkpointing enabled
        process(env);
        env.execute("Vishal Share Live Location");
    }

    private static void process(StreamExecutionEnvironment env) throws Exception {
        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", "localhost:9092");
        consumerProps.setProperty("group.id", "location-pings");
        Properties producerProps = new Properties();
        producerProps.setProperty("bootstrap.servers", "localhost:9092");
        producerProps.setProperty("acks", "1");
        String sseEndpoint = "http://localhost:8083/share-live-location/";

        FlinkKafkaConsumer<String> locationDataSource = new FlinkKafkaConsumer<String>("share.live.location.gps.source.v1",
                new SimpleStringSchema(), consumerProps);
        DataStream<String> locationDataStream = env.addSource(locationDataSource).setParallelism(1).name("location-data-source")
                .map(new LocationDataMapper()).setParallelism(1).name("data-mapper")
                .filter(new NullFilters<LocationData>()).setParallelism(1).name("null-filter")
                .keyBy(LocationData::getDeviceId)
                .process(new OverSpeedProccessor()).setParallelism(1).name("location-ping-processor");
        locationDataStream.addSink(new SSESink(sseEndpoint)).setParallelism(1).name("sse-sink");
    }

    }

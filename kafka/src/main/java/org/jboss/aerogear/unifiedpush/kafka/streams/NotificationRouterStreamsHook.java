package org.jboss.aerogear.unifiedpush.kafka.streams;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.enterprise.concurrent.ManagedExecutorService;

import net.wessendorf.kafka.serialization.CafdiSerdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.jboss.aerogear.unifiedpush.kafka.CustomSerdes.InternalPushMessageSerde;
import org.jboss.aerogear.unifiedpush.message.InternalUnifiedPushMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;


@Singleton
@Startup
public class NotificationRouterStreamsHook {

    private final static Logger logger = LoggerFactory.getLogger(NotificationRouterStreamsHook.class);

    private KafkaStreams streams;

    @Resource(name = "DefaultManagedExecutorService")
    private ManagedExecutorService executor;


    @PostConstruct
    private void startup() {
        logger.warn("TRIGGERED THE NOTIFICATION ROUTER");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "notification-router-streams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "172.18.0.3:9092");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, InternalPushMessageSerde.class);
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, InternalPushMessageSerde.class);

        // Initialize specific serializers to be used later
        final Serde<InternalUnifiedPushMessage> pushMessageSerde = CafdiSerdes.serdeFrom(InternalUnifiedPushMessage.class);
        // final Serde<PushApplication> pushApplicationSerdeSerde = CafdiSerdes.serdeFrom(PushApplication.class);
        final Serde<String> stringSerde = CafdiSerdes.serdeFrom(String.class);

        KStreamBuilder builder = new KStreamBuilder();

        // Read from the source stream
        KStream<InternalUnifiedPushMessage, InternalUnifiedPushMessage> source = builder.stream("my-topic3-test");

        // Get variants for each message

        KStream<InternalUnifiedPushMessage, String> transformed = source.flatMap(
                // Here, we generate two output records for each input record.
                // We also change the key and value types.
                // Example: (345L, "Hello") -> ("HELLO", 1000), ("hello", 9000)
                (key, value) -> {
                    List<KeyValue<InternalUnifiedPushMessage, String>> result = new LinkedList<>();
                    result.add(KeyValue.pair(key, value.getIpAddress()));
                    result.add(KeyValue.pair(key, value.getIpAddress() + " TROLL "));

                    logger.warn(result.toString());
                    return result;
                }
        ).through(pushMessageSerde, stringSerde, "topic-cool-beans");

        //transformed.to(pushMessageSerde, stringSerde, "topic-cool-beans");
        transformed.foreach((key, value) -> logger.warn(key + " => " + value));

        streams = new KafkaStreams(builder, props);
        streams.start();
    }


    @PreDestroy
    private void shutdown() {
        logger.debug("Shutting down the streams.");
        streams.close();
    }

}
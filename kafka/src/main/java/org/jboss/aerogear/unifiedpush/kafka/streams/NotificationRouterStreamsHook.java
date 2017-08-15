package org.jboss.aerogear.unifiedpush.kafka.streams;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.enterprise.concurrent.ManagedExecutorService;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;

import net.wessendorf.kafka.serialization.CafdiSerdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.jboss.aerogear.unifiedpush.api.PushApplication;
import org.jboss.aerogear.unifiedpush.api.Variant;
import org.jboss.aerogear.unifiedpush.kafka.CustomSerdes.InternalPushMessageSerde;
import org.jboss.aerogear.unifiedpush.kafka.CustomSerdes.PushApplicationSerde;
import org.jboss.aerogear.unifiedpush.message.InternalUnifiedPushMessage;
import org.jboss.aerogear.unifiedpush.service.GenericVariantService;
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

    @Inject
    private Instance<GenericVariantService> genericVariantService;

    @PostConstruct
    private void startup() {
        logger.warn("TRIGGERED THE NOTIFICATION ROUTER");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "notification-router-streams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "172.18.0.3:9092");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, PushApplicationSerde.class);
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, InternalPushMessageSerde.class);

        // Initialize specific serializers to be used later
        final Serde<InternalUnifiedPushMessage> pushMessageSerde = CafdiSerdes.serdeFrom(InternalUnifiedPushMessage.class);
        final Serde<PushApplication> pushApplicationSerde = CafdiSerdes.serdeFrom(PushApplication.class);
        final Serde<Variant> variantSerde = CafdiSerdes.serdeFrom(Variant.class);

        KStreamBuilder builder = new KStreamBuilder();

        // Read from the source stream
        KStream<PushApplication, InternalUnifiedPushMessage> source = builder.stream("my-topic4-test");

        // Get variants for each message

        KStream<PushApplication, Variant> transformed = source.flatMap(
                // Here, we generate two output records for each input record.
                // We also change the key and value types.
                // Example: (345L, "Hello") -> ("HELLO", 1000), ("hello", 9000)
                (app, message) -> {
                    List<KeyValue<PushApplication, Variant>> result = new LinkedList<>();

                    // get variant ids
                    final List<String> variantIDs = message.getCriteria().getVariants();

                    variantIDs.forEach(
                            (variantID) -> result.add(KeyValue.pair(app, genericVariantService.get().findByVariantID(variantID)))
                    );

                    logger.warn(result.toString());
                    return result;
                }
        ).through(pushApplicationSerde, variantSerde, "topic-cool3-beans");

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
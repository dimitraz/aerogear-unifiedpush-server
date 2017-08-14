package org.jboss.aerogear.unifiedpush.kafka.streams;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.enterprise.concurrent.ManagedExecutorService;

import net.wessendorf.kafka.cdi.extension.VerySimpleEnvironmentResolver;
import net.wessendorf.kafka.serialization.CafdiSerdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.jboss.aerogear.unifiedpush.kafka.serdes.InternalUnifiedPushMessageSerde;
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
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "notification-router-streams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, VerySimpleEnvironmentResolver.simpleBootstrapServerResolver("#{KAFKA_HOST}:#{KAFKA_PORT}"));
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, InternalUnifiedPushMessageSerde.class);
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, InternalUnifiedPushMessageSerde.class);

        // Initialize specific serializers to be used later
        final Serde<InternalUnifiedPushMessage> pushMessageSerde = CafdiSerdes.serdeFrom(InternalUnifiedPushMessage.class);
        final Serde<String> stringSerde = CafdiSerdes.serdeFrom(String.class);

        KStreamBuilder builder = new KStreamBuilder();

        // Read from the source stream
        KStream<InternalUnifiedPushMessage, InternalUnifiedPushMessage> source = builder.stream("my-topic3-test");

        // For each unified push message add two records to the resulting stream
        // with key/value pair (message, ipAdress)
        KStream<InternalUnifiedPushMessage, String> transformed = source.flatMap(
                (key, value) -> {
                    List<KeyValue<InternalUnifiedPushMessage, String>> result = new LinkedList<>();
                    result.add(KeyValue.pair(key, value.getIpAddress()));
                    result.add(KeyValue.pair(key, value.getIpAddress() + " test"));

                    logger.warn(result.toString());
                    return result;
                })
                .through(pushMessageSerde, stringSerde, "topic-cool-be4ans");

        // transformed.to(pushMessageSerde, stringSerde, "topic-cool-beans");
        transformed.foreach((key, value) -> logger.warn(key + " => " + value));


        /*****************************************************************************

        // Get variants for each message
        KStream<InternalUnifiedPushMessage, Variant> getVariants = source.flatMap(
            (app, message) -> {
                // collections for all the different variants:
                final List<Variant> variants = new ArrayList<>();
                final List<String> variantIDs = message.getCriteria().getVariants();

                if (variantIDs != null) {
                    variantIDs.forEach(variantID -> {
                        Variant variant = genericVariantService.get().findByVariantID(variantID);

                        // does the variant exist ?
                        if (variant != null) {
                            variants.add(variant);
                        }
                    });
                } else {
                    // No specific variants have been requested,
                    // we get all the variants, from the given PushApplicationEntity:
                    variants.addAll(app.getVariants());
                }

                logger.warn("got variants.." + variants.toString());

                List<KeyValue<InternalUnifiedPushMessage, Variant>> result = new LinkedList<>();
                variants.forEach((variant) -> {
                    result.add(KeyValue.pair(message, variant));
                });

                logger.warn("added key value pair to results list..");
                return result;
            }
        );

        KStream<InternalUnifiedPushMessage, Variant>[] branches = getVariants.branch(
                (message, variant) -> true
        );

        branches[0].foreach((key, value) ->
                logger.warn(key + " " + value)
        );

        ****************************************************************************/

        streams = new KafkaStreams(builder, props);
        streams.start();
    }


    @PreDestroy
    private void shutdown() {
        logger.debug("Shutting down the streams.");
        streams.close();
    }

}
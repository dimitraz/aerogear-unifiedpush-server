/**
 * JBoss, Home of Professional Open Source
 * Copyright Red Hat, Inc., and individual contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jboss.aerogear.unifiedpush.message;

import net.wessendorf.kafka.serialization.CafdiSerdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.jboss.aerogear.unifiedpush.api.PushApplication;
import org.jboss.aerogear.unifiedpush.api.Variant;
import org.jboss.aerogear.unifiedpush.message.token.TokenLoader;
import org.jboss.aerogear.unifiedpush.message.holder.MessageHolderWithVariants;
import org.jboss.aerogear.unifiedpush.message.jms.DispatchToQueue;
import org.jboss.aerogear.unifiedpush.service.GenericVariantService;
import org.jboss.aerogear.unifiedpush.service.metrics.PushMessageMetricsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.enterprise.event.Event;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import java.util.*;


/**
 * Takes a request for sending {@link UnifiedPushMessage} and submits it to messaging subsystem for further processing.
 *
 * Router splits messages to specific variant types (push network type) so that they can be processed separately,
 * giving attention to limitations and requirements of specific push networks.
 *
 * {@link NotificationRouter} receives a request for sending a {@link UnifiedPushMessage} and queues one message per variant type, both in transaction.
 * The transactional behavior makes sure the request for sending notification is recorded and then asynchronously processed.
 *
 * The further processing of the push message happens in {@link TokenLoader}.
 */
@Stateless
public class NotificationRouter {

    private final Logger logger = LoggerFactory.getLogger(NotificationRouter.class);

    private final String NOTIFCATION_ROUTER_INPUT_TOPIC = "my-topic2-test";

    @Inject
    private Instance<GenericVariantService> genericVariantService;
    @Inject
    private PushMessageMetricsService metricsService;

    @Inject
    @DispatchToQueue
    private Event<MessageHolderWithVariants> dispatchVariantMessageEvent;

    /**
     * Receives a request for sending a {@link UnifiedPushMessage} and queues one message per variant type, both in one transaction.
     *
     * Once this method returns, message is recorded and will be eventually delivered in the future.
     *
     */
    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public void submit() {

        logger.warn("TRIGGERED THE NOTIFICATION ROUTER");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "notification-router-streams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "172.18.0.3:9092");
        // props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, PushAppSerde.class);
        // props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, InternalPushMessageSerde.class);

        // Initialize specific serializers to be used later
        final Serde<InternalUnifiedPushMessage> pushMessageSerde = CafdiSerdes.serdeFrom(InternalUnifiedPushMessage.class);
        final Serde<PushApplication> pushApplicationSerdeSerde = CafdiSerdes.serdeFrom(PushApplication.class);
        final Serde<Variant> variantSerde = CafdiSerdes.serdeFrom(Variant.class);

        KStreamBuilder builder = new KStreamBuilder();

        // Read from the source stream
        KStream<PushApplication, InternalUnifiedPushMessage> source = builder.stream(NOTIFCATION_ROUTER_INPUT_TOPIC);

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

       /*KStream<String, Integer> getVariants = source.flatMap(
                // Here, we generate two output records for each input record.
                // We also change the key and value types.
                // Example: (345L, "Hello") -> ("HELLO", 1000), ("hello", 9000)
                (key, value) -> {
                    List<KeyValue<String, Integer>> result = new LinkedList<>();
                    result.add(KeyValue.pair(value.getIpAddress(), 1000));
                    result.add(KeyValue.pair(value.getIpAddress(), 9000));
                    return result;
                }
        );*/


        getVariants.to(pushMessageSerde, variantSerde, "topic-cool-beans");
        getVariants.foreach((key, value) -> logger.warn(key + " => " + value));

        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();

        // getVariants.to(pushMessageSerde, variantSerde, "topic-cool-beans");

/*        logger.warn("done");
        logger.warn("branching..");
        KStream<InternalUnifiedPushMessage, Variant>[] branches = getVariants.branch(
               /*(message, variant) -> variant.getType().equals(ADM),
               (message, variant) -> variant.getType().equals(ANDROID),
                (message, variant) -> variant.getType().equals(IOS),
                (message, variant) -> variant.getType().equals(SIMPLE_PUSH),
                (message, variant) -> variant.getType().equals(WINDOWS_MPNS),
                (message, variant) -> variant.getType().equals(WINDOWS_WNS),
                (message, variant) -> true,
                (message, variant) -> true
        );

        logger.warn("branches length: " + branches.length);

        branches[0].foreach((key, value) ->
                logger.warn(key + " " + value)
        );


        branches[0].to(pushMessageSerde, variantSerde,"my-cool-topic3"); */

        /*case ADM:
        return admTokenBatchQueue;
        case ANDROID:
        return gcmTokenBatchQueue;
        case IOS:
        return apnsTokenBatchQueue;
        case SIMPLE_PUSH:
        return simplePushTokenBatchQueue;
        case WINDOWS_MPNS:
        return mpnsTokenBatchQueue;
        case WINDOWS_WNS:
        return wnsTokenBatchQueue;
        default:*/

      }
/*
        // TODO: Not sure the transformation should be done here...
        // There are likely better places to check if the metadata is way to long
        String jsonMessageContent = message.toStrippedJsonString() ;
        if (jsonMessageContent != null && jsonMessageContent.length() >= 4500) {
            jsonMessageContent = message.toMinimizedJsonString();
        }

        final PushMessageInformation pushMessageInformation =
                metricsService.storeNewRequestFrom(
                        pushApplication.getPushApplicationID(),
                        jsonMessageContent,
                        message.getIpAddress(),
                        message.getClientIdentifier(),
                        variants.getVariantCount()
                        );

        // we split the variants per type since each type may have its own configuration (e.g. batch size)
        variants.forEach((variantType, variant) -> {
            logger.info(String.format("Internal dispatching of push message for one %s variant (by %s)", variantType.getTypeName(), message.getClientIdentifier()));
            dispatchVariantMessageEvent.fire(new MessageHolderWithVariants(pushMessageInformation, message, variantType, variant));
        });*/
    }

    /**
     * Map for storing variants split by the variant type
     *
    private static class VariantMap extends EnumMap<VariantType, List<Variant>> {
        private static final long serialVersionUID = -1942168038908630961L;
        VariantMap() {
            super(VariantType.class);
        }
        void add(Variant variant) {
            List<Variant> list = this.get(variant.getType());
            if (list == null) {
                list = new ArrayList<>();
                this.put(variant.getType(), list);
            }
            list.add(variant);
        }
        void addAll(Collection<Variant> variants) {
            variants.forEach(this::add);
        }
        int getVariantCount() {
            int count = 0;
            for (Collection<Variant> variants : values()) {
                count += variants.size();
            }
            return count;
        }
    }
     */
//}//

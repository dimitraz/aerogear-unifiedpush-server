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
import net.wessendorf.kafka.serialization.GenericDeserializer;
import net.wessendorf.kafka.serialization.GenericSerializer;
import net.wessendorf.kafka.serialization.Serdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.jboss.aerogear.unifiedpush.api.PushApplication;
import org.jboss.aerogear.unifiedpush.api.PushMessageInformation;
import org.jboss.aerogear.unifiedpush.api.Variant;
import org.jboss.aerogear.unifiedpush.api.VariantType;
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

    private final String NOTIFCATION_ROUTER_INPUT_TOPIC = "kafka-streams";

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
     * @param pushApplication the push application
     * @param message the message
     */
    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public void submit() {
        //logger.debug("Processing send request with '{}' payload", message.getMessage());

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-twitter-streams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Initialize specific serializers to be used later
        final Serde<InternalUnifiedPushMessage> pushMessageSerde = CafdiSerdes.serdeFrom(InternalUnifiedPushMessage.class);
        final Serde<PushApplication> pushApplicationSerdeSerde = CafdiSerdes.serdeFrom(PushApplication.class);
        final Serde<Variant> variantSerde = CafdiSerdes.serdeFrom(Variant.class);

        KStreamBuilder builder = new KStreamBuilder();

        // Read from the source stream
        KStream<PushApplication, InternalUnifiedPushMessage> source = builder.stream(NOTIFCATION_ROUTER_INPUT_TOPIC);


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

                List<KeyValue<InternalUnifiedPushMessage, Variant>> result = new LinkedList<>();
                variants.forEach((variant) -> {
                    result.add(KeyValue.pair(message, variant));
                });

                return result;
            }
        );

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
     */
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
}

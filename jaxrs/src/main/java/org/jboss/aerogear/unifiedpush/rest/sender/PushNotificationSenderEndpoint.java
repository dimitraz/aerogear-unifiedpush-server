/**
 * JBoss, Home of Professional Open Source
 * Copyright Red Hat, Inc., and individual contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jboss.aerogear.unifiedpush.rest.sender;

import com.qmino.miredot.annotations.BodyType;
import com.qmino.miredot.annotations.ReturnType;
import net.wessendorf.kafka.SimpleKafkaProducer;
import net.wessendorf.kafka.cdi.annotation.Producer;
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
import org.jboss.aerogear.unifiedpush.message.InternalUnifiedPushMessage;
import org.jboss.aerogear.unifiedpush.message.NotificationRouter;
import org.jboss.aerogear.unifiedpush.rest.EmptyJSON;
import org.jboss.aerogear.unifiedpush.rest.util.HttpBasicHelper;
import org.jboss.aerogear.unifiedpush.rest.util.HttpRequestUtil;
import org.jboss.aerogear.unifiedpush.service.GenericVariantService;
import org.jboss.aerogear.unifiedpush.service.PushApplicationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

@Path("/sender")
public class PushNotificationSenderEndpoint {

    private final Logger logger = LoggerFactory.getLogger(PushNotificationSenderEndpoint.class);
    @Inject
    private PushApplicationService pushApplicationService;
    @Inject
    private NotificationRouter notificationRouter;

    @Inject
    private Instance<GenericVariantService> genericVariantService;

    @Producer
    SimpleKafkaProducer<InternalUnifiedPushMessage, InternalUnifiedPushMessage> producer;

    /**
     * RESTful API for sending Push Notifications.
     * The Endpoint is protected using <code>HTTP Basic</code> (credentials <code>PushApplicationID:masterSecret</code>).
     * <p>
     *
     * Messages are submitted as flexible JSON maps. Below is a simple example:
     * <pre>
     * curl -u "PushApplicationID:MasterSecret"
     *   -v -H "Accept: application/json" -H "Content-type: application/json"
     *   -X POST
     *   -d '{
     *     "message": {
     *      "alert": "HELLO!",
     *      "sound": "default",
     *      "user-data": {
     *          "key": "value",
     *      }
     *   }'
     *   https://SERVER:PORT/CONTEXT/rest/sender
     * </pre>
     *
     * Details about the Message Format can be found HERE!
     * <p>
     *
     * <b>Request Header</b> {@code aerogear-sender} uses to identify the used client. If the header is not present, the standard "user-agent" header is used.
     *
     * @param message   message to send
     * @param request the request
     * @return          empty JSON body
     *
     * @responseheader WWW-Authenticate Basic realm="AeroGear UnifiedPush Server" (only for 401 response)
     *
     * @statuscode 202 Indicates the Job has been accepted and is being process by the AeroGear UnifiedPush Server
     * @statuscode 401 The request requires authentication
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @BodyType("org.jboss.aerogear.unifiedpush.message.UnifiedPushMessage")
    @ReturnType("org.jboss.aerogear.unifiedpush.rest.EmptyJSON")
    public Response send(final InternalUnifiedPushMessage message, @Context HttpServletRequest request) {

        final PushApplication pushApplication = loadPushApplicationWhenAuthorized(request);
        if (pushApplication == null) {
            return Response.status(Status.UNAUTHORIZED)
                    .header("WWW-Authenticate", "Basic realm=\"AeroGear UnifiedPush Server\"")
                    .entity("Unauthorized Request")
                    .build();
        }

        // submit http request metadata:
        message.setIpAddress(HttpRequestUtil.extractIPAddress(request));

        // add the client identifier
        message.setClientIdentifier(HttpRequestUtil.extractAeroGearSenderInformation(request));

        // submitted to EJB:
        producer.send("my-topic3-test", message, message);
        logger.debug(String.format("Push Message Request from [%s] API was internally submitted for further processing", message.getClientIdentifier()));
        // submit();

        return Response.status(Status.ACCEPTED).entity(EmptyJSON.STRING).build();
    }

    public void submit() {
        // logger.info("Processing send request with '{}' payload", message.getMessage());

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-twitter-streams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "172.18.0.3:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, InternalPushMessageSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, InternalPushMessageSerde.class);

        // Initialize specific serializers to be used later
        final Serde<InternalUnifiedPushMessage> pushMessageSerde = CafdiSerdes.serdeFrom(InternalUnifiedPushMessage.class);
        final Serde<PushApplication> pushApplicationSerdeSerde = CafdiSerdes.serdeFrom(PushApplication.class);
        final Serde<Variant> variantSerde = CafdiSerdes.serdeFrom(Variant.class);
        final Serde<String> stringSerde = CafdiSerdes.serdeFrom(String.class);

        KStreamBuilder builder = new KStreamBuilder();

        // Read from the source stream
        KStream<InternalUnifiedPushMessage, InternalUnifiedPushMessage> source = builder.stream("my-topic2-test");

        // Get variants for each message
        /*KStream<InternalUnifiedPushMessage, Variant> getVariants = source.flatMap(
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
                    }

                    logger.warn("got variants.." + variants.toString());

                    List<KeyValue<InternalUnifiedPushMessage, Variant>> result = new LinkedList<>();
                    variants.forEach((variant) -> {
                        result.add(KeyValue.pair(message, variant));
                    });

                    logger.warn("added key value pair to results list..");
                    return result;
                }
        );*/

        logger.warn("transformeing");

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
        );

        logger.warn("transformed");

       // KStream<InternalUnifiedPushMessage, Variant> unmodifiedStream = getVariants.peek((key, value) -> logger.warn("key=" + key + ", value=" + value));

        transformed.to(pushMessageSerde, stringSerde, "my-utopic");

        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();
    }

    /**
     * returns application if the masterSecret is valid for the request PushApplicationEntity
     */
    private PushApplication loadPushApplicationWhenAuthorized(HttpServletRequest request) {
        // extract the pushApplicationID and its secret from the HTTP Basic header:
        String[] credentials = HttpBasicHelper.extractUsernameAndPasswordFromBasicHeader(request);
        String pushApplicationID = credentials[0];
        String secret = credentials[1];

        final PushApplication pushApplication = pushApplicationService.findByPushApplicationID(pushApplicationID);
        if (pushApplication != null && pushApplication.getMasterSecret().equals(secret)) {
            return pushApplication;
        }

        // unauthorized...
        return null;
    }
}


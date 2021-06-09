package org.mskcc.cmo.metadb.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.nats.client.Message;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mskcc.cmo.messaging.Gateway;
import org.mskcc.cmo.messaging.MessageConsumer;
import org.mskcc.cmo.metadb.model.SampleMetadata;
import org.mskcc.cmo.metadb.model.web.PublishedMetaDbRequest;
import org.mskcc.cmo.metadb.service.CmoLabelGeneratorService;
import org.mskcc.cmo.metadb.service.MessageHandlingService;
import org.mskcc.cmo.metadb.service.util.RequestStatusLogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 *
 * @author ochoaa
 */
@Service
public class MessageHandlingServiceImpl implements MessageHandlingService {

    @Value("${igo.cmo_label_generator_topic}")
    private String CMO_LABEL_GENERATOR_TOPIC;

    @Value("${igo.new_request_topic}")
    private String IGO_NEW_REQUEST_TOPIC;

    @Value("${num.new_request_handler_threads}")
    private int NUM_NEW_REQUEST_HANDLERS;

    @Value("${igo.cmo_request_filter:false}")
    private Boolean igoCmoRequestFilter;

    @Autowired
    private CmoLabelGeneratorService cmoLabelGeneratorService;

    @Autowired
    private RequestStatusLogger requestStatusLogger;

    private final ObjectMapper mapper = new ObjectMapper();
    private static boolean initialized = false;
    private static volatile boolean shutdownInitiated;
    private static final ExecutorService exec = Executors.newCachedThreadPool();
    private static final BlockingQueue<PublishedMetaDbRequest> cmoLabelGeneratorQueue =
        new LinkedBlockingQueue<PublishedMetaDbRequest>();
    private static CountDownLatch cmoLableGeneratorHandlerShutdownLatch;
    private static Gateway messagingGateway;

    private static final Log LOG = LogFactory.getLog(MessageHandlingServiceImpl.class);

    private class CmoLabelGeneratorHandler implements Runnable {

        final Phaser phaser;
        boolean interrupted = false;

        CmoLabelGeneratorHandler(Phaser phaser) {
            this.phaser = phaser;
        }

        @Override
        public void run() {
            phaser.arrive();
            while (true) {
                try {
                    PublishedMetaDbRequest request = cmoLabelGeneratorQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (request != null) {
                        // skip request if filtering by only cmo requests and cmoRequest status is false
                        if (igoCmoRequestFilter && !request.isCmoRequest()) {
                            requestStatusLogger.logRequestStatus(request.getRequestJson(),
                                    RequestStatusLogger.StatusType.CMO_REQUEST_FILTER_SKIPPED_REQUEST);
                            continue;
                        }
                        // skip request if there are no samples to generate cmo labels for
                        if (request.getSamples().isEmpty()) {
                            requestStatusLogger.logRequestStatus(request.getRequestJson(),
                                    RequestStatusLogger.StatusType.REQUEST_WITH_MISSING_SAMPLES);
                            continue;
                        }
                        // generate cmo labels for each sample metadata
                        List<SampleMetadata> updatedSamples = new ArrayList<>();
                        for (SampleMetadata sample : request.getSamples()) {
                            // generate cmo label
                            String sampleCmoLabel = cmoLabelGeneratorService.generateCmoSampleLabel(sample);
                            sample.setCmoSampleName(sampleCmoLabel);
                            updatedSamples.add(sample);
                        }
                        // save the updated samples to the request and publish to the igo new request topic
                        request.setSamples(updatedSamples);
                        // publish to igo new request topic
                        messagingGateway.publish(request.getRequestId(),
                                    IGO_NEW_REQUEST_TOPIC,
                                    mapper.writeValueAsString(request));
                    }
                    if (interrupted && cmoLabelGeneratorQueue.isEmpty()) {
                        break;
                    }
                } catch (InterruptedException e) {
                    interrupted = true;
                } catch (Exception e) {
                    LOG.error("Error during request handling", e);
                }
            }
            cmoLableGeneratorHandlerShutdownLatch.countDown();
        }
    }

    @Override
    public void initialize(Gateway gateway) throws Exception {
        if (!initialized) {
            messagingGateway = gateway;
            setupCmoLabelGeneratorHandler(messagingGateway, this);
            initializeCmoLabelGeneratorHandlers();
            initialized = true;
        } else {
            LOG.error("Messaging Handler Service has already been initialized, ignoring request.\n");
        }
    }

    @Override
    public void cmoLabelGeneratorHandler(PublishedMetaDbRequest request) throws Exception {
        if (!initialized) {
            throw new IllegalStateException("Message Handling Service has not been initialized");
        }
        if (!shutdownInitiated) {
            cmoLabelGeneratorQueue.put(request);
        } else {
            LOG.error("Shutdown initiated, not accepting request: " + request);
            throw new IllegalStateException("Shutdown initiated, not handling any more requests");
        }
    }

    @Override
    public void shutdown() throws Exception {
        if (!initialized) {
            throw new IllegalStateException("Message Handling Service has not been initialized");
        }
        exec.shutdownNow();
        cmoLableGeneratorHandlerShutdownLatch.await();
        shutdownInitiated = true;
    }

    private void initializeCmoLabelGeneratorHandlers() throws Exception {
        cmoLableGeneratorHandlerShutdownLatch = new CountDownLatch(NUM_NEW_REQUEST_HANDLERS);
        final Phaser cmoLabelGeneratorPhasser = new Phaser();
        cmoLabelGeneratorPhasser.register();
        for (int lc = 0; lc < NUM_NEW_REQUEST_HANDLERS; lc++) {
            cmoLabelGeneratorPhasser.register();
            exec.execute(new CmoLabelGeneratorHandler(cmoLabelGeneratorPhasser));
        }
        cmoLabelGeneratorPhasser.arriveAndAwaitAdvance();
    }

    private void setupCmoLabelGeneratorHandler(Gateway gateway, MessageHandlingService messageHandlingService)
        throws Exception {
        gateway.subscribe(CMO_LABEL_GENERATOR_TOPIC, Object.class, new MessageConsumer() {
            public void onMessage(Message msg, Object message) {
                LOG.info("Received message on topic: " + CMO_LABEL_GENERATOR_TOPIC);
                try {
                    String requestJson = mapper.readValue(
                            new String(msg.getData(), StandardCharsets.UTF_8),
                            String.class);
                    PublishedMetaDbRequest request = mapper.readValue(requestJson,
                            PublishedMetaDbRequest.class);
                    request.setRequestJson(requestJson);
                    messageHandlingService.cmoLabelGeneratorHandler(request);
                } catch (Exception e) {
                    LOG.error("Exception during processing of request on topic: "
                            + CMO_LABEL_GENERATOR_TOPIC, e);
                    try {
                        requestStatusLogger.logRequestStatus(message.toString(),
                                RequestStatusLogger.StatusType.REQUEST_PARSING_ERROR);
                    } catch (IOException ex) {
                        LOG.error("Error during attempt to write request status to logger file", ex);
                    }
                }
            }
        });
    }
}

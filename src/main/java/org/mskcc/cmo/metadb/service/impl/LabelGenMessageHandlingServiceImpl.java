package org.mskcc.cmo.metadb.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.nats.client.Message;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.mskcc.cmo.metadb.model.igo.IgoSampleManifest;
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
public class LabelGenMessageHandlingServiceImpl implements MessageHandlingService {

    @Value("${igo.cmo_label_generator_topic}")
    private String CMO_LABEL_GENERATOR_TOPIC;

    @Value("${igo.new_request_topic}")
    private String IGO_NEW_REQUEST_TOPIC;

    @Value("${igo.cmo_sample_label_update_topic}")
    private String CMO_LABEL_UPDATE_TOPIC;

    @Value("${metadb.sample_update_topic}")
    private String IGO_SAMPLE_UPDATE_TOPIC;

    @Value("${num.new_request_handler_threads}")
    private int NUM_NEW_REQUEST_HANDLERS;

    @Value("${request_reply.patient_samples_topic}")
    private String PATIENT_SAMPLES_REQUEST_TOPIC;

    @Autowired
    private CmoLabelGeneratorService cmoLabelGeneratorService;

    @Autowired
    private RequestStatusLogger requestStatusLogger;

    private final ObjectMapper mapper = new ObjectMapper();
    private static boolean initialized = false;
    private static volatile boolean shutdownInitiated;
    private static final ExecutorService exec = Executors.newCachedThreadPool();
    private static final BlockingQueue<String> cmoLabelGeneratorQueue =
        new LinkedBlockingQueue<String>();
    private static final BlockingQueue<String> igoNewRequestPublisherQueue =
            new LinkedBlockingQueue<String>();
    private static final BlockingQueue<SampleMetadata> cmoSampleLabelUpdateQueue =
            new LinkedBlockingQueue<SampleMetadata>();
    private static CountDownLatch cmoLabelGeneratorShutdownLatch;
    private static CountDownLatch newRequestPublisherShutdownLatch;
    private static CountDownLatch cmoSampleLabelUpdateShutdownLatch;
    private static Gateway messagingGateway;

    private static final Log LOG = LogFactory.getLog(LabelGenMessageHandlingServiceImpl.class);

    /**
     * Message handler for publishing requests to IGO_NEW_REQUEST topic.
     */
    private class IgoNewRequestPublisherHandler implements Runnable {
        final Phaser phaser;
        boolean interrupted = false;

        /**
         * IgoNewRequestPublisherHandler constructor.
         * @param phaser
         */
        IgoNewRequestPublisherHandler(Phaser phaser) {
            this.phaser = phaser;
        }

        @Override
        public void run() {
            phaser.arrive();
            while (true) {
                try {
                    String requestJson = igoNewRequestPublisherQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (requestJson != null) {
                        try {
                            // publish to igo new request topic
                            LOG.info("Publishing request to: " + IGO_NEW_REQUEST_TOPIC);
                            messagingGateway.publish(getRequestIdFromRequestJson(requestJson),
                                        IGO_NEW_REQUEST_TOPIC,
                                        requestJson);
                        } catch (Exception e) {
                            LOG.error("Error occurred during attempt to publish on topic: "
                                    + IGO_NEW_REQUEST_TOPIC, e);
                        }
                    }
                    if (interrupted && igoNewRequestPublisherQueue.isEmpty()) {
                        break;
                    }
                } catch (InterruptedException e) {
                    interrupted = true;
                } catch (Exception e) {
                    LOG.error("Error during handling of igo new request publishing queue", e);
                }
            }
            newRequestPublisherShutdownLatch.countDown();
        }
    }

    /**
     * Message handler for generating CMO labels for samples in requests.
     * Updates the request json contents with the generated label sample metadata
     * and adds message to a separate queue which will publish to IGO_NEW_REQUEST.
     */
    private class CmoLabelGeneratorHandler implements Runnable {

        final Phaser phaser;
        boolean interrupted = false;

        /**
         * CmoLabelGeneratorHandler constructor.
         * @param phaser
         */
        CmoLabelGeneratorHandler(Phaser phaser) {
            this.phaser = phaser;
        }

        @Override
        public void run() {
            phaser.arrive();
            while (true) {
                try {
                    String requestJson = cmoLabelGeneratorQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (requestJson != null) {
                        LOG.info("Extracting samples from request received...");
                        String requestId = getRequestIdFromRequestJson(requestJson);
                        List<IgoSampleManifest> samples = getSamplesFromRequestJson(requestJson);

                        // get existing samples for all patients in the request
                        Map<String, List<SampleMetadata>> patientSamplesMap = getPatientSamplesMap(samples);

                        // udpated samples list will store samples which had a label generated successfully
                        List<Object> updatedSamples = new ArrayList<>();
                        for (IgoSampleManifest sample : samples) {
                            // get existing patient samples for cmo patient id
                            List<SampleMetadata> existingSamples =
                                    patientSamplesMap.getOrDefault(sample.getCmoPatientId(),
                                            new ArrayList<>());
                            // TODO resolve any issues that arise with errors in generating cmo label
                            String sampleCmoLabel = cmoLabelGeneratorService.generateCmoSampleLabel(
                                    requestId, sample, existingSamples);

                            // check if matching sample found and determine if label actually needs updating
                            // or if we can use the same label that is already persisted for this sample
                            // note that we want to continue publishing to the IGO_SAMPLE_UPDATE_TOPIC since
                            // there might be other metadata changes that need to be persisted that may not
                            // necessarily affect the cmo label generated
                            SampleMetadata matchingSample = null;
                            for (SampleMetadata s : existingSamples) {
                                if (s.getPrimaryId().equalsIgnoreCase(sample.getIgoId())) {
                                    matchingSample = s;
                                    LOG.info("Sample with matching IGO ID found in existing samples - "
                                            + "comparing metadata used for generating cmo label");
                                    break;
                                }
                            }

                            if (matchingSample != null
                                    && !cmoLabelGeneratorService.igoSampleRequiresLabelUpdate(
                                            sampleCmoLabel, matchingSample.getCmoSampleName())) {
                                LOG.info("No change detected for CMO sample label metadata - using "
                                        + "existing CMO label for matching IGO sample from database.");
                                sample.setCmoSampleName(matchingSample.getCmoSampleName());
                            } else {
                                LOG.info("Changes detected in CMO sample label metadata - "
                                        + "updating sample CMO label to newly generated label.");
                                sample.setCmoSampleName(sampleCmoLabel);
                            }

                            // update patient sample map and list of updated samples for request
                            updatedSamples.add(sample);
                            patientSamplesMap.put(sample.getCmoPatientId(),
                                    updatePatientSampleList(existingSamples, sample));
                        }

                        // if sizes of the input samples and updated samples are different
                        // then that indicates that some labels were not generated successfully
                        // for the current request
                        if (samples.size() != updatedSamples.size()) {
                            LOG.error("Input sample size does not match the number of samples for which "
                                    + "a CMO label was successfully generated - logging request status");
                            requestStatusLogger.logRequestStatus(requestJson,
                                    RequestStatusLogger.StatusType.REQUEST_WITH_FAILED_CMO_LABEL_GENERATION);
                        }

                        // update contents of 'samples' in request json map to publish
                        // and add updated request json to publisher queue
                        Map<String, Object> requestJsonMap = mapper.readValue(requestJson, Map.class);
                        requestJsonMap.put("samples",
                                updatedSamples);
                        igoNewRequestPublisherQueue.add(mapper.writeValueAsString(requestJsonMap));
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
            cmoLabelGeneratorShutdownLatch.countDown();
        }
    }

    /**
     * Message handler for generating new CMO labels for a given sample.
     * Updates the sample metadata contents with the new generated label
     * and publishes sample metadata to CMO_LABEL_UPDATE_TOPIC.
     */
    private class CmoSampleLabelUpdateHandler implements Runnable {

        final Phaser phaser;
        boolean interrupted = false;

        /**
         * CmoSampleLabelUpdateHandler constructor.
         * @param phaser
         */
        CmoSampleLabelUpdateHandler(Phaser phaser) {
            this.phaser = phaser;
        }

        @Override
        public void run() {
            phaser.arrive();
            while (true) {
                try {
                    SampleMetadata sample = cmoSampleLabelUpdateQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (sample != null) {
                        List<SampleMetadata> existingSamples =
                                getExistingPatientSamples(sample.getCmoPatientId());

                        // generate new cmo sample label and update sample metadata object
                        String updatedCmoSampleLabel =
                                cmoLabelGeneratorService.generateCmoSampleLabel(sample, existingSamples);

                        // check if matching sample found and determine if label actually needs updating
                        // or if we can use the same label that is already persisted for this sample
                        // note that we want to continue publishing to the IGO_SAMPLE_UPDATE_TOPIC since
                        // there might be other metadata changes that need to be persisted that may not
                        // necessarily affect the cmo label generated
                        SampleMetadata matchingSample = null;
                        for (SampleMetadata s : existingSamples) {
                            if (s.getPrimaryId().equalsIgnoreCase(sample.getPrimaryId())) {
                                matchingSample = s;
                                break;
                            }
                        }

                        if (matchingSample != null && !cmoLabelGeneratorService.igoSampleRequiresLabelUpdate(
                                        updatedCmoSampleLabel, matchingSample.getCmoSampleName())) {
                            LOG.info("No change detected for CMO sample label metadata - using "
                                    + "existing CMO label for matching IGO sample from database.");
                            sample.setCmoSampleName(matchingSample.getCmoSampleName());
                        } else {
                            LOG.info("Changes detected in CMO sample label metadata - "
                                    + "updating sample CMO label to newly generated label.");
                            sample.setCmoSampleName(updatedCmoSampleLabel);
                        }
                        LOG.info("Publishing sample to IGO_SAMPLE_UPDATE_TOPIC");
                        messagingGateway.publish(IGO_SAMPLE_UPDATE_TOPIC, mapper.writeValueAsString(sample));
                    }
                    if (interrupted && cmoSampleLabelUpdateQueue.isEmpty()) {
                        break;
                    }
                } catch (InterruptedException e) {
                    interrupted = true;
                } catch (Exception e) {
                    LOG.error("Error during request handling", e);
                }
            }
            cmoSampleLabelUpdateShutdownLatch.countDown();
        }
    }

    private List<SampleMetadata> updatePatientSampleList(List<SampleMetadata> existingSamples,
            IgoSampleManifest sample) throws JsonProcessingException {
        Boolean foundMatching = Boolean.FALSE;
        // if sample already exists in the existing samples list then simply replace at the matching index
        for (SampleMetadata existing : existingSamples) {
            if (existing.getPrimaryId().equalsIgnoreCase(sample.getIgoId())) {
                existingSamples.set(existingSamples.indexOf(existing), new SampleMetadata(sample));
                foundMatching = Boolean.TRUE;
                break;
            }
        }
        // if matching sample not found then append to list and return
        if (!foundMatching) {
            existingSamples.add(new SampleMetadata(sample));
        }
        return existingSamples;
    }

    private Map<String, List<SampleMetadata>> getPatientSamplesMap(List<IgoSampleManifest> samples)
            throws Exception {
        Map<String, List<SampleMetadata>> patientSamplesMap = new HashMap<>();
        for (IgoSampleManifest sample : samples) {
            // get or request existing patient samples and update patient sample mapping
            if (!patientSamplesMap.containsKey(sample.getCmoPatientId())) {
                List<SampleMetadata> ptSamples = getExistingPatientSamples(sample.getCmoPatientId());
                patientSamplesMap.put(sample.getCmoPatientId(),
                        new ArrayList<>(ptSamples));
            }
        }
        return patientSamplesMap;
    }

    private List<SampleMetadata> getExistingPatientSamples(String cmoPatientId) throws Exception {
        Message reply = messagingGateway.request(PATIENT_SAMPLES_REQUEST_TOPIC,
                    cmoPatientId);
        SampleMetadata[] ptSamples = mapper.readValue(
                new String(reply.getData(), StandardCharsets.UTF_8),
                SampleMetadata[].class);
        return new ArrayList<>(Arrays.asList(ptSamples));
    }

    private String getRequestIdFromRequestJson(String requestJson) throws JsonProcessingException {
        Map<String, Object> requestJsonMap = mapper.readValue(requestJson, Map.class);
        return requestJsonMap.get("requestId").toString();
    }

    private List<IgoSampleManifest> getSamplesFromRequestJson(String requestJson)
            throws JsonProcessingException {
        Map<String, Object> requestJsonMap = mapper.readValue(requestJson, Map.class);
        List<IgoSampleManifest> sampleManifests =
                Arrays.asList(mapper.convertValue(requestJsonMap.get("samples"),
                IgoSampleManifest[].class));
        return sampleManifests;
    }

    @Override
    public void initialize(Gateway gateway) throws Exception {
        if (!initialized) {
            messagingGateway = gateway;
            setupCmoLabelGeneratorHandler(messagingGateway, this);
            setupCmoSampleLabelUpdateHandler(messagingGateway, this);
            initializeMessageHandlers();
            initialized = true;
        } else {
            LOG.error("Messaging Handler Service has already been initialized, ignoring request.\n");
        }
    }

    @Override
    public void cmoLabelGeneratorHandler(String requestJson) throws Exception {
        if (!initialized) {
            throw new IllegalStateException("Message Handling Service has not been initialized");
        }
        if (!shutdownInitiated) {
            cmoLabelGeneratorQueue.put(requestJson);
        } else {
            LOG.error("Shutdown initiated, not accepting request: " + requestJson);
            throw new IllegalStateException("Shutdown initiated, not handling any more requests");
        }
    }

    @Override
    public void cmoSampleLabelUpdateHandler(SampleMetadata sampleMetadata) throws Exception {
        if (!initialized) {
            throw new IllegalStateException("Message Handling Service has not been initialized");
        }
        if (!shutdownInitiated) {
            cmoSampleLabelUpdateQueue.put(sampleMetadata);
        } else {
            LOG.error("Shutdown initiated, not accepting update for IGO sample: "
                    + sampleMetadata.getPrimaryId());
            throw new IllegalStateException("Shutdown initiated, not handling any more requests");
        }
    }

    @Override
    public void shutdown() throws Exception {
        if (!initialized) {
            throw new IllegalStateException("Message Handling Service has not been initialized");
        }
        exec.shutdownNow();
        cmoLabelGeneratorShutdownLatch.await();
        newRequestPublisherShutdownLatch.await();
        shutdownInitiated = true;
    }

    private void initializeMessageHandlers() throws Exception {
        cmoLabelGeneratorShutdownLatch = new CountDownLatch(NUM_NEW_REQUEST_HANDLERS);
        final Phaser cmoLabelGeneratorPhaser = new Phaser();
        cmoLabelGeneratorPhaser.register();
        for (int lc = 0; lc < NUM_NEW_REQUEST_HANDLERS; lc++) {
            cmoLabelGeneratorPhaser.register();
            exec.execute(new CmoLabelGeneratorHandler(cmoLabelGeneratorPhaser));
        }
        cmoLabelGeneratorPhaser.arriveAndAwaitAdvance();

        newRequestPublisherShutdownLatch = new CountDownLatch(NUM_NEW_REQUEST_HANDLERS);
        final Phaser newRequestPhaser = new Phaser();
        newRequestPhaser.register();
        for (int lc = 0; lc < NUM_NEW_REQUEST_HANDLERS; lc++) {
            newRequestPhaser.register();
            exec.execute(new IgoNewRequestPublisherHandler(newRequestPhaser));
        }
        newRequestPhaser.arriveAndAwaitAdvance();

        cmoSampleLabelUpdateShutdownLatch = new CountDownLatch(NUM_NEW_REQUEST_HANDLERS);
        final Phaser cmoSampleLabelUpdatePhaser = new Phaser();
        cmoSampleLabelUpdatePhaser.register();
        for (int lc = 0; lc < NUM_NEW_REQUEST_HANDLERS; lc++) {
            cmoSampleLabelUpdatePhaser.register();
            exec.execute(new CmoSampleLabelUpdateHandler(cmoSampleLabelUpdatePhaser));
        }
        cmoSampleLabelUpdatePhaser.arriveAndAwaitAdvance();
    }

    private void setupCmoLabelGeneratorHandler(Gateway gateway,
            MessageHandlingService messageHandlingService) throws Exception {
        gateway.subscribe(CMO_LABEL_GENERATOR_TOPIC, Object.class, new MessageConsumer() {
            public void onMessage(Message msg, Object message) {
                LOG.info("Received message on topic: " + CMO_LABEL_GENERATOR_TOPIC);
                try {
                    messageHandlingService.cmoLabelGeneratorHandler(
                            mapper.readValue(new String(msg.getData(), StandardCharsets.UTF_8),
                                    String.class));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    private void setupCmoSampleLabelUpdateHandler(Gateway gateway,
            MessageHandlingService messageHandlingService) throws Exception {
        gateway.subscribe(CMO_LABEL_UPDATE_TOPIC, Object.class, new MessageConsumer() {
            public void onMessage(Message msg, Object message) {
                LOG.info("Received message on topic: " + CMO_LABEL_UPDATE_TOPIC);
                try {
                    String sampleMetadataJson = mapper.readValue(
                            new String(msg.getData(), StandardCharsets.UTF_8),
                            String.class);
                    SampleMetadata sampleMetadata =
                            mapper.readValue(sampleMetadataJson, SampleMetadata.class);
                    messageHandlingService.cmoSampleLabelUpdateHandler(sampleMetadata);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }
}

package org.mskcc.smile.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
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
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mskcc.cmo.messaging.Gateway;
import org.mskcc.cmo.messaging.MessageConsumer;
import org.mskcc.smile.model.SampleMetadata;
import org.mskcc.smile.model.Status;
import org.mskcc.smile.model.igo.IgoSampleManifest;
import org.mskcc.smile.service.CmoLabelGeneratorService;
import org.mskcc.smile.service.MessageHandlingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 *
 * @author ochoaa
 */
@Service
public class LabelGenMessageHandlingServiceImpl implements MessageHandlingService {

    @Value("${igo.cmo_label_generator_topic:}")
    private String CMO_LABEL_GENERATOR_TOPIC;

    @Value("${igo.cmo_promoted_label_topic:}")
    private String CMO_PROMOTED_LABEL_TOPIC;

    @Value("${igo.new_request_topic:}")
    private String IGO_NEW_REQUEST_TOPIC;

    @Value("${igo.promoted_request_topic:}")
    private String IGO_PROMOTED_REQUEST_TOPIC;

    @Value("${igo.cmo_sample_label_update_topic:}")
    private String CMO_LABEL_UPDATE_TOPIC;

    @Value("${smile.sample_update_topic:}")
    private String IGO_SAMPLE_UPDATE_TOPIC;

    @Value("${num.new_request_handler_threads:1}")
    private int NUM_NEW_REQUEST_HANDLERS;

    @Value("${num.promoted_request_handler_threads:1}")
    private int NUM_PROMOTED_REQUEST_HANDLERS;

    @Value("${request_reply.patient_samples_topic:}")
    private String PATIENT_SAMPLES_REQUEST_TOPIC;

    @Value("${request_reply.samples_by_cmo_label_topic}")
    private String SAMPLES_BY_CMO_LABEL_REQREPLY_TOPIC;

    @Value("${request_reply.samples_by_alt_id_topic}")
    private String SAMPLES_BY_ALT_ID_REQREPLY_TOPIC;

    @Autowired
    private CmoLabelGeneratorService cmoLabelGeneratorService;

    private final ObjectMapper mapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private static boolean initialized = false;
    private static volatile boolean shutdownInitiated;
    private static final ExecutorService exec = Executors.newCachedThreadPool();

    private static final BlockingQueue<String> cmoLabelGeneratorQueue =
        new LinkedBlockingQueue<String>();
    private static final BlockingQueue<String> cmoPromotedLabelQueue =
        new LinkedBlockingQueue<String>();
    private static final BlockingQueue<String> igoNewRequestQueue =
        new LinkedBlockingQueue<String>();
    private static final BlockingQueue<String> igoPromotedRequestQueue =
        new LinkedBlockingQueue<String>();
    private static final BlockingQueue<SampleMetadata> cmoSampleLabelUpdateQueue =
        new LinkedBlockingQueue<SampleMetadata>();

    private static CountDownLatch cmoLabelGeneratorShutdownLatch;
    private static CountDownLatch cmoPromotedLabelShutdownLatch;
    private static CountDownLatch newRequestPublisherShutdownLatch;
    private static CountDownLatch promotedRequestPublisherShutdownLatch;
    private static CountDownLatch cmoSampleLabelUpdateShutdownLatch;
    private static Gateway messagingGateway;

    private static final Log LOG = LogFactory.getLog(LabelGenMessageHandlingServiceImpl.class);

    public static enum IgoRequestDest {
        NEW_REQUEST_DEST,
        PROMOTED_REQUEST_DEST
    }

    /**
     * Message handler for new requests and promoted requests.
     */
    private class IgoRequestHandler implements Runnable {
        final Phaser phaser;
        final IgoRequestDest igoRequestDest;
        final BlockingQueue<String> igoRequestQueue;
        final CountDownLatch shutdownLatch;
        boolean interrupted = false;

        /**
         * IgoRequestPublisherHandler constructor.
         */
        IgoRequestHandler(Phaser phaser, IgoRequestDest igoRequestDest,
                BlockingQueue<String> igoRequestQueue, CountDownLatch shutdownLatch) {
            this.phaser = phaser;
            this.igoRequestDest = igoRequestDest;
            this.igoRequestQueue = igoRequestQueue;
            this.shutdownLatch = shutdownLatch;
        }

        @Override
        public void run() {
            phaser.arrive();
            while (true) {
                try {
                    String requestJson = igoRequestQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (requestJson != null) {
                        try {
                            switch (igoRequestDest) {
                                case NEW_REQUEST_DEST:
                                    LOG.info("Publishing request to: " + IGO_NEW_REQUEST_TOPIC);
                                    messagingGateway.publish(getRequestIdFromRequestJson(requestJson),
                                            IGO_NEW_REQUEST_TOPIC,
                                            requestJson);
                                    break;
                                case PROMOTED_REQUEST_DEST:
                                    LOG.info("Publishing request to: " + IGO_PROMOTED_REQUEST_TOPIC);
                                    messagingGateway.publish(getRequestIdFromRequestJson(requestJson),
                                            IGO_PROMOTED_REQUEST_TOPIC,
                                            requestJson);
                                    break;
                                default:
                                    break;
                            }
                        } catch (Exception e) {
                            LOG.error("Error occurred during attempt to publish request "
                                    + "to destination topic: TOPIC=" + igoRequestDest
                                    + ", JSON=" + requestJson, e);
                        }
                    }
                    if (interrupted && igoRequestQueue.isEmpty()) {
                        break;
                    }
                } catch (InterruptedException e) {
                    interrupted = true;
                } catch (Exception e) {
                    LOG.error("Error during handling of igo request publishing queue", e);
                }
            }
            shutdownLatch.countDown();
        }
    }

    /**
     * Message handler for generating CMO labels for samples in requests.
     * Updates the request json contents with the generated label sample metadata
     * and adds message to a separate queue which will publish to IGO_NEW_REQUEST or
     * IGO_PROMOTED_REQUEST.
     */
    private class CmoLabelGeneratorHandler implements Runnable {

        final Phaser phaser;
        final IgoRequestDest igoRequestDest;
        final BlockingQueue<String> labelGeneratorQueue;
        final CountDownLatch shutdownLatch;
        boolean interrupted = false;

        /**
         * CmoLabelGeneratorHandler constructor.
         */
        CmoLabelGeneratorHandler(Phaser phaser, IgoRequestDest igoRequestDest,
                BlockingQueue<String> labelGeneratorQueue, CountDownLatch shutdownLatch) {
            this.phaser = phaser;
            this.igoRequestDest = igoRequestDest;
            this.labelGeneratorQueue = labelGeneratorQueue;
            this.shutdownLatch = shutdownLatch;
        }

        @Override
        public void run() {
            phaser.arrive();
            while (true) {
                try {
                    String requestJson = labelGeneratorQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (requestJson != null) {
                        String requestId = getRequestIdFromRequestJson(requestJson);
                        LOG.info("Extracting samples from request received: " + requestId);
                        List<Object> samples = getSamplesFromRequestJson(requestJson);

                        // get existing samples for all patients in the request
                        Map<String, List<SampleMetadata>> patientSamplesMap = getPatientSamplesMap(samples);

                        // udpated samples list will store samples which had a label generated successfully
                        List<Object> updatedSamples = new ArrayList<>();
                        for (Object sample : samples) {
                            Map<String, Object> sampleMap = mapper.convertValue(sample, Map.class);
                            if (StringUtils.isBlank(sampleMap.get("cmoPatientId").toString())) {
                                // skip over samples with missing cmo patient id this should be
                                // getting caught by the request filter but we are taking extra precautions
                                // due to ongoing timeout exception investigations
                                LOG.warn("Sample is missing CMO patient ID that was not caught by the "
                                        + "request filter: " + mapper.writeValueAsString(sampleMap));
                                continue;
                            }
                            Map<String, Object> sampleStatusMap = mapper.convertValue(
                                    sampleMap.get("status"), Map.class);
                            Status sampleStatus = new Status(Boolean.valueOf(
                                    sampleStatusMap.get("validationStatus").toString()),
                                    sampleStatusMap.get("validationReport").toString());
                            IgoSampleManifest sampleManifest = mapper.convertValue(sample,
                                    IgoSampleManifest.class);

                            if (sampleStatus.getValidationStatus()) {
                                // get existing patient samples for cmo patient id
                                List<SampleMetadata> existingSamples =
                                        patientSamplesMap.getOrDefault(sampleManifest.getCmoPatientId(),
                                                new ArrayList<>());
                                List<SampleMetadata> samplesByAltId
                                        = getSamplesByAltId(sampleManifest.getAltid());

                                // TODO resolve any issues that arise with errors in generating cmo label
                                String newSampleCmoLabel = cmoLabelGeneratorService.generateCmoSampleLabel(
                                        requestId, sampleManifest, existingSamples, samplesByAltId);
                                if (newSampleCmoLabel == null) {
                                    sampleStatus = cmoLabelGeneratorService.generateSampleStatus(
                                            requestId, sampleManifest, existingSamples, samplesByAltId);
                                    LOG.error("Unable to generate new CMO sample label for sample: "
                                            + sampleManifest.getIgoId());
                                    // check if we can fall back on an existing cmo label that might have
                                    // come in with the incoming request json
                                    if (!StringUtils.isBlank(sampleManifest.getCmoSampleName())) {
                                        LOG.info("Could not generate new CMO sample label for sample: "
                                                + sampleManifest.getIgoId()
                                                + ". Falling back on incoming CMO sample label for sample: "
                                                + sampleManifest.getCmoSampleName());
                                        sampleMap.put("cmoSampleName", sampleManifest.getCmoSampleName());
                                    }
                                } else {
                                    // check if matching sample found and determine if label actually needs
                                    // updating or if we can use the same label that
                                    // is already persisted for this sample
                                    // note that we want to continue publishing to the IGO_SAMPLE_UPDATE_TOPIC
                                    // since there might be other metadata changes that need to be persisted
                                    // that may not necessarily affect the cmo label generated
                                    String resolvedCmoSampleLabel = resolveAndUpdateCmoSampleLabel(
                                            sampleManifest.getIgoId(), existingSamples, newSampleCmoLabel);
                                    if (!StringUtils.isBlank(sampleManifest.getCmoSampleName())) {
                                        // if incomfing sample has an existing cmo label then check
                                        // if there are any meaningful changes to the metadata that
                                        // affects the sample type abbreviation or nucleic acid abbreviation
                                        Boolean hasMeaningfulUpdate =
                                                cmoLabelGeneratorService.igoSampleRequiresLabelUpdate(
                                                        newSampleCmoLabel,
                                                        sampleManifest.getCmoSampleName());
                                        if (hasMeaningfulUpdate) {
                                            LOG.warn("Incoming sample manifest has existing CMO label but "
                                                    + "the label generator indicates that a change to the "
                                                    + "metadata or updates to the label generation rules has "
                                                    + "resulted in a new sample type abbreviation or nucleic "
                                                    + "acid abbreviation: primary id="
                                                    + sampleManifest.getIgoId()
                                                    + ", incoming CMO sample label (not using)="
                                                    + sampleManifest.getCmoSampleName()
                                                    + ", newly generated CMO label (using)="
                                                    + newSampleCmoLabel);
                                        } else {
                                            // before settling on using the provided cmo label from the
                                            // incoming sample check if that label already exists in smile
                                            // for another sample
                                            if (isCmoLabelAlreadyInUse(sampleManifest.getIgoId(),
                                                    sampleManifest.getCmoSampleName())) {

                                                String nextAvailableLabel = findNextAvailableCmoLabel(
                                                        sampleManifest.getIgoId(),
                                                        sampleManifest.getCmoSampleName());

                                                if (nextAvailableLabel == null) {
                                                    LOG.info("Incoming sample: " + sampleManifest.getIgoId()
                                                            + " sample manifest contains an existing CMO "
                                                            + "label: " + sampleManifest.getCmoSampleName()
                                                            + " that is already in use by another sample in "
                                                            + "SMILE. To prevent duplicate labels from "
                                                            + "getting persisted into SMILE, the new label "
                                                            + "generated will be used instead: "
                                                            + newSampleCmoLabel);
                                                    resolvedCmoSampleLabel = newSampleCmoLabel;
                                                } else {
                                                    LOG.info("Incoming sample: " + sampleManifest.getIgoId()
                                                            + " sample manifest contains an existing CMO "
                                                            + "label: " + sampleManifest.getCmoSampleName()
                                                            + " that is already in use by another sample in "
                                                            + "SMILE. To prevent duplicate labels from "
                                                            + "getting persisted into SMILE, a new label "
                                                            + "with an incremented nuc acid counter will be "
                                                            + "used instead: " + nextAvailableLabel);
                                                    resolvedCmoSampleLabel = nextAvailableLabel;
                                                }
                                            } else {
                                                LOG.info("Using existing CMO label for incoming sample: "
                                                    + sampleManifest.getIgoId() + ", existing CMO label: "
                                                    + sampleManifest.getCmoSampleName());
                                                resolvedCmoSampleLabel = sampleManifest.getCmoSampleName();
                                            }
                                        }
                                    }
                                    sampleMap.put("cmoSampleName", resolvedCmoSampleLabel);

                                    // update patient sample map and list of updated samples for request
                                    SampleMetadata sampleMetadata = new SampleMetadata(sampleManifest);
                                    sampleMetadata.setStatus(sampleStatus);
                                    sampleMetadata.setCmoSampleName(resolvedCmoSampleLabel);
                                    patientSamplesMap.put(sampleManifest.getCmoPatientId(),
                                            updatePatientSampleList(existingSamples, sampleMetadata));
                                }
                                // update sample status
                                sampleMap.replace("status", sampleStatus);
                            }
                            updatedSamples.add(sampleMap);
                        }
                        // update contents of 'samples' in request json map to publish
                        // and add updated request json to publisher queue
                        Map<String, Object> requestJsonMap = mapper.readValue(requestJson, Map.class);
                        requestJsonMap.put("samples", updatedSamples);
                        String updatedRequestJson = mapper.writeValueAsString(requestJsonMap);
                        // data dog log message
                        String ddogLogMessage = cmoLabelGeneratorService.generateValidationReport(
                                requestJson, updatedRequestJson, Boolean.FALSE);
                        if (ddogLogMessage != null) {
                            LOG.info(ddogLogMessage);
                        }
                        switch (igoRequestDest) {
                            case NEW_REQUEST_DEST:
                                igoNewRequestQueue.add(updatedRequestJson);
                                break;
                            case PROMOTED_REQUEST_DEST:
                                igoPromotedRequestQueue.add(updatedRequestJson);
                                break;
                            default:
                                break;
                        }
                    }
                    if (interrupted && labelGeneratorQueue.isEmpty()) {
                        break;
                    }
                } catch (InterruptedException e) {
                    interrupted = true;
                } catch (Exception e) {
                    LOG.error("Error during request handling", e);
                }
            }
            shutdownLatch.countDown();
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
                        String origSampleJson = mapper.writeValueAsString(sample);
                        List<SampleMetadata> existingSamples =
                                getExistingPatientSamples(sample.getCmoPatientId());
                        List<SampleMetadata> samplesByAltId
                                = getSamplesByAltId(sample.getAdditionalProperty("altId"));
                        // Case when sample update json doesn't have status
                        if (sample.getStatus() == null) {
                            Status newSampleStatus = cmoLabelGeneratorService
                                    .generateSampleStatus(sample, existingSamples, samplesByAltId);
                            sample.setStatus(newSampleStatus);
                        }
                        if (sample.getStatus().getValidationStatus()) {
                            // generate new cmo sample label and update sample metadata object
                            String newCmoSampleLabel =
                                    cmoLabelGeneratorService.generateCmoSampleLabel(sample,
                                            existingSamples, samplesByAltId);
                            if (newCmoSampleLabel == null) {
                                Status newSampleStatus = cmoLabelGeneratorService
                                        .generateSampleStatus(sample, existingSamples, samplesByAltId);
                                sample.setStatus(newSampleStatus);
                            }

                            // check if matching sample found and determine if label actually needs updating
                            // or if we can use the same label that is already persisted for this sample
                            // note that we want to continue publishing to the IGO_SAMPLE_UPDATE_TOPIC since
                            // there might be other metadata changes that need to be persisted that may not
                            // necessarily affect the cmo label generated
                            String resolvedCmoSampleLabel = resolveAndUpdateCmoSampleLabel(
                                    sample.getPrimaryId(), existingSamples, newCmoSampleLabel);

                            // if existing cmo label isn't blank then determine if the update is a meaningful
                            // update with respect to the sample metadata
                            if (!StringUtils.isBlank(sample.getCmoSampleName())) {
                                // if incoming updated sample has an existing cmo label then check
                                // if there are any meaningful changes to the metadata that
                                // affects the sample type abbreviation or nucleic acid abbreviation
                                Boolean hasMeaningfulUpdate =
                                        cmoLabelGeneratorService.igoSampleRequiresLabelUpdate(
                                                newCmoSampleLabel,
                                                sample.getCmoSampleName());
                                if (hasMeaningfulUpdate) {
                                    LOG.warn("Incoming updated sample has existing CMO label but "
                                            + "the label generator indicates that a change to the "
                                            + "metadata or updates to the label generation rules has "
                                            + "resulted in a new sample type abbreviation or nucleic "
                                            + "acid abbreviation: primary id="
                                            + sample.getPrimaryId()
                                            + ", incoming CMO sample label (not using)="
                                            + sample.getCmoSampleName()
                                            + ", newly generated CMO label (using)="
                                            + newCmoSampleLabel);
                                    resolvedCmoSampleLabel = newCmoSampleLabel;
                                } else {
                                    // before settling on using the provided cmo label from the
                                    // incoming sample check if that label already exists in smile
                                    // for another sample
                                    if (isCmoLabelAlreadyInUse(sample.getPrimaryId(),
                                            resolvedCmoSampleLabel)) {

                                        String nextAvailableLabel = findNextAvailableCmoLabel(
                                                        sample.getPrimaryId(),
                                                        resolvedCmoSampleLabel);
                                        if (nextAvailableLabel == null) {
                                            LOG.info("Incoming updated sample: " + sample.getPrimaryId()
                                                    + " sample metadata has a resolved CMO label: "
                                                    + resolvedCmoSampleLabel + " that is "
                                                    + "already in use by another sample in SMILE. To "
                                                    + "prevent duplicate labels from getting persisted "
                                                    + "into SMILE, the new label generated "
                                                    + "will be used instead: " + newCmoSampleLabel);
                                            resolvedCmoSampleLabel = newCmoSampleLabel;
                                        } else {
                                            LOG.info("Incoming updated sample: " + sample.getPrimaryId()
                                                    + " sample metadata has a resolved CMO label: "
                                                    + resolvedCmoSampleLabel + " that is "
                                                    + "already in use by another sample in SMILE. To "
                                                    + "prevent duplicate labels from getting persisted "
                                                    + "into SMILE, a new label with an incremented nucleic "
                                                    + "acid counter will be used instead: "
                                                    + nextAvailableLabel);
                                            resolvedCmoSampleLabel = nextAvailableLabel;
                                        }
                                    } else {
                                        LOG.info("Using existing CMO label for incoming sample: "
                                            + sample.getPrimaryId() + ", existing CMO label: "
                                            + sample.getCmoSampleName());
                                        resolvedCmoSampleLabel = sample.getCmoSampleName();
                                    }
                                }
                            }
                            // doesn't hurt to check to really make sure that this
                            // label isn't already in use by another sample
                            if (isCmoLabelAlreadyInUse(sample.getPrimaryId(),
                                            resolvedCmoSampleLabel)) {
                                LOG.info("Resolved label " + resolvedCmoSampleLabel + " is already in use "
                                        + "by another sample. Using the next available label instead.");
                                resolvedCmoSampleLabel = findNextAvailableCmoLabel(
                                        sample.getPrimaryId(),
                                        resolvedCmoSampleLabel);
                            }
                            // update the sample label for data being sent to smile server
                            sample.setCmoSampleName(resolvedCmoSampleLabel);
                        }
                        String updatedSampleJson = mapper.writeValueAsString(sample);
                        // data dog log message
                        String ddogLogMessage = cmoLabelGeneratorService.generateValidationReport(
                                origSampleJson, updatedSampleJson, Boolean.TRUE);
                        if (ddogLogMessage != null) {
                            LOG.info(ddogLogMessage);
                        }
                        LOG.info("Publishing sample to IGO_SAMPLE_UPDATE_TOPIC");
                        messagingGateway.publish(IGO_SAMPLE_UPDATE_TOPIC, updatedSampleJson);
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

    private String resolveAndUpdateCmoSampleLabel(String samplePrimaryId,
            List<SampleMetadata> existingSamples, String newCmoSampleLabel) {
        // check for matching sample in existing samples list and determine if label
        // actually needs updating or if we can use the same label that is alredy
        // persisted for this sample
        SampleMetadata matchingSample = null;
        for (SampleMetadata s : existingSamples) {
            if (s.getPrimaryId().equalsIgnoreCase(samplePrimaryId)) {
                matchingSample = s;
                break;
            }
        }
        // if sample does not require a label update then use the existing label from the
        // matching sample identified if applicable - otherwise use the newly generated label
        Boolean updateRequired = Boolean.FALSE;
        if (matchingSample != null) {
            try {
                updateRequired = cmoLabelGeneratorService.igoSampleRequiresLabelUpdate(
                        newCmoSampleLabel, matchingSample.getCmoSampleName());
            } catch (IllegalStateException e) {
                // note: special cases where we just want to keep the existing label even if it's not
                // meeting the cmo id regex requirements only if the existing cmo sample name
                // matches the existing investigator sample id
                if (matchingSample.getCmoSampleName().equals(matchingSample.getInvestigatorSampleId())) {
                    return matchingSample.getCmoSampleName();
                } else {
                    LOG.error("IllegalStateException thrown. Falling back on existing CMO label for sample: "
                            + samplePrimaryId, e);
                    return matchingSample.getCmoSampleName();
                }
            } catch (NullPointerException e2) {
                LOG.error("NPE caught during label generation check. Falling back on existing CMO label "
                        + "name for sample: " + samplePrimaryId, e2);
                return matchingSample.getCmoSampleName();
            }
            if (!updateRequired) {
                LOG.info("No change detected for CMO sample label metadata. Falling back on "
                        + "existing CMO label for matching IGO sample from database "
                        + "for sample: " + samplePrimaryId);
                return matchingSample.getCmoSampleName();
            }
        }
        LOG.info("Changes detected in CMO sample label metadata - "
                    + "updating sample CMO label to newly generated label: "
                + samplePrimaryId + ", new label: " + newCmoSampleLabel);
        return newCmoSampleLabel;
    }

    private List<SampleMetadata> updatePatientSampleList(List<SampleMetadata> existingSamples,
            SampleMetadata sample) throws JsonProcessingException {
        Boolean foundMatching = Boolean.FALSE;
        // if sample already exists in the existing samples list then simply replace at the matching index
        for (SampleMetadata existing : existingSamples) {
            if (existing.getPrimaryId().equalsIgnoreCase(sample.getPrimaryId())) {
                existingSamples.set(existingSamples.indexOf(existing), sample);
                foundMatching = Boolean.TRUE;
                break;
            }
        }
        // if matching sample not found then append to list and return
        if (!foundMatching) {
            existingSamples.add(sample);
        }
        return existingSamples;
    }

    private Map<String, List<SampleMetadata>> getPatientSamplesMap(List<Object> samples)
            throws Exception {
        Map<String, List<SampleMetadata>> patientSamplesMap = new HashMap<>();
        for (Object sample : samples) {
            IgoSampleManifest igoSampleManifest = mapper.convertValue(sample, IgoSampleManifest.class);
            // get or request existing patient samples and update patient sample mapping
            if (!patientSamplesMap.containsKey(igoSampleManifest.getCmoPatientId())
                    && !StringUtils.isBlank(igoSampleManifest.getCmoPatientId())) {
                List<SampleMetadata> ptSamples = getExistingPatientSamples(
                        igoSampleManifest.getCmoPatientId());
                patientSamplesMap.put(igoSampleManifest.getCmoPatientId(),
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

    private List<SampleMetadata> getSamplesByCmoLabel(String cmoLabel) throws Exception {
        // return empty list if cmo label is null/empty
        if (StringUtils.isBlank(cmoLabel)) {
            return new ArrayList<>();
        }

        Message reply = messagingGateway.request(SAMPLES_BY_CMO_LABEL_REQREPLY_TOPIC,
                    cmoLabel);
        SampleMetadata[] samplesByCmoLabel = mapper.readValue(
                new String(reply.getData(), StandardCharsets.UTF_8),
                SampleMetadata[].class);
        return new ArrayList<>(Arrays.asList(samplesByCmoLabel));
    }

    private List<SampleMetadata> getSamplesByAltId(String altId) throws Exception {
        // return empty list if alt id is null/empty
        if (StringUtils.isBlank(altId)) {
            return new ArrayList<>();
        }

        Message reply = messagingGateway.request(SAMPLES_BY_ALT_ID_REQREPLY_TOPIC,
                    altId);
        SampleMetadata[] samplesByAltId = mapper.readValue(
                new String(reply.getData(), StandardCharsets.UTF_8),
                SampleMetadata[].class);
        return new ArrayList<>(Arrays.asList(samplesByAltId));
    }

    private Boolean isCmoLabelAlreadyInUse(String primaryId, String cmoLabel) throws Exception {
        List<SampleMetadata> samplesByCmoLabel = getSamplesByCmoLabel(cmoLabel);
        for (SampleMetadata sm : samplesByCmoLabel) {
            // if there are any samples returned that aren't the same primary id
            // as the one provided then that indicates that the cmo label already exists
            // in smile and is associated with a different sample
            if (!sm.getPrimaryId().equals(primaryId)) {
                return Boolean.TRUE;
            }
        }
        return Boolean.FALSE;
    }

    private String getRequestIdFromRequestJson(String requestJson) throws JsonProcessingException {
        Map<String, Object> requestJsonMap = mapper.readValue(requestJson, Map.class);
        return requestJsonMap.get("requestId").toString();
    }

    private List<Object> getSamplesFromRequestJson(String requestJson)
            throws JsonProcessingException {
        Map<String, Object> requestJsonMap = mapper.readValue(requestJson, Map.class);
        List<Object> sampleManifests =
                Arrays.asList(mapper.convertValue(requestJsonMap.get("samples"),
                        Object[].class));
        return sampleManifests;
    }

    @Override
    public void initialize(Gateway gateway) throws Exception {
        if (!initialized) {
            messagingGateway = gateway;
            setupCmoLabelGeneratorHandler(messagingGateway, this);
            setupCmoPromotedLabelHandler(messagingGateway, this);
            setupCmoSampleLabelUpdateHandler(messagingGateway, this);
            initializeMessageHandlers();
            initialized = true;
        } else {
            LOG.error("Messaging Handler Service has already been initialized, ignoring request.");
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
    public void cmoPromotedLabelHandler(String requestJson) throws Exception {
        if (!initialized) {
            throw new IllegalStateException("Message Handling Service has not been initialized");
        }
        if (!shutdownInitiated) {
            cmoPromotedLabelQueue.put(requestJson);
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
        cmoPromotedLabelShutdownLatch.await();
        newRequestPublisherShutdownLatch.await();
        promotedRequestPublisherShutdownLatch.await();
        shutdownInitiated = true;
    }

    private void initializeMessageHandlers() throws Exception {
        cmoLabelGeneratorShutdownLatch = new CountDownLatch(NUM_NEW_REQUEST_HANDLERS);
        final Phaser cmoLabelGeneratorPhaser = new Phaser();
        cmoLabelGeneratorPhaser.register();
        for (int lc = 0; lc < NUM_NEW_REQUEST_HANDLERS; lc++) {
            cmoLabelGeneratorPhaser.register();
            exec.execute(new CmoLabelGeneratorHandler(cmoLabelGeneratorPhaser,
                    IgoRequestDest.NEW_REQUEST_DEST, cmoLabelGeneratorQueue,
                    cmoLabelGeneratorShutdownLatch));
        }
        cmoLabelGeneratorPhaser.arriveAndAwaitAdvance();

        cmoPromotedLabelShutdownLatch = new CountDownLatch(NUM_NEW_REQUEST_HANDLERS);
        final Phaser cmoPromotedLabelPhaser = new Phaser();
        cmoPromotedLabelPhaser.register();
        for (int lc = 0; lc < NUM_PROMOTED_REQUEST_HANDLERS; lc++) {
            cmoPromotedLabelPhaser.register();
            exec.execute(new CmoLabelGeneratorHandler(cmoPromotedLabelPhaser,
                    IgoRequestDest.PROMOTED_REQUEST_DEST, cmoPromotedLabelQueue,
                    cmoPromotedLabelShutdownLatch));
        }
        cmoPromotedLabelPhaser.arriveAndAwaitAdvance();

        newRequestPublisherShutdownLatch = new CountDownLatch(NUM_NEW_REQUEST_HANDLERS);
        final Phaser newRequestPhaser = new Phaser();
        newRequestPhaser.register();
        for (int lc = 0; lc < NUM_NEW_REQUEST_HANDLERS; lc++) {
            newRequestPhaser.register();
            exec.execute(new IgoRequestHandler(newRequestPhaser, IgoRequestDest.NEW_REQUEST_DEST,
                    igoNewRequestQueue, newRequestPublisherShutdownLatch));
        }
        newRequestPhaser.arriveAndAwaitAdvance();

        promotedRequestPublisherShutdownLatch = new CountDownLatch(NUM_PROMOTED_REQUEST_HANDLERS);
        final Phaser promotedRequestPhaser = new Phaser();
        promotedRequestPhaser.register();
        for (int lc = 0; lc < NUM_PROMOTED_REQUEST_HANDLERS; lc++) {
            promotedRequestPhaser.register();
            exec.execute(new IgoRequestHandler(promotedRequestPhaser, IgoRequestDest.PROMOTED_REQUEST_DEST,
                    igoPromotedRequestQueue, promotedRequestPublisherShutdownLatch));
        }
        promotedRequestPhaser.arriveAndAwaitAdvance();

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

    private void setupCmoPromotedLabelHandler(Gateway gateway,
            MessageHandlingService messageHandlingService) throws Exception {
        gateway.subscribe(CMO_PROMOTED_LABEL_TOPIC, Object.class, new MessageConsumer() {
            public void onMessage(Message msg, Object message) {
                LOG.info("Received message on topic: " + CMO_PROMOTED_LABEL_TOPIC);
                try {
                    messageHandlingService.cmoPromotedLabelHandler(
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
                    if (StringUtils.isEmpty(sampleMetadata.getIgoRequestId())) {
                        String requestId = ObjectUtils.firstNonNull(
                                sampleMetadata.getAdditionalProperties().get("requestId"),
                                sampleMetadata.getAdditionalProperties().get("igoRequestId"));
                        sampleMetadata.setIgoRequestId(requestId);
                    }
                    messageHandlingService.cmoSampleLabelUpdateHandler(sampleMetadata);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    private String findNextAvailableCmoLabel(String primaryId, String cmoLabel) throws Exception {
        String incrementedLabel = cmoLabelGeneratorService.incrementNucleicAcidCounter(cmoLabel);
        while (isCmoLabelAlreadyInUse(primaryId, incrementedLabel)) {
            incrementedLabel = cmoLabelGeneratorService.incrementNucleicAcidCounter(incrementedLabel);
        }
        return incrementedLabel;
    }
}

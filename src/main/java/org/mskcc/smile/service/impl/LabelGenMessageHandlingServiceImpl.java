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
import org.mskcc.smile.service.CmoLabelGeneratorService;
import org.mskcc.smile.service.MessageHandlingService;
import org.mskcc.smile.service.util.CmoLabelParts;
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
    private static final BlockingQueue<List<Map<String, Object>>> cmoSampleLabelUpdateQueue =
        new LinkedBlockingQueue<>();

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
                        List<Map<String, Object>> samples = getSamplesFromRequestJson(requestJson);

                        // get existing samples for all patients in the request
                        Map<String, List<CmoLabelParts>> patientSamplesMap = getPatientSamplesMap(samples);
                        Map<String, List<CmoLabelParts>> altIdSamplesMap = getAltIdSamplesMap(samples);

                        // udpated samples list will store samples which had a label generated successfully
                        List<Map<String, Object>> updatedSamples = new ArrayList<>();
                        for (Map<String, Object> sampleMap : samples) {
                            CmoLabelParts labelParts = new CmoLabelParts(sampleMap, requestId);
                            if (StringUtils.isBlank(labelParts.getCmoPatientId())) {
                                // skip over samples with missing cmo patient id this should be
                                // getting caught by the request filter but we are taking extra precautions
                                // due to ongoing timeout exception investigations
                                LOG.warn("Sample is missing CMO patient ID that was not caught by the "
                                        + "request filter: " + mapper.writeValueAsString(sampleMap));
                                continue;
                            }

                            Map<String, Object> statusMap = mapper.convertValue(
                                    sampleMap.get("status"), Map.class);

                            Boolean validationStatus = (Boolean) statusMap.get("validationStatus");
                            if (validationStatus) {
                                // get existing patient samples for cmo patient id
                                List<CmoLabelParts> existingSamples =
                                        patientSamplesMap.getOrDefault(labelParts.getCmoPatientId(),
                                                new ArrayList<>());
                                List<CmoLabelParts> samplesByAltId
                                        = altIdSamplesMap.getOrDefault(labelParts.getAltId(),
                                                new ArrayList<>());

                                String newLabel = cmoLabelGeneratorService.generateCmoSampleLabel(
                                        labelParts, existingSamples, samplesByAltId);
                                if (newLabel == null) {
                                    statusMap = cmoLabelGeneratorService.generateSampleStatus(
                                            labelParts, existingSamples, samplesByAltId);
                                    LOG.error("Unable to generate new CMO sample label for sample: "
                                            + labelParts.getPrimaryId());
                                    // check if we can fall back on an existing cmo label that might have
                                    // come in with the incoming request json
                                    if (!StringUtils.isBlank(labelParts.getCmoSampleName())) {
                                        LOG.info("Could not generate new CMO sample label for sample: "
                                                + labelParts.getPrimaryId()
                                                + ". Falling back on incoming CMO sample label for sample: "
                                                + labelParts.getCmoSampleName());
                                        // don't actually need to set this?
                                        sampleMap.put("cmoSampleName", labelParts.getCmoSampleName());
                                    }
                                } else {
                                    // check if matching sample found and determine if label actually needs
                                    // updating or if we can use the same label that
                                    // is already persisted for this sample
                                    // note that we want to continue publishing to the IGO_SAMPLE_UPDATE_TOPIC
                                    // since there might be other metadata changes that need to be persisted
                                    // that may not necessarily affect the cmo label generated
                                    String resolvedLabel = resolveAndUpdateCmoSampleLabel(
                                            labelParts.getPrimaryId(), existingSamples, newLabel);
                                    if (!StringUtils.isBlank(labelParts.getCmoSampleName())) {
                                        // if incoming sample has an existing cmo label then check
                                        // if there are any meaningful changes to the metadata that
                                        // affects the sample type abbreviation or nucleic acid abbreviation
                                        Boolean hasMeaningfulUpdate =
                                                cmoLabelGeneratorService.igoSampleRequiresLabelUpdate(
                                                        resolvedLabel,
                                                        labelParts.getCmoSampleName());
                                        if (hasMeaningfulUpdate) {
                                            LOG.warn(makeLogMsgExistingCmoLabelNotUsing(
                                                    labelParts.getPrimaryId(),
                                                    labelParts.getCmoSampleName(),
                                                    newLabel));
                                        } else {
                                            // before settling on using the provided cmo label from the
                                            // incoming sample check if that label already exists in smile
                                            // for another sample
                                            if (isCmoLabelAlreadyInUse(labelParts.getPrimaryId(),
                                                    labelParts.getCmoSampleName())) {

                                                String nextAvailableLabel = findNextAvailableCmoLabel(
                                                        labelParts.getPrimaryId(),
                                                        labelParts.getCmoSampleName(),
                                                        labelParts.getAltId());

                                                if (nextAvailableLabel == null) {
                                                    LOG.info(makeLogMsgResolvedLabelNotUsing(
                                                            labelParts.getPrimaryId(),
                                                            labelParts.getCmoSampleName(),
                                                            newLabel));
                                                    resolvedLabel = newLabel;
                                                } else {
                                                    LOG.info(makeLogMsgResolvedLabelNotUsing(
                                                            labelParts.getPrimaryId(),
                                                            labelParts.getCmoSampleName(),
                                                            nextAvailableLabel));
                                                    resolvedLabel = nextAvailableLabel;
                                                }
                                            } else {
                                                LOG.info("Using existing CMO label for incoming sample: "
                                                    + labelParts.getPrimaryId() + ", existing label: "
                                                    + labelParts.getCmoSampleName());
                                                resolvedLabel = labelParts.getCmoSampleName();
                                            }
                                        }
                                    }
                                    // update patient sample map and list of updated samples for request
                                    sampleMap.put("cmoSampleName", resolvedLabel);
                                    labelParts.setCmoSampleName(resolvedLabel);
                                    patientSamplesMap.put(labelParts.getCmoPatientId(),
                                            updatePatientSampleList(existingSamples, labelParts));
                                    altIdSamplesMap.put(labelParts.getAltId(),
                                            updateAltIdSampleList(samplesByAltId, labelParts));
                                }
                                // update sample status
                                sampleMap.replace("status", statusMap);
                            }
                            updatedSamples.add(sampleMap);
                        }
                        // update contents of 'samples' in request json map to publish
                        // and add updated request json to publisher queue
                        Map<String, Object> requestJsonMap = mapper.readValue(requestJson, Map.class);
                        requestJsonMap.put("samples", updatedSamples);
                        String updatedRequestJson = mapper.writeValueAsString(requestJsonMap);
                        // data dog log message
                        String ddogLogMessage = cmoLabelGeneratorService.generateValidationReportLog(
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
                    List<Map<String, Object>> samples
                            = cmoSampleLabelUpdateQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (samples != null) {
                        // get existing samples for all patients in the request
                        Map<String, List<CmoLabelParts>> patientSamplesMap = getPatientSamplesMap(samples);
                        Map<String, List<CmoLabelParts>> altIdSamplesMap = getAltIdSamplesMap(samples);

                        Map<String, String> origSampleJsonMap = new HashMap<>();
                        for (int i = 0; i < samples.size(); i++) {
                            Map<String, Object> sampleMap = samples.get(i);
                            CmoLabelParts labelParts = new CmoLabelParts(sampleMap, null);
                            origSampleJsonMap.put(labelParts.getPrimaryId(),
                                    labelParts.getOrigSampleJsonStr());

                            List<CmoLabelParts> existingSamples =
                                            patientSamplesMap.getOrDefault(labelParts.getCmoPatientId(),
                                                    new ArrayList<>());
                            List<CmoLabelParts> samplesByAltId
                                    = altIdSamplesMap.getOrDefault(labelParts.getAltId(), new ArrayList<>());

                            // Case when sample update json doesn't have status
                            Map<String, Object> statusMap
                                    = mapper.convertValue(sampleMap.get("status"), Map.class);
                            if (sampleMap.get("status") == null) {
                                statusMap = cmoLabelGeneratorService
                                        .generateSampleStatus(labelParts, existingSamples, samplesByAltId);
                                sampleMap.put("status", statusMap);
                            }
                            Boolean validationStatus = (Boolean) statusMap.get("validationStatus");
                            if (validationStatus) {
                                // generate new cmo sample label and update sample metadata object

                                String newLabel =
                                        cmoLabelGeneratorService.generateCmoSampleLabel(labelParts,
                                                existingSamples, samplesByAltId);
                                if (newLabel == null) {
                                    Map<String, Object> newSampleStatus
                                            = cmoLabelGeneratorService.generateSampleStatus(labelParts,
                                                    existingSamples, samplesByAltId);
                                    sampleMap.put("status", newSampleStatus);
                                }

                                // check if matching sample found and determine if label actually needs
                                // updating or if we can use the same label that is already persisted for
                                // this sample note that we want to continue publishing to the
                                // IGO_SAMPLE_UPDATE_TOPIC since there might be other metadata changes
                                // that need to be persisted that may not necessarily affect
                                // the cmo label generated
                                String resolvedLabel
                                        = resolveAndUpdateCmoSampleLabel(labelParts.getPrimaryId(),
                                                existingSamples, newLabel);

                                // if existing cmo label isn't blank then determine if the update is
                                // a meaningful update with respect to the sample metadata
                                if (!StringUtils.isBlank(labelParts.getCmoSampleName())) {
                                    // if incoming updated sample has an existing cmo label then check
                                    // if there are any meaningful changes to the metadata that
                                    // affects the sample type abbreviation or nucleic acid abbreviation
                                    Boolean hasMeaningfulUpdate =
                                            cmoLabelGeneratorService.igoSampleRequiresLabelUpdate(
                                                    resolvedLabel, labelParts.getCmoSampleName());
                                    if (hasMeaningfulUpdate) {
                                        LOG.warn(makeLogMsgExistingCmoLabelNotUsing(labelParts.getPrimaryId(),
                                                labelParts.getCmoSampleName(),
                                                newLabel));
                                    } else {
                                        // before settling on using the provided cmo label from the
                                        // incoming sample check if that label already exists in smile
                                        // for another sample
                                        if (isCmoLabelAlreadyInUse(labelParts.getPrimaryId(),
                                                resolvedLabel)) {
                                            String nextAvailableLabel
                                                    = findNextAvailableCmoLabel(
                                                            labelParts.getPrimaryId(),
                                                            resolvedLabel,
                                                            labelParts.getAltId());
                                            if (nextAvailableLabel == null) {
                                                LOG.info(makeLogMsgResolvedLabelNotUsing(
                                                        labelParts.getPrimaryId(),
                                                        resolvedLabel,
                                                        newLabel));
                                                resolvedLabel = newLabel;
                                            } else {
                                                LOG.info(makeLogMsgResolvedLabelNotUsing(
                                                        labelParts.getPrimaryId(),
                                                        resolvedLabel,
                                                        nextAvailableLabel));
                                                resolvedLabel = nextAvailableLabel;
                                            }
                                        } else {
                                            LOG.info("Using existing CMO label for incoming sample: "
                                                + labelParts.getPrimaryId() + ", existing CMO label: "
                                                + labelParts.getCmoSampleName());
                                            resolvedLabel = labelParts.getCmoSampleName();
                                        }
                                    }
                                }
                                // doesn't hurt to check to really make sure that this
                                // label isn't already in use by another sample
                                if (isCmoLabelAlreadyInUse(labelParts.getPrimaryId(), resolvedLabel)) {
                                    LOG.info("Resolved label " + resolvedLabel
                                            + " is already in use by another sample. "
                                            + "Using the next available label instead.");
                                    resolvedLabel = findNextAvailableCmoLabel(labelParts.getPrimaryId(),
                                            resolvedLabel,
                                            labelParts.getAltId());
                                }
                                // update the sample label for data being sent to smile server
                                sampleMap.put("cmoSampleName", resolvedLabel);
                                labelParts.setCmoSampleName(resolvedLabel);
                                patientSamplesMap.put(labelParts.getCmoPatientId(),
                                        updatePatientSampleList(existingSamples, labelParts));
                                altIdSamplesMap.put(labelParts.getAltId(),
                                        updateAltIdSampleList(samplesByAltId, labelParts));
                            }
                            samples.set(i, sampleMap);
                        }

                        // samples can still publish to the smile server individually but only after
                        // all possible sample label updates have been completed to avoid clashes with
                        // numbering when handling updates for samples that share the same patient
                        for (Map<String, Object> sample : samples) {
                            // data dog log message
                            String origSampleJson = origSampleJsonMap.get(sample.get("primaryId").toString());
                            String ddogLogMessage = cmoLabelGeneratorService.generateValidationReportLog(
                                    origSampleJson, mapper.writeValueAsString(sample), Boolean.TRUE);
                            if (ddogLogMessage != null) {
                                LOG.info(ddogLogMessage);
                            }
                            messagingGateway.publish(IGO_SAMPLE_UPDATE_TOPIC,
                                    mapper.writeValueAsString(sample));
                        }
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
            List<CmoLabelParts> existingSamples, String newCmoSampleLabel) {
        // check for matching sample in existing samples list and determine if label
        // actually needs updating or if we can use the same label that is alredy
        // persisted for this sample
        CmoLabelParts matchingSample = null;
        for (CmoLabelParts s : existingSamples) {
            if (s.getPrimaryId().equalsIgnoreCase(samplePrimaryId)) {
                matchingSample = s;
                break;
            }
        }
        // if sample does not require a label update then use the existing label from the
        // matching sample identified if applicable - otherwise use the newly generated label
        Boolean updateRequired = Boolean.FALSE;
        if (matchingSample != null) {
            // if matching sample cmo label is blank then return new label by default
            if (StringUtils.isBlank(matchingSample.getCmoSampleName())) {
                return newCmoSampleLabel;
            }
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

    private List<CmoLabelParts> updateAltIdSampleList(List<CmoLabelParts> altIdSamples,
            CmoLabelParts sample) throws JsonProcessingException {
        Boolean foundMatching = Boolean.FALSE;
        // if sample already exists in the existing samples list then simply replace at the matching index
        String currentPrimaryId = sample.getPrimaryId();
        for (CmoLabelParts existing : altIdSamples) {
            String primaryId = existing.getPrimaryId();
            if (primaryId.equalsIgnoreCase(currentPrimaryId)) {
                altIdSamples.set(altIdSamples.indexOf(existing), sample);
                foundMatching = Boolean.TRUE;
                break;
            }
        }
        // if matching sample not found then append to list and return
        if (!foundMatching) {
            altIdSamples.add(sample);
        }
        return altIdSamples;
    }

    private List<CmoLabelParts> updatePatientSampleList(List<CmoLabelParts> existingSamples,
            CmoLabelParts sample) throws JsonProcessingException {
        Boolean foundMatching = Boolean.FALSE;
        // if sample already exists in the existing samples list then simply replace at the matching index
        String currentPrimaryId = sample.getPrimaryId();
        for (CmoLabelParts existing : existingSamples) {
            String primaryId = existing.getPrimaryId();
            if (primaryId.equalsIgnoreCase(currentPrimaryId)) {
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

    private Map<String, List<CmoLabelParts>> getPatientSamplesMap(List<Map<String, Object>> samples)
            throws Exception {
        Map<String, List<CmoLabelParts>> patientSamplesMap = new HashMap<>();
        for (Map<String, Object> sm : samples) {
            // get or request existing patient samples and update patient sample mapping
            CmoLabelParts sample = new CmoLabelParts(sm, null);
            if (StringUtils.isBlank(sample.getCmoPatientId())
                    || patientSamplesMap.containsKey(sample.getCmoPatientId())) {
                continue;
            }
            List<CmoLabelParts> ptSamples = getExistingPatientSamples(sample.getCmoPatientId());
            patientSamplesMap.put(sample.getCmoPatientId(),
                    new ArrayList<>(ptSamples));
        }
        return patientSamplesMap;
    }

    private List<CmoLabelParts> getExistingPatientSamples(String cmoPatientId) throws Exception {
        Message reply = messagingGateway.request(PATIENT_SAMPLES_REQUEST_TOPIC,
                    cmoPatientId);
        List<Object> sampleObjList = mapper.readValue(
                new String(reply.getData(), StandardCharsets.UTF_8),
                List.class);
        List<CmoLabelParts> samples = new ArrayList<>();
        for (Object s : sampleObjList) {
            Map<String, Object> sm = mapper.convertValue(s, Map.class);
            samples.add(new CmoLabelParts(sm, null));
        }
        return samples;
    }

    private List<CmoLabelParts> getSamplesByCmoLabel(String cmoLabel) throws Exception {
        // return empty list if cmo label is null/empty
        if (StringUtils.isBlank(cmoLabel)) {
            return new ArrayList<>();
        }

        Message reply = messagingGateway.request(SAMPLES_BY_CMO_LABEL_REQREPLY_TOPIC,
                    cmoLabel);
        List<Object> sampleObjList = mapper.readValue(
                new String(reply.getData(), StandardCharsets.UTF_8),
                List.class);
        List<CmoLabelParts> samples = new ArrayList<>();
        for (Object s : sampleObjList) {
            Map<String, Object> sm = mapper.convertValue(s, Map.class);
            samples.add(new CmoLabelParts(sm, null));
        }
        return samples;
    }

    private Map<String, List<CmoLabelParts>> getAltIdSamplesMap(List<Map<String, Object>> samples)
            throws Exception {
        Map<String, List<CmoLabelParts>> altIdSamplesMap = new HashMap<>();
        for (Map<String, Object> sm : samples) {
            // get or request existing patient samples and update patient sample mapping
            CmoLabelParts sample = new CmoLabelParts(sm, null);
            if (StringUtils.isBlank(sample.getAltId())
                    || altIdSamplesMap.containsKey(sample.getAltId())) {
                continue;
            }
            List<CmoLabelParts> altIdSamples = getSamplesByAltId(sample.getAltId());
            altIdSamplesMap.put(sample.getCmoPatientId(),
                    new ArrayList<>(altIdSamples));
        }
        return altIdSamplesMap;
    }

    private List<CmoLabelParts> getSamplesByAltId(String altId) throws Exception {
        // return empty list if alt id is null/empty
        if (StringUtils.isBlank(altId)) {
            return new ArrayList<>();
        }

        Message reply = messagingGateway.request(SAMPLES_BY_ALT_ID_REQREPLY_TOPIC,
                    altId);
        List<Object> sampleObjList = mapper.readValue(
                new String(reply.getData(), StandardCharsets.UTF_8),
                List.class);
        List<CmoLabelParts> samples = new ArrayList<>();
        for (Object s : sampleObjList) {
            Map<String, Object> sm = mapper.convertValue(s, Map.class);
            samples.add(new CmoLabelParts(sm, null));
        }
        return samples;
    }

    private Boolean isCmoLabelAlreadyInUse(String primaryId, String cmoLabel) throws Exception {
        List<CmoLabelParts> samplesByCmoLabel = getSamplesByCmoLabel(cmoLabel);
        for (CmoLabelParts sm : samplesByCmoLabel) {
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

    private List<Map<String, Object>> getSamplesFromRequestJson(String requestJson)
            throws JsonProcessingException {
        Map<String, Object> requestJsonMap = mapper.readValue(requestJson, Map.class);
        List<Object> sampleObjList = mapper.convertValue(requestJsonMap.get("samples"),
                List.class);
        List<Map<String, Object>> sampleManifests = new ArrayList<>();
        for (Object s : sampleObjList) {
            Map<String, Object> sm = mapper.convertValue(s, Map.class);
            sampleManifests.add(sm);
        }
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
    public void cmoSampleLabelUpdateHandler(List<Map<String, Object>> sampleMetadataList) throws Exception {
        if (!initialized) {
            throw new IllegalStateException("Message Handling Service has not been initialized");
        }
        if (!shutdownInitiated) {
            cmoSampleLabelUpdateQueue.put(sampleMetadataList);
        } else {
            LOG.error("Shutdown initiated, not accepting update for IGO sample: "
                    + sampleMetadataList);
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
                    Object msgDataObject = mapper.readValue(
                            new String(msg.getData(), StandardCharsets.UTF_8),
                            Object.class);
                    List<Object> sampleObjList = mapper.readValue(msgDataObject.toString(), List.class);
                    List<Map<String, Object>> sampleMetadataList = new ArrayList<>();
                    for (Object s : sampleObjList) {
                        Map<String, Object> sm = mapper.convertValue(s, Map.class);
                        if (!sm.containsKey("igoRequestId")) {
                            CmoLabelParts labelParts = new CmoLabelParts(sm, null);
                            sm.put("igoRequestId", labelParts.getIgoRequestId());
                        }
                        sampleMetadataList.add(sm);
                    }
                    messageHandlingService.cmoSampleLabelUpdateHandler(sampleMetadataList);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    private String findNextAvailableCmoLabel(String primaryId, String cmoLabel,
            String altId) throws Exception {
        while (isCmoLabelAlreadyInUse(primaryId, cmoLabel)) {
            List<CmoLabelParts> samplesByCmoLabel = getSamplesByCmoLabel(cmoLabel);
            for (Object s : samplesByCmoLabel) {
                // if there are any samples returned that aren't the same primary id
                // as the one provided then that indicates that the cmo label already exists
                // in smile and is associated with a different sample
                // if diff alt ids then increment by sample counter otherwise increment nuc acid counter
                Map<String, Object> sm = mapper.convertValue(s, Map.class);
                CmoLabelParts sample = new CmoLabelParts(sm, null);
                if (!sample.getPrimaryId().equals(primaryId)) {
                    String otherAltId = sample.getAltId();
                    if (!StringUtils.isBlank(altId) && !StringUtils.isBlank(otherAltId)
                            && !altId.equals(otherAltId)) {
                        cmoLabel = cmoLabelGeneratorService.incrementSampleCounter(cmoLabel);
                    } else {
                        cmoLabel = cmoLabelGeneratorService.incrementNucleicAcidCounter(cmoLabel);
                    }
                }
            }
        }
        return cmoLabel;
    }

    private String makeLogMsgExistingCmoLabelNotUsing(String primaryId, String labelNotUsing,
            String labelUsing) {
        StringBuilder builder = new StringBuilder();
        builder.append("Incoming sample manifest has existing CMO label but ")
                .append("the label generator indicates that a change to the ")
                .append("metadata or updates to the label generation rules has ")
                .append("resulted in a new sample type abbreviation or nucleic ")
                .append("acid abbreviation: primary id=").append(primaryId)
                .append(", incoming CMO sample label (not using)=").append(labelNotUsing)
                .append(", newly generated CMO label (using)=").append(labelUsing);
        return builder.toString();
    }

    private String makeLogMsgResolvedLabelNotUsing(String primaryId, String labelNotUsing,
            String labelUsing) {
        StringBuilder builder = new StringBuilder();
        builder.append("Incoming updated sample: ")
                .append(primaryId).append(" sample metadata has a resolved CMO label: ")
                .append(labelNotUsing)
                .append(" that is already in use by another sample in SMILE. To ")
                .append("prevent duplicate labels from getting persisted ")
                .append("into SMILE, the new label generated ")
                .append("will be used instead: ").append(labelUsing);
        return builder.toString();
    }
}

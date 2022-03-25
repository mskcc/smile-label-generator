package org.mskcc.smile.service;

import org.mskcc.cmo.messaging.Gateway;
import org.mskcc.smile.model.SampleMetadata;

public interface MessageHandlingService {
    void initialize(Gateway gateway) throws Exception;
    void cmoLabelGeneratorHandler(String requestJson) throws Exception;
    void cmoSampleLabelUpdateHandler(SampleMetadata sampleMetadata) throws Exception;
    void shutdown() throws Exception;
}

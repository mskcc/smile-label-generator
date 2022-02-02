package org.mskcc.cmo.metadb.service;

import org.mskcc.cmo.messaging.Gateway;
import org.mskcc.cmo.metadb.model.SampleMetadata;

public interface MessageHandlingService {
    void initialize(Gateway gateway) throws Exception;
    void cmoLabelGeneratorHandler(String requestJson) throws Exception;
    void cmoSampleLabelUpdateHandler(SampleMetadata sampleMetadata) throws Exception;
    void shutdown() throws Exception;
}

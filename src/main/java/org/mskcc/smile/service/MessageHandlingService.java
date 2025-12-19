package org.mskcc.smile.service;

import java.util.List;
import java.util.Map;
import org.mskcc.cmo.messaging.Gateway;

public interface MessageHandlingService {
    void initialize(Gateway gateway) throws Exception;
    void cmoLabelGeneratorHandler(String requestJson) throws Exception;
    void cmoPromotedLabelHandler(String requestJson) throws Exception;
    void cmoSampleLabelUpdateHandler(List<Map<String, Object>> sampleMetadataList) throws Exception;
    void shutdown() throws Exception;
}

package org.mskcc.cmo.metadb.service;

import org.mskcc.cmo.messaging.Gateway;
import org.mskcc.cmo.metadb.model.web.PublishedMetaDbRequest;

public interface MessageHandlingService {
    void initialize(Gateway gateway) throws Exception;
    void cmoLabelGeneratorHandler(PublishedMetaDbRequest request) throws Exception;
    void shutdown() throws Exception;
}

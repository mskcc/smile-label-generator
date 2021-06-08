package org.mskcc.cmo.metadb.service;

import org.mskcc.cmo.metadb.model.SampleMetadata;

/**
 *
 * @author ochoaa
 */
public interface CmoLabelGeneratorService {
    String generateCmoSampleLabel(SampleMetadata sampleMetadata);
}

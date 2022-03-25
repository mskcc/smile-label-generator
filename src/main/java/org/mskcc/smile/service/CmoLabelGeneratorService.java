package org.mskcc.smile.service;

import java.util.List;
import org.mskcc.smile.model.SampleMetadata;
import org.mskcc.smile.model.igo.IgoSampleManifest;

/**
 *
 * @author ochoaa
 */
public interface CmoLabelGeneratorService {
    String generateCmoSampleLabel(String requestId,
            IgoSampleManifest sampleManifest, List<SampleMetadata> existingPatientSamples);
    String generateCmoSampleLabel(SampleMetadata sample, List<SampleMetadata> existingPatientSamples);
    Boolean igoSampleRequiresLabelUpdate(String newCmoLabel, String existingCmoLabel);
}

package org.mskcc.smile.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.List;
import java.util.Map;
import org.mskcc.smile.service.util.CmoLabelParts;

/**
 *
 * @author ochoaa
 */
public interface CmoLabelGeneratorService {
    String generateCmoSampleLabel(CmoLabelParts sample, List<CmoLabelParts> existingPatientSamples,
            List<CmoLabelParts> samplesByAltId);
    Map<String, Object> generateSampleStatus(CmoLabelParts sample, List<CmoLabelParts> existingSamples,
            List<CmoLabelParts> samplesByAltId) throws JsonProcessingException;
    Boolean igoSampleRequiresLabelUpdate(String newCmoLabel, String existingCmoLabel);
    String resolveSampleTypeAbbreviation(CmoLabelParts sample);
    String resolveSampleTypeAbbrevWithContext(String primaryId, String resolvedSampleTypeAbbrev,
            List<CmoLabelParts> samplesByAltId);
    String generateValidationReportLog(String originalJson, String filteredJson, Boolean isSample)
            throws JsonProcessingException;
    String incrementNucleicAcidCounter(String cmoLabel);
    String incrementSampleCounter(String cmoLabel);
}

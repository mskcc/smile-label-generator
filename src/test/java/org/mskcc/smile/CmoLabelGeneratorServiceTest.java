package org.mskcc.smile;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mskcc.smile.commons.enums.NucleicAcid;
import org.mskcc.smile.commons.enums.SpecimenType;
import org.mskcc.smile.config.TestConfiguration;
import org.mskcc.smile.model.MockJsonTestData;
import org.mskcc.smile.model.SampleMetadata;
import org.mskcc.smile.model.Status;
import org.mskcc.smile.model.igo.IgoSampleManifest;
import org.mskcc.smile.service.CmoLabelGeneratorService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest(classes = LabelGeneratorTestApp.class)
@Import(TestConfiguration.class)
public class CmoLabelGeneratorServiceTest {
    private final ObjectMapper mapper = new ObjectMapper();

    // setting as empty for now until alt id is fully supported
    List<SampleMetadata> DEFAULT_SAMPLES_BY_ALT_ID = new ArrayList<>();

    @Autowired
    private CmoLabelGeneratorService cmoLabelGeneratorService;

    @Autowired
    private Map<String, MockJsonTestData> mockedRequestJsonDataMap;

    /**
     * Tests to ensure the mocked request json data map is not null
     */
    @Test
    public void testMockedRequestJsonDataLoading() {
        Assertions.assertNotNull(mockedRequestJsonDataMap);
    }

    /**
     * Test for handling of fields that are a mix of null or empty strings in the requestJson
     * The filter is expected to fail
     */
    @Test
    public void testValidRequestJson() throws Exception {
        MockJsonTestData requestJson = mockedRequestJsonDataMap
                .get("mockIncomingRequest1JsonDataWith2T2N");
        String modifiedRequestJson = requestJson.getJsonString();
        Assertions.assertNotNull(modifiedRequestJson);
    }

    /**
     * Tests if a new cmoLabel is properly generated for
     *      a sample with metadata updates including igoId/primaryId
     *  Expected behavior: Should return a label with
     *      incremented sample count and new Sample Type Abbreviation
     * @throws Exception
     */
    @Test
    public void testCmoLabelGenForSampleUpdate() throws Exception {
        // existing samples for patientId: C-MP789JR
        List<SampleMetadata> existingSamples =
                getPatientSamplesFromRequestJson("mockPublishedRequest1JsonDataWith2T2N", "C-MP789JR");

        // updated SampleMetadata
        SampleMetadata updatedSample = mapper.convertValue(
                existingSamples.get(0).clone(), SampleMetadata.class);
        updatedSample.setSampleClass("Non-PDX");
        updatedSample.setPrimaryId("newPrimaryId");

        // generate cmoLabel for new incoming sample with no matching samples by alt id
        // note: the sample type abbreviations for the existing patient samples are X and N
        // which makes the new sample type (T) the first of its kind
        String newCmoLabel = cmoLabelGeneratorService.generateCmoSampleLabel(
                updatedSample, existingSamples, DEFAULT_SAMPLES_BY_ALT_ID);
        Assertions.assertEquals("C-MP789JR-T001-d01", newCmoLabel);

        Status sampleStatus = cmoLabelGeneratorService.generateSampleStatus(
                updatedSample, existingSamples, DEFAULT_SAMPLES_BY_ALT_ID);
        Assertions.assertEquals(Boolean.TRUE, sampleStatus.getValidationStatus());
        Assertions.assertEquals(sampleStatus.getValidationReport(), (new HashMap()).toString());
    }

    /**
     * Tests if a new cmoLabel is properly generated for
     *     a sample with metadata with updated cmoPatientId and primaryId/igoId
     * Expected behavior: Should return a label with
     *     incremented sample count and new cmoPatientId
     * @throws Exception
     */
    @Test
    public void testCmoLabelGenForSampleWithPatientCorrection()
            throws Exception {
        // existing samples for patientId: C-MP789JR
        List<SampleMetadata> existingSamples =
                getPatientSamplesFromRequestJson("mockPublishedRequest1JsonDataWith2T2N", "C-MP789JR");

        // updated SampleMetadata
        SampleMetadata updatedSample = mapper.convertValue(
                existingSamples.get(0).clone(), SampleMetadata.class);
        updatedSample.setCmoPatientId("C-newPatient");
        updatedSample.setPrimaryId("newIgoId");

        // generate cmoLabel for sample with updates
        String newCmoLabel = cmoLabelGeneratorService.generateCmoSampleLabel(updatedSample,
                existingSamples, DEFAULT_SAMPLES_BY_ALT_ID);

        // if the cmo label of the existing sample is C-MP789JR-X001-d
        // then a new sample coming in of this same sample type should be given X002
        Assertions.assertEquals("C-newPatient-X002-d01", newCmoLabel);

        Status sampleStatus = cmoLabelGeneratorService.generateSampleStatus(
                updatedSample, existingSamples, DEFAULT_SAMPLES_BY_ALT_ID);
        Assertions.assertEquals(Boolean.TRUE, sampleStatus.getValidationStatus());
        Assertions.assertEquals(sampleStatus.getValidationReport(), (new HashMap()).toString());
    }

    /**
     * Tests if a new cmoLabel is properly generated for
     *     a sample with metadata with updated cmoPatientId and primaryId/igoId
     * Expected behavior: Should return a label with
     *     only a new cmoPatientId change
     * @throws Exception
     */
    @Test
    public void testCmoLabelGenForExistingSampleWithPatientCorrection()
            throws Exception {
        // existing samples for patientId: C-MP789JR
        List<SampleMetadata> existingSamples =
                getPatientSamplesFromRequestJson("mockPublishedRequest1JsonDataWith2T2N", "C-MP789JR");

        // updated SampleMetadata
        SampleMetadata updatedSample = mapper.convertValue(
                existingSamples.get(0).clone(), SampleMetadata.class);
        updatedSample.setCmoPatientId("C-newPatient");

        // generate cmoLabel for sample with updates
        String newCmoLabel = cmoLabelGeneratorService.generateCmoSampleLabel(updatedSample,
                existingSamples, DEFAULT_SAMPLES_BY_ALT_ID);

        // NOTE: now that the nucleic acid counter doesn't increment for unless it's
        // another sample of the same alt id, the 'updatedCmoLabel' returned should
        // have a nuc acid counter of 01

        // if the cmo label before the update is C-MP789JR-X001-d
        Assertions.assertEquals("C-newPatient-X001-d01", newCmoLabel);

        Status sampleStatus = cmoLabelGeneratorService.generateSampleStatus(
                updatedSample, existingSamples, DEFAULT_SAMPLES_BY_ALT_ID);
        Assertions.assertEquals(Boolean.TRUE, sampleStatus.getValidationStatus());
        Assertions.assertEquals(sampleStatus.getValidationReport(), (new HashMap()).toString());
    }

    /**
     * Tests that label generation fails as expected for sample with the following:
     * - sample type (cmo sample class) = other
     * - sample origin = whole blood
     * - sample class (specimen type) = other
     * Expected behavior: Should return null label.
     * @throws Exception
     */
    @Test
    public void testCmoLabelGenForSampleWithOtherSpecimenType()
            throws Exception {
        // existing samples for patientId: C-MP789JR
        List<SampleMetadata> existingSamples =
                getPatientSamplesFromRequestJson("mockPublishedRequest1JsonDataWith2T2N", "C-MP789JR");

        // updated SampleMetadata
        SampleMetadata updatedSample = mapper.convertValue(
                existingSamples.get(0).clone(), SampleMetadata.class);
        updatedSample.setSampleType("Other");
        updatedSample.setSampleOrigin("Whole Blood");
        updatedSample.setSampleClass("Other");

        // generate cmoLabel for sample with spec type (sample type) = other
        // should return label with 'F' sample type abbreviation and validaiton status = false
        String newCmoLabel = cmoLabelGeneratorService.generateCmoSampleLabel(updatedSample,
                existingSamples, DEFAULT_SAMPLES_BY_ALT_ID);
        Assertions.assertEquals("C-MP789JR-F001-d01", newCmoLabel);

        Status sampleStatus = cmoLabelGeneratorService.generateSampleStatus(
                updatedSample, existingSamples, DEFAULT_SAMPLES_BY_ALT_ID);
        Assertions.assertEquals(Boolean.FALSE, sampleStatus.getValidationStatus());
        Assertions.assertNotSame(sampleStatus.getValidationReport(), (new HashMap()).toString());
    }

    /**
     * Tests various sample updates for an incoming IGO sample manifest.
     */
    @Test
    public void testCmoCelllineLabelGenerationUpdates() {
        String requestId = "86793_T";
        List<SampleMetadata> existingSamples = new ArrayList<>();

        // generate reference cell line sample label and assert it matches the expected value
        IgoSampleManifest sample = getSampleMetadata("86793_T_4", "C-76767",
                SpecimenType.CELLLINE, NucleicAcid.DNA, "AMP1", "ABC-444");
        String sampleLabel =  cmoLabelGeneratorService.generateCmoSampleLabel(requestId,
                sample, existingSamples, DEFAULT_SAMPLES_BY_ALT_ID);
        String sampleExpectedLabel = "AMP1-86793T";
        Assertions.assertEquals("AMP1-86793T", sampleLabel);

        // test label generated with new investigator id and assert
        // that the sample would require a label update
        IgoSampleManifest sampleUpdatedInvestigatorId = getSampleMetadata("86793_T_4", "C-76767",
                SpecimenType.CELLLINE, NucleicAcid.DNA, "MIP2", "ABC-444");
        String sampleUpdatedInvestigatorIdLabel = cmoLabelGeneratorService.generateCmoSampleLabel(requestId,
                sampleUpdatedInvestigatorId, existingSamples, DEFAULT_SAMPLES_BY_ALT_ID);
        String expectedLabelWithInvestiagorIdUpdate = "MIP2-86793T";
        Assertions.assertEquals(expectedLabelWithInvestiagorIdUpdate, sampleUpdatedInvestigatorIdLabel);
        // assert that the sample would require a label update
        Assertions.assertTrue(cmoLabelGeneratorService.igoSampleRequiresLabelUpdate(
                sampleUpdatedInvestigatorIdLabel, sampleLabel));

        // test label generated with same investigator id as original sample
        // but with a different nucleic acid value
        IgoSampleManifest sampleUpdatedNaExtract = getSampleMetadata("86793_T_4", "C-76767",
                SpecimenType.CELLLINE, NucleicAcid.CFDNA, "AMP1", "ABC-444");
        String sampleUpdatedNaExtractLabel = cmoLabelGeneratorService.generateCmoSampleLabel(requestId,
                sampleUpdatedNaExtract, existingSamples, DEFAULT_SAMPLES_BY_ALT_ID);
        Assertions.assertEquals(sampleExpectedLabel, sampleUpdatedNaExtractLabel);
        Assertions.assertFalse(cmoLabelGeneratorService.igoSampleRequiresLabelUpdate(
                sampleUpdatedNaExtractLabel, sampleExpectedLabel));
    }

    /**
     * Tests 'F' resolved sample type abbreviation.
     */
    @Test
    public void testDefaultSampleTypeAbbreviation() {
        String sampleTypeAbbrev = cmoLabelGeneratorService.resolveSampleTypeAbbreviation("RapidAutopsy",
                "Cerebrospinal Fluid", "Other");
        Assertions.assertTrue(sampleTypeAbbrev.equals("F"));
    }

    /**
     * Tests the behavior of the nucleic acid increment method.
     * @throws Exception
     */
    @Test
    public void testIncrementNucleicAcidCounter() throws Exception {
        // test label increment when nuc acid counter isn't present
        String legacyLabel = "C-MP789JR-F001-d";
        String expectedIncrementLegacy = "C-MP789JR-F001-d02";
        String actualIncrementLegacy = cmoLabelGeneratorService.incrementNucleicAcidCounter(legacyLabel);
        Assertions.assertEquals(expectedIncrementLegacy, actualIncrementLegacy);

        // test increment with a non-legacy label
        String inputLabel = "C-MP789JR-F001-d01";
        String expectedLabel = "C-MP789JR-F001-d02";
        String actualLabel = cmoLabelGeneratorService.incrementNucleicAcidCounter(inputLabel);
        Assertions.assertEquals(expectedLabel, actualLabel);

        // test another increment
        String nextExpectedLabel = "C-MP789JR-F001-d03";
        String nextActualLabel = cmoLabelGeneratorService.incrementNucleicAcidCounter(expectedLabel);
        Assertions.assertEquals(nextExpectedLabel, nextActualLabel);
    }

    /**
     * Tests a new sample (new primary id) with an existing alt id and has the
     * same nucleic acid type as the samples matching the alt id.
     * Expected behavior: sample should have the same resolved tumor counter
     *   and an incremented nuc acid counter.
     * @throws Exception
     */
    @Test
    public void testNewSampleExistingAltIdDnaNucAcid() throws Exception {
        // existing samples for patientId: C-MP789JR
        List<SampleMetadata> existingSamples =
                getPatientSamplesFromRequestJson("mockPublishedRequest1JsonDataWith2T2N", "C-MP789JR");

        // new sample from same source sample tissue and dna profile
        SampleMetadata newSample1 = mapper.convertValue(
                existingSamples.get(0).clone(), SampleMetadata.class);
        newSample1.setPrimaryId("98755_B_1");
        newSample1.setSampleClass("Non-PDX");

        // if no samples exist by the same alt id and there are no existing samples of the same
        // resolved sample type abbreviation then the sample counter should be #1
        // note: the sample type abbreviations for the existing samples are X and N
        String cmoLabelNoAltIds = cmoLabelGeneratorService.generateCmoSampleLabel(newSample1,
                existingSamples, DEFAULT_SAMPLES_BY_ALT_ID);
        Assertions.assertEquals("C-MP789JR-T001-d01", cmoLabelNoAltIds);

        // set up samples by alt id as same as existing samples
        List<SampleMetadata> samplesByAltId =
                getPatientSamplesFromRequestJson("mockPublishedRequest1JsonDataWith2T2N", "C-MP789JR");

        // if there are samples by the same alt id then the new sample should receive tumor counter #1
        // and dna counter
        String cmoLabelWithAltIds = cmoLabelGeneratorService.generateCmoSampleLabel(newSample1,
                existingSamples, samplesByAltId);
        Assertions.assertEquals("C-MP789JR-T001-d02", cmoLabelWithAltIds);
        newSample1.setCmoSampleName(cmoLabelWithAltIds);
        samplesByAltId.add(newSample1);

        // if there's another new sample coming in with the same alt id, same dna profile then
        // the new label should have tumor counter #2 and dna counter #3
        SampleMetadata newSample2 = mapper.convertValue(
                existingSamples.get(0).clone(), SampleMetadata.class);
        newSample2.setPrimaryId("98989_C_4");
        newSample2.setSampleClass("Non-PDX");
        String cmoLabelWithAltIds2 = cmoLabelGeneratorService.generateCmoSampleLabel(newSample2,
                existingSamples, samplesByAltId);
        Assertions.assertEquals("C-MP789JR-T001-d03", cmoLabelWithAltIds2);
        newSample2.setCmoSampleName(cmoLabelWithAltIds2);
        samplesByAltId.add(newSample2);
    }

    /**
     * Simple test to make sure that changes to the sample counter are considered valid changes
     * for the cmo label that should result in an update to the database.
     * @throws Exception
     */
    @Test
    public void testChangeInSampleCounter() throws Exception {
        String origLabel = "C-VVF33N-P003-d";
        String newLabel = "C-VVF33N-P004-d01";
        Assertions.assertTrue(cmoLabelGeneratorService.igoSampleRequiresLabelUpdate(newLabel, origLabel));

        String labelWithoutSampleCountChange = "C-VVF33N-P003-d01";
        Assertions.assertFalse(cmoLabelGeneratorService.igoSampleRequiresLabelUpdate(
                labelWithoutSampleCountChange, origLabel));
    }

    @Test
    public void testNextConsecutiveCounter() throws Exception {
        Set<Integer> counters = new HashSet<>(Arrays.asList(1, 2, 7));
        Integer nextConsecutiveInt = getNextNucleicAcidIncrement(counters);
        Assertions.assertEquals(3, nextConsecutiveInt);
    }

    private Integer getNextNucleicAcidIncrement(Set<Integer> counters) {
        if (counters.isEmpty()) {
            return 1;
        }
        if (counters.size() == 1) {
            if (Collections.min(counters) != 1) {
                return 1;
            } else {
                return 2;
            }
        }

        List<Integer> sortedCounters = Arrays.asList(counters.toArray(Integer[]::new));
        Collections.sort(sortedCounters);

        Integer refCounter = Collections.min(counters);
        for (int i = 1; i < sortedCounters.size(); i++) {
            Integer currentCounter = sortedCounters.get(i);
            Integer prevCounter = sortedCounters.get(i - 1);
            if ((currentCounter - prevCounter) > 1) {
                return prevCounter + 1;
            } else {
                refCounter = currentCounter;
            }
        }
        return refCounter + 1;
    }

    @Test
    public void testAltIdUniqueBySampleTypeMonoSampleTypes() throws Exception {
        String primaryId = "12345_C_6";
        String sampleTypeAbbrev = "T";

        List<SampleMetadata> samplesByAltId = new ArrayList<>();
        samplesByAltId.add(getSampleWithPrimaryIdAndLabel("12345_B_3", "C-MPJKLE-P002-d"));
        samplesByAltId.add(getSampleWithPrimaryIdAndLabel("12345_F_4", "C-MPJKLE-P002-d02"));

        String resolvedSampleType = cmoLabelGeneratorService.resolveSampleTypeAbbrevWithContext(
                primaryId, sampleTypeAbbrev, samplesByAltId);
        Assertions.assertEquals("P", resolvedSampleType);
    }

    @Test
    public void testAltIdUniqueBySampleTypeMixedTumorSampleTypes() throws Exception {
        String primaryId = "12345_C_6";
        String sampleTypeAbbrev = "T";

        List<SampleMetadata> samplesByAltId = new ArrayList<>();
        samplesByAltId.add(getSampleWithPrimaryIdAndLabel("12345_B_3", "C-MPJKLE-P002-d"));
        samplesByAltId.add(getSampleWithPrimaryIdAndLabel("12345_F_4", "C-MPJKLE-R002-d02"));

        String resolvedSampleType = cmoLabelGeneratorService.resolveSampleTypeAbbrevWithContext(
                primaryId, sampleTypeAbbrev, samplesByAltId);
        Assertions.assertEquals("T", resolvedSampleType);
    }

    @Test
    public void testAltIdUniqueBySampleTypeMixedTumorSampleTypesAndMatchingPrimaryId() throws Exception {
        String primaryId = "12345_C_6";
        String sampleTypeAbbrev = "T";

        List<SampleMetadata> samplesByAltId = new ArrayList<>();
        samplesByAltId.add(getSampleWithPrimaryIdAndLabel("12345_B_3", "C-MPJKLE-P002-d"));
        samplesByAltId.add(getSampleWithPrimaryIdAndLabel("12345_F_4", "C-MPJKLE-R002-d02"));
        samplesByAltId.add(getSampleWithPrimaryIdAndLabel("12345_C_6", "C-MPJKLE-M002-d02"));

        String resolvedSampleType = cmoLabelGeneratorService.resolveSampleTypeAbbrevWithContext(
                primaryId, sampleTypeAbbrev, samplesByAltId);
        Assertions.assertEquals("M", resolvedSampleType);
    }

    @Test
    public void testAltIdUniqueBySampleTypeComplexMixedTumorTypesAndNoMatchingPrimaryId() throws Exception {
        String primaryId = "12345_C_6";
        String sampleTypeAbbrev = "L";

        List<SampleMetadata> samplesByAltId = new ArrayList<>();
        samplesByAltId.add(getSampleWithPrimaryIdAndLabel("12345_B_3", "C-MPJKLE-P002-d"));
        samplesByAltId.add(getSampleWithPrimaryIdAndLabel("12345_F_4", "C-MPJKLE-R002-d02"));

        String resolvedSampleType = cmoLabelGeneratorService.resolveSampleTypeAbbrevWithContext(
                primaryId, sampleTypeAbbrev, samplesByAltId);
        Assertions.assertEquals("L", resolvedSampleType);
    }

    @Test
    public void testAltIdUniqueBySampleTypeComplexMixedTumorTypesAndMatchingPrimaryId() throws Exception {
        String primaryId = "12345_C_6";
        String sampleTypeAbbrev = "L";

        List<SampleMetadata> samplesByAltId = new ArrayList<>();
        samplesByAltId.add(getSampleWithPrimaryIdAndLabel("12345_B_3", "C-MPJKLE-P002-d"));
        samplesByAltId.add(getSampleWithPrimaryIdAndLabel("12345_F_4", "C-MPJKLE-R002-d02"));
        samplesByAltId.add(getSampleWithPrimaryIdAndLabel("12345_C_6", "C-MPJKLE-N002-d02"));

        String resolvedSampleType = cmoLabelGeneratorService.resolveSampleTypeAbbrevWithContext(
                primaryId, sampleTypeAbbrev, samplesByAltId);
        Assertions.assertEquals("L", resolvedSampleType);
    }

    @Test
    public void testAltIdUniqueBySampleTypeNoSamplesByAltId() throws Exception {
        String primaryId = "12345_C_6";
        String sampleTypeAbbrev = "L";

        String resolvedSampleType = cmoLabelGeneratorService.resolveSampleTypeAbbrevWithContext(
                primaryId, sampleTypeAbbrev, DEFAULT_SAMPLES_BY_ALT_ID);
        Assertions.assertEquals("L", resolvedSampleType);
    }

    @Test
    public void testLabelDoesNotRequireUpdate() throws Exception {
        String newLabel = "C-MPJKLE-T002-d01";
        String existingLabel = "C-MPJKLE-P002-d";
        Boolean requiresUpdate = cmoLabelGeneratorService.igoSampleRequiresLabelUpdate(newLabel,
                existingLabel);
        Assertions.assertFalse(requiresUpdate);
    }

    @Test
    public void testLabelRequiresUpdate() throws Exception {
        String newLabel = "C-MPJKLE-N002-d01";
        String existingLabel = "C-MPJKLE-P002-d";
        Boolean requiresUpdate = cmoLabelGeneratorService.igoSampleRequiresLabelUpdate(newLabel,
                existingLabel);
        Assertions.assertTrue(requiresUpdate);
    }

    private SampleMetadata getSampleWithPrimaryIdAndLabel(String primaryId, String cmoLabel) {
        SampleMetadata s = new SampleMetadata();
        s.setPrimaryId(primaryId);
        s.setCmoSampleName(cmoLabel);
        return s;
    }

    private IgoSampleManifest getSampleMetadata(String igoId, String cmoPatientId,
            SpecimenType specimenType, NucleicAcid naToExtract, String investigatorSampleId, String altId) {
        IgoSampleManifest sample = new IgoSampleManifest();
        sample.setIgoId(igoId);
        sample.setCmoPatientId(cmoPatientId);
        sample.setSpecimenType(specimenType.getValue());
        sample.setInvestigatorSampleId(investigatorSampleId);
        sample.setCmoSampleClass("Tumor");
        sample.setAltid(altId);

        Map<String, String> cmoSampleIdFields = new HashMap<>();
        cmoSampleIdFields.put("naToExtract", naToExtract.getValue());
        cmoSampleIdFields.put("normalizedPatientId", cmoPatientId);
        sample.setCmoSampleIdFields(cmoSampleIdFields);
        return sample;
    }

    private List<SampleMetadata> getPatientSamplesFromRequestJson(String mockedRequestId,
            String patientId) throws JsonProcessingException {
        MockJsonTestData requestJson = mockedRequestJsonDataMap
                .get(mockedRequestId);
        Map<String, String> mappedRequestJson = mapper.readValue(requestJson.getJsonString(), Map.class);
        SampleMetadata[] samples = mapper.convertValue(mappedRequestJson.get("samples"),
                SampleMetadata[].class);

        List<SampleMetadata> existingSamples = new ArrayList<>();
        for (SampleMetadata sample : samples) {
            if (sample.getCmoPatientId().equals(patientId)) {
                existingSamples.add(sample);
            }
        }
        return existingSamples;
    }
}

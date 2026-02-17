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
import java.util.Objects;
import java.util.Set;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mskcc.smile.commons.enums.NucleicAcid;
import org.mskcc.smile.commons.enums.SpecimenType;
import org.mskcc.smile.config.TestConfiguration;
import org.mskcc.smile.model.MockJsonTestData;
import org.mskcc.smile.service.CmoLabelGeneratorService;
import org.mskcc.smile.service.util.CmoLabelParts;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest(classes = LabelGeneratorTestApp.class)
@Import(TestConfiguration.class)
public class CmoLabelGeneratorServiceTest {
    private final ObjectMapper mapper = new ObjectMapper();

    // setting as empty for now until alt id is fully supported
    List<CmoLabelParts> DEFAULT_SAMPLES_BY_ALT_ID = new ArrayList<>();

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
        List<CmoLabelParts> existingSamples =
                getPatientSamplesFromRequestJson("mockPublishedRequest1JsonDataWith2T2N", "C-MP789JR");

        // updated SampleMetadata
        CmoLabelParts updatedSample = mapper.convertValue(
                existingSamples.get(0).clone(), CmoLabelParts.class);
        updatedSample.setSampleClass("Non-PDX");
        updatedSample.setPrimaryId("newPrimaryId");

        // generate cmoLabel for new incoming sample with no matching samples by alt id
        // note: the sample type abbreviations for the existing patient samples are X and N
        // which makes the new sample type (T) the first of its kind
        String newCmoLabel = cmoLabelGeneratorService.generateCmoSampleLabel(
                updatedSample, existingSamples, DEFAULT_SAMPLES_BY_ALT_ID);
        Assertions.assertEquals("C-MP789JR-T001-d01", newCmoLabel);

        Map<String, Object> sampleStatus = cmoLabelGeneratorService.generateSampleStatus(
                updatedSample, existingSamples, DEFAULT_SAMPLES_BY_ALT_ID);
        Assertions.assertEquals(Boolean.TRUE, (Boolean) sampleStatus.get("validationStatus"));
        Assertions.assertEquals("{}", sampleStatus.get("validationReport").toString());
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
        List<CmoLabelParts> existingSamples =
                getPatientSamplesFromRequestJson("mockPublishedRequest1JsonDataWith2T2N", "C-MP789JR");

        // updated SampleMetadata
        CmoLabelParts updatedSample = mapper.convertValue(
                existingSamples.get(0).clone(), CmoLabelParts.class);
        updatedSample.setCmoPatientId("C-newPatient");
        updatedSample.setPrimaryId("newIgoId");

        // generate cmoLabel for sample with updates
        String newCmoLabel = cmoLabelGeneratorService.generateCmoSampleLabel(updatedSample,
                existingSamples, DEFAULT_SAMPLES_BY_ALT_ID);

        // if the cmo label of the existing sample is C-MP789JR-X001-d
        // then a new sample coming in of this same sample type should be given X002
        Assertions.assertEquals("C-newPatient-X002-d01", newCmoLabel);

        Map<String, Object> sampleStatus = cmoLabelGeneratorService.generateSampleStatus(
                updatedSample, existingSamples, DEFAULT_SAMPLES_BY_ALT_ID);
        Assertions.assertEquals(Boolean.TRUE, (Boolean) sampleStatus.get("validationStatus"));
        Assertions.assertEquals("{}", sampleStatus.get("validationReport").toString());
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
        List<CmoLabelParts> existingSamples =
                getPatientSamplesFromRequestJson("mockPublishedRequest1JsonDataWith2T2N", "C-MP789JR");

        // updated SampleMetadata
        CmoLabelParts updatedSample = mapper.convertValue(
                existingSamples.get(0).clone(), CmoLabelParts.class);
        updatedSample.setCmoPatientId("C-newPatient");

        // generate cmoLabel for sample with updates
        String newCmoLabel = cmoLabelGeneratorService.generateCmoSampleLabel(updatedSample,
                existingSamples, DEFAULT_SAMPLES_BY_ALT_ID);

        // NOTE: now that the nucleic acid counter doesn't increment for unless it's
        // another sample of the same alt id, the 'updatedCmoLabel' returned should
        // have a nuc acid counter of 01

        // if the cmo label before the update is C-MP789JR-X001-d
        Assertions.assertEquals("C-newPatient-X001-d01", newCmoLabel);

        Map<String, Object> sampleStatus = cmoLabelGeneratorService.generateSampleStatus(
                updatedSample, existingSamples, DEFAULT_SAMPLES_BY_ALT_ID);
        Assertions.assertEquals(Boolean.TRUE, (Boolean) sampleStatus.get("validationStatus"));
        Assertions.assertEquals("{}", sampleStatus.get("validationReport").toString());
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
        List<CmoLabelParts> existingSamples =
                getPatientSamplesFromRequestJson("mockPublishedRequest1JsonDataWith2T2N", "C-MP789JR");

        // updated SampleMetadata
        CmoLabelParts updatedSample = mapper.convertValue(
                existingSamples.get(0).clone(), CmoLabelParts.class);
        updatedSample.setSampleType("Other");
        updatedSample.setSampleOrigin("Whole Blood");
        updatedSample.setSampleClass("Other");

        // generate cmoLabel for sample with spec type (sample type) = other
        // should return label with 'F' sample type abbreviation and validaiton status = false
        String newCmoLabel = cmoLabelGeneratorService.generateCmoSampleLabel(updatedSample,
                existingSamples, DEFAULT_SAMPLES_BY_ALT_ID);
        Assertions.assertEquals("C-MP789JR-F001-d01", newCmoLabel);

        Map<String, Object> sampleStatus = cmoLabelGeneratorService.generateSampleStatus(
                updatedSample, existingSamples, DEFAULT_SAMPLES_BY_ALT_ID);
        Assertions.assertEquals(Boolean.FALSE, (Boolean) sampleStatus.get("validationStatus"));
        Assertions.assertNotSame("{}", sampleStatus.get("validationReport").toString());
    }

    /**
     * Tests various sample updates for an incoming IGO sample manifest.
     * @throws com.fasterxml.jackson.core.JsonProcessingException
     */
    @Test
    public void testCmoCelllineLabelGenerationUpdates() throws JsonProcessingException {
        String requestId = "86793_T";
        List<CmoLabelParts> existingSamples = new ArrayList<>();

        // generate reference cell line sample label and assert it matches the expected value
        CmoLabelParts sample = initIgoSampleLabelParts("86793_T_4", "C-76767",
                SpecimenType.CELLLINE, NucleicAcid.DNA, "AMP1", "ABC-444", requestId,
                "Tumor", Boolean.TRUE);
        String sampleLabel =  cmoLabelGeneratorService.generateCmoSampleLabel(sample,
                existingSamples, DEFAULT_SAMPLES_BY_ALT_ID);
        String sampleExpectedLabel = "AMP1-86793T";
        Assertions.assertEquals("AMP1-86793T", sampleLabel);

        // test label generated with new investigator id and assert
        // that the sample would require a label update
        CmoLabelParts sampleUpdatedInvestigatorId = initIgoSampleLabelParts("86793_T_4", "C-76767",
                SpecimenType.CELLLINE, NucleicAcid.DNA, "MIP2", "ABC-444", requestId,
                "Tumor", Boolean.TRUE);
        String sampleUpdatedInvestigatorIdLabel
                = cmoLabelGeneratorService.generateCmoSampleLabel(sampleUpdatedInvestigatorId,
                        existingSamples, DEFAULT_SAMPLES_BY_ALT_ID);
        String expectedLabelWithInvestiagorIdUpdate = "MIP2-86793T";
        Assertions.assertEquals(expectedLabelWithInvestiagorIdUpdate, sampleUpdatedInvestigatorIdLabel);
        // assert that the sample would require a label update
        Assertions.assertTrue(cmoLabelGeneratorService.igoSampleRequiresLabelUpdate(
                sampleUpdatedInvestigatorIdLabel, sampleLabel));

        // test label generated with same investigator id as original sample
        // but with a different nucleic acid value
        CmoLabelParts sampleUpdatedNaExtract = initIgoSampleLabelParts("86793_T_4", "C-76767",
                SpecimenType.CELLLINE, NucleicAcid.CFDNA, "AMP1", "ABC-444", requestId,
                "Tumor", Boolean.TRUE);
        String sampleUpdatedNaExtractLabel
                = cmoLabelGeneratorService.generateCmoSampleLabel(sampleUpdatedNaExtract,
                        existingSamples, DEFAULT_SAMPLES_BY_ALT_ID);
        Assertions.assertEquals(sampleExpectedLabel, sampleUpdatedNaExtractLabel);
        Assertions.assertFalse(cmoLabelGeneratorService.igoSampleRequiresLabelUpdate(
                sampleUpdatedNaExtractLabel, sampleExpectedLabel));
    }

    /**
     * Tests 'F' resolved sample type abbreviation.
     * @throws com.fasterxml.jackson.core.JsonProcessingException
     */
    @Test
    public void testDefaultSampleTypeAbbreviation() throws JsonProcessingException {
        Map<String, Object> sampleMap = new HashMap<>();
        sampleMap.put("specimenType", "RapidAutopsy");
        sampleMap.put("sampleOrigin", "Cerebrospinal Fluid");
        sampleMap.put("cmoSampleClass", "Other");

        Map<String, Object> cmoSampleIdFields = new HashMap<>();
        cmoSampleIdFields.put("sampleType", null);
        sampleMap.put("cmoSampleIdFields", cmoSampleIdFields);

        CmoLabelParts sampleLabelParts = new CmoLabelParts(sampleMap, null, null);
        String sampleTypeAbbrev = cmoLabelGeneratorService.resolveSampleTypeAbbreviation(sampleLabelParts);
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
     * Tests the behavior of the sample counter increment method.
     * @throws Exception
     */
    @Test
    public void testIncrementSampleCounter() throws Exception {
        // test label increment when nuc acid counter isn't present
        String legacyLabel = "C-MP789JR-F001-d";
        String expectedIncrementLegacy = "C-MP789JR-F002-d";
        String actualIncrementLegacy = cmoLabelGeneratorService.incrementSampleCounter(legacyLabel);
        Assertions.assertEquals(expectedIncrementLegacy, actualIncrementLegacy);

        // test increment with a non-legacy label
        String inputLabel = "C-MP789JR-F001-d01";
        String expectedLabel = "C-MP789JR-F002-d01";
        String actualLabel = cmoLabelGeneratorService.incrementSampleCounter(inputLabel);
        Assertions.assertEquals(expectedLabel, actualLabel);

        // test another increment
        String nextExpectedLabel = "C-MP789JR-F003-d01";
        String nextActualLabel = cmoLabelGeneratorService.incrementSampleCounter(expectedLabel);
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
        List<CmoLabelParts> existingSamples =
                getPatientSamplesFromRequestJson("mockPublishedRequest1JsonDataWith2T2N", "C-MP789JR");

        // new sample from same source sample tissue and dna profile
        CmoLabelParts newSample1 = mapper.convertValue(
                existingSamples.get(0).clone(), CmoLabelParts.class);
        newSample1.setPrimaryId("98755_B_1");
        newSample1.setSampleClass("Non-PDX");

        // if no samples exist by the same alt id and there are no existing samples of the same
        // resolved sample type abbreviation then the sample counter should be #1
        // note: the sample type abbreviations for the existing samples are X and N
        String cmoLabelNoAltIds = cmoLabelGeneratorService.generateCmoSampleLabel(newSample1,
                existingSamples, DEFAULT_SAMPLES_BY_ALT_ID);
        Assertions.assertEquals("C-MP789JR-T001-d01", cmoLabelNoAltIds);

        // set up samples by alt id as same as existing samples
        List<CmoLabelParts> samplesByAltId = new ArrayList<>();
        samplesByAltId.add(getSampleWithPrimaryIdAndLabel("08944_B_1", "C-MP789JR-X001-d"));

        // if there are samples by the same alt id then the new sample should receive tumor counter #1
        // and dna counter
        String cmoLabelWithAltIds = cmoLabelGeneratorService.generateCmoSampleLabel(newSample1,
                existingSamples, samplesByAltId);
        Assertions.assertEquals("C-MP789JR-T001-d01", cmoLabelWithAltIds);
        newSample1.setCmoSampleName(cmoLabelWithAltIds);
        samplesByAltId.add(newSample1);

        // if there's another new sample coming in with the same alt id, same dna profile then
        // the new label should have tumor counter #2 and dna counter #3
        CmoLabelParts newSample2 = mapper.convertValue(
                existingSamples.get(0).clone(), CmoLabelParts.class);
        newSample2.setPrimaryId("98989_C_4");
        newSample2.setSampleClass("Non-PDX");
        String cmoLabelWithAltIds2 = cmoLabelGeneratorService.generateCmoSampleLabel(newSample2,
                existingSamples, samplesByAltId);
        Assertions.assertEquals("C-MP789JR-T001-d02", cmoLabelWithAltIds2);
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
        Integer nextConsecutiveInt1 = getNextNucleicAcidIncrement(counters, null);
        Assertions.assertEquals(3, nextConsecutiveInt1);

        Integer nextConsecutiveInt2 = getNextNucleicAcidIncrement(counters, 2);
        Assertions.assertEquals(2, nextConsecutiveInt2);
    }

    private Integer getNextNucleicAcidIncrement(Set<Integer> counters, Integer existingNucAcidCounter) {
        if (counters.isEmpty() || Collections.min(counters) != 1) {
            return 1;
        }

        List<Integer> sortedCounters = Arrays.asList(counters.toArray(Integer[]::new));
        Collections.sort(sortedCounters);

        Integer refCounter = Collections.min(counters);
        for (int i = 1; i < sortedCounters.size(); i++) {
            Integer currentCounter = sortedCounters.get(i);
            Integer prevCounter = sortedCounters.get(i - 1);

            // if the difference between the counters is > 1 then return the prev counter + 1
            if ((currentCounter - prevCounter) > 1) {
                return prevCounter + 1;
            }

            // if the current counter matches the existing nuc acid counter
            // then return since the current counter is +1 from the prev counter
            // and therefore is already the next consecutive integer
            if (existingNucAcidCounter != null && Objects.equals(existingNucAcidCounter, currentCounter)) {
                return existingNucAcidCounter;
            }

            // move onto the next counter in the list
            refCounter = currentCounter;
        }
        return refCounter + 1;
    }

    @Test
    public void testAltIdUniqueBySampleTypeMonoSampleTypes() throws Exception {
        String primaryId = "12345_C_6";
        String sampleTypeAbbrev = "T";

        List<CmoLabelParts> samplesByAltId = new ArrayList<>();
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

        List<CmoLabelParts> samplesByAltId = new ArrayList<>();
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

        List<CmoLabelParts> samplesByAltId = new ArrayList<>();
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

        List<CmoLabelParts> samplesByAltId = new ArrayList<>();
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

        List<CmoLabelParts> samplesByAltId = new ArrayList<>();
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

    @Test
    public void testSampleTypeFLabelRequiresUpdateChecks() throws Exception {
        // existing label for comparison
        String existingLabel = "C-MPJKLE-F002-d01";

        // does not require an update
        String newLabel1 = "C-MPJKLE-F001-d01";
        Boolean requiresUpdate1 = cmoLabelGeneratorService.igoSampleRequiresLabelUpdate(newLabel1,
                existingLabel);
        Assertions.assertFalse(requiresUpdate1);

        // requires an update because sample type T is a meaningful change
        String newLabel2 = "C-MPJKLE-T001-d01";
        Boolean requiresUpdate2 = cmoLabelGeneratorService.igoSampleRequiresLabelUpdate(newLabel2,
                existingLabel);
        Assertions.assertTrue(requiresUpdate2);

        // requires an update because cmo patient id change is meaningful even though sample types are both F
        String newLabel3 = "C-ABCDEF-F001-d01";
        Boolean requiresUpdate3 = cmoLabelGeneratorService.igoSampleRequiresLabelUpdate(newLabel3,
                existingLabel);
        Assertions.assertTrue(requiresUpdate3);
    }

    @Test
    public void testUpdateToSampleType() throws Exception {
        CmoLabelParts sample1ExistingData = initSmileSampleLabelParts("SAMPLE_A_1", "C-BRC0DE-F001-d01",
                "C-BRC0DE", "ABC-123", "", "Non-PDX", NucleicAcid.DNA, "Tissue", null, "SAMPLE_A",
                "Tumor", Boolean.TRUE);
        CmoLabelParts sample2ExistingData = initSmileSampleLabelParts("SAMPLE_A_4", "C-BRC0DE-F002-d01",
                "C-BRC0DE", "ABC-456", "", "Non-PDX", NucleicAcid.DNA, "Tissue", null, "SAMPLE_A",
                "Tumor", Boolean.TRUE);
        List<CmoLabelParts> samplesByAltId123 = Arrays.asList(sample1ExistingData);
        List<CmoLabelParts> existingPatientSamples
                = Arrays.asList(sample1ExistingData, sample2ExistingData);

        // say that sample 1 gets an update that corrects its missing sample type with 'Unknown Tumor'
        // --> this should generate the label: C-BRC0DE-T001-d01
        CmoLabelParts sample1UpdatedData = initSmileSampleLabelParts("SAMPLE_A_1", "", "C-BRC0DE", "ABC-123",
                "Unknown Tumor", "Non-PDX", NucleicAcid.DNA, "Tissue", null, "SAMPLE_A",
                "Tumor", Boolean.TRUE);
        Assertions.assertTrue(cmoLabelGeneratorService.sampleHasLabelSpecificUpdates(sample1UpdatedData,
                existingPatientSamples));
        String sample1UpdatedLabel = cmoLabelGeneratorService.generateCmoSampleLabel(sample1UpdatedData,
                existingPatientSamples, samplesByAltId123);
        Assertions.assertEquals("C-BRC0DE-T001-d01", sample1UpdatedLabel);
        sample1UpdatedData.setCmoSampleName(sample1UpdatedLabel);
        List<CmoLabelParts> updatedPatientSamples = Arrays.asList(sample1UpdatedData, sample2ExistingData);

        // now say that sample 2 gets an update that corrects its missing sample type with 'Normal'
        // --> this should generate the label C-BRC0DE-N001-d01
        List<CmoLabelParts> samplesByAltId456 = Arrays.asList(sample2ExistingData);
        CmoLabelParts sample2UpdatedData = initSmileSampleLabelParts("SAMPLE_A_4", "", "C-BRC0DE", "ABC-456",
                "Normal", "Non-PDX", NucleicAcid.DNA, "Whole Blood", null, "SAMPLE_A",
                "Normal", Boolean.TRUE);
        Assertions.assertTrue(cmoLabelGeneratorService.sampleHasLabelSpecificUpdates(sample2UpdatedData,
                updatedPatientSamples));
        String sample2UpdatedLabel = cmoLabelGeneratorService.generateCmoSampleLabel(sample2UpdatedData,
                updatedPatientSamples, samplesByAltId456);
        Assertions.assertEquals("C-BRC0DE-N001-d01", sample2UpdatedLabel);
        sample2UpdatedData.setCmoSampleName(sample2UpdatedLabel);

        // now say that there is an additional tumor sample for this patient with a different ALT ID and
        // there are no existing samples in smile by the
        // same ALT ID --> we would expect the label to be: C-BRC0DE-T002-d01
        List<CmoLabelParts> newUpdatedPatientSamples
                = Arrays.asList(sample1UpdatedData, sample2UpdatedData);
        CmoLabelParts newTumorSample = initSmileSampleLabelParts("SAMPLE_B_1", "", "C-BRC0DE", "ABC-789",
                "Unknown Tumor", "Non-PDX", NucleicAcid.DNA, "Tissue", null,  "SAMPLE_B",
                "Tumor", Boolean.TRUE);
        Assertions.assertTrue(cmoLabelGeneratorService.sampleHasLabelSpecificUpdates(newTumorSample,
                newUpdatedPatientSamples));
        String newTumorLabel = cmoLabelGeneratorService.generateCmoSampleLabel(newTumorSample,
                newUpdatedPatientSamples, DEFAULT_SAMPLES_BY_ALT_ID);
        Assertions.assertEquals("C-BRC0DE-T002-d01", newTumorLabel);
    }

    /**
     * Tests various combinations of sample type, sample class, and sample origin still resolve
     * to expected sample type abbreviations following updates to the logic that applies to cfDNA samples.
     * @throws Exception
     */
    @Test
    public void testExpandedCellFreeDnaRules() throws Exception {
        // sample type abbreviation expected to resolve to L
        CmoLabelParts sm1 = initSmileSampleLabelParts("12345_C_7", "", "C-BRCD03", "ABF-89D",
                "Unknown Tumor", "cfDNA", NucleicAcid.DNA, "Plasma", "cfDNA", "12345_C",
                "Tumor", Boolean.TRUE);
        String label1 = cmoLabelGeneratorService.generateCmoSampleLabel(sm1,
                DEFAULT_SAMPLES_BY_ALT_ID, DEFAULT_SAMPLES_BY_ALT_ID);
        Assertions.assertEquals("C-BRCD03-L001-d01", label1);

        // sample type abbreviation expected to resolve to L
        CmoLabelParts sm2 = initSmileSampleLabelParts("12345_C_7", "", "C-BRCD03", "ABF-89D",
                "cfDNA", "Non-PDX", NucleicAcid.DNA, "Whole Blood", "cfDNA", "12345_C",
                "Tumor", Boolean.TRUE);
        String label2 = cmoLabelGeneratorService.generateCmoSampleLabel(sm2,
                DEFAULT_SAMPLES_BY_ALT_ID, DEFAULT_SAMPLES_BY_ALT_ID);
        Assertions.assertEquals("C-BRCD03-L001-d01", label2);

        // sample type abbreviation expected to resolve to F
        CmoLabelParts sm3 = initSmileSampleLabelParts("12345_C_7", "", "C-BRCD03", "ABF-89D",
                "cfDNA", "Non-PDX", NucleicAcid.DNA, "",  "cfDNA", "12345_C", "Tumor", Boolean.TRUE);
        String label3 = cmoLabelGeneratorService.generateCmoSampleLabel(sm3,
                DEFAULT_SAMPLES_BY_ALT_ID, DEFAULT_SAMPLES_BY_ALT_ID);
        Assertions.assertEquals("C-BRCD03-F001-d01", label3);

        // sample type abbreviation expected to resolve to U
        CmoLabelParts sm4 = initSmileSampleLabelParts("12345_C_7", "", "C-BRCD03", "ABF-89D",
                "Unknown Tumor", "Non-PDX", NucleicAcid.DNA, "Urine",  "cfDNA", "12345_C",
                "Tumor", Boolean.TRUE);
        String label4 = cmoLabelGeneratorService.generateCmoSampleLabel(sm4,
                DEFAULT_SAMPLES_BY_ALT_ID, DEFAULT_SAMPLES_BY_ALT_ID);
        Assertions.assertEquals("C-BRCD03-U001-d01", label4);

        // sample type abbreviation expected to resolve to T
        CmoLabelParts sm5 = initSmileSampleLabelParts("12345_C_7", "", "C-BRCD03", "ABF-89D",
                "Unknown Tumor", "Non-PDX", NucleicAcid.DNA, "",  "", "12345_C",
                "Tumor", Boolean.TRUE);
        String label5 = cmoLabelGeneratorService.generateCmoSampleLabel(sm5,
                DEFAULT_SAMPLES_BY_ALT_ID, DEFAULT_SAMPLES_BY_ALT_ID);
        Assertions.assertEquals("C-BRCD03-T001-d01", label5);

        // sample type abbreviation expected to resolve to F
        CmoLabelParts sm6 = initSmileSampleLabelParts("12345_C_7", "", "C-BRCD03", "ABF-89D",
                "Unknown Tumor", "Non-PDX", NucleicAcid.DNA, "",  "cfDNA", "12345_C",
                "Tumor", Boolean.TRUE);
        String label6 = cmoLabelGeneratorService.generateCmoSampleLabel(sm6,
                DEFAULT_SAMPLES_BY_ALT_ID, DEFAULT_SAMPLES_BY_ALT_ID);
        Assertions.assertEquals("C-BRCD03-L001-d01", label6);
    }

    /**
     * Tests if sample has label specific updates that would warrant running the label generator.
     * @throws Exception
     */
    @Test
    public void testNoApplicableMetadataUpdates() throws Exception {
        CmoLabelParts sm1 = initSmileSampleLabelParts("12345_C_7", "C-BRCD03-L001-d", "C-BRCD03", "ABF-89D",
                "Unknown Tumor", "cfDNA", NucleicAcid.DNA, "Plasma", "cfDNA", "12345_C",
                "Tumor", Boolean.TRUE);
        CmoLabelParts sm1Updated = initSmileSampleLabelParts("12345_C_7", "C-BRCD03-L001-d01", "C-BRCD03",
                "ABF-89D", "Unknown Tumor", "cfDNA", NucleicAcid.DNA, "Plasma", "cfDNA", "12345_C",
                "Tumor", Boolean.TRUE);
        // labels should be ignored during comparison
        Assertions.assertFalse(
                cmoLabelGeneratorService.sampleHasLabelSpecificUpdates(sm1Updated, Arrays.asList(sm1)));
    }

    /**
     * Tests if sample has label specific updates that would warrant running the label generator.
     * @throws Exception
     */
    @Test
    public void testHasApplicableMetadataUpdates() throws Exception {
        CmoLabelParts sm1 = initSmileSampleLabelParts("12345_C_7", "C-BRCD03-L001-d", "C-BRCD03", "ABF-89D",
                "Normal", "cfDNA", NucleicAcid.DNA, "Plasma", "cfDNA", "12345_C", "Normal", Boolean.TRUE);
        CmoLabelParts sm1Updated = initSmileSampleLabelParts("12345_C_7", "C-BRCD03-L001-d01", "C-BRCD03",
                "ABF-89D", "Unknown Tumor", "cfDNA", NucleicAcid.DNA, "Plasma", "cfDNA", "12345_C",
                "Tumor", Boolean.TRUE);
        // labels should be ignored during comparison
        Assertions.assertTrue(
                cmoLabelGeneratorService.sampleHasLabelSpecificUpdates(sm1Updated, Arrays.asList(sm1)));
    }

    /**
     * Tests sample labels resolved for dual extraction cases.
     * @throws Exception
     */
    @Test
    public void testDualExtractionSample() throws Exception {
        CmoLabelParts sm1 = initSmileSampleLabelParts("12345_C_7", "", "C-BRCD03", "ABF-89D",
                "Normal", "", NucleicAcid.DNA_AND_RNA, "", "", "12345_C", "Normal", Boolean.TRUE);
        sm1.setNormalizedPatientId("MRN_REDACTED"); // make this a non-cellline sample
        String actualLabel1 = cmoLabelGeneratorService.generateCmoSampleLabel(sm1,
                DEFAULT_SAMPLES_BY_ALT_ID, DEFAULT_SAMPLES_BY_ALT_ID);
        sm1.setCmoSampleName(actualLabel1);
        Assertions.assertEquals("C-BRCD03-N001-d01", actualLabel1);

        // smaple with same patient as sm1
        CmoLabelParts sm2 = initSmileSampleLabelParts("12345_C_9", "", "C-BRCD03", "AFB-8D9",
                "Normal", "", NucleicAcid.DNA_AND_RNA, "", "", "12345_C", "Normal", Boolean.TRUE);
        sm2.setNormalizedPatientId("MRN_REDACTED"); // make this a non-cellline sample
        String actualLabel2 = cmoLabelGeneratorService.generateCmoSampleLabel(sm2,
                Arrays.asList(sm1), DEFAULT_SAMPLES_BY_ALT_ID);
        sm2.setCmoSampleName(actualLabel2);
        Assertions.assertEquals("C-BRCD03-N002-d01", actualLabel2);

        // sample with same alt id as sm2
        CmoLabelParts sm2a = initSmileSampleLabelParts("12345_C_11", "", "C-BRCD03", "AFB-8D9",
                "Normal", "", NucleicAcid.DNA_AND_RNA, "", "", "12345_C", "Normal", Boolean.TRUE);
        sm2a.setNormalizedPatientId("MRN_REDACTED"); // make this a non-cellline sample
        String actualLabel2a = cmoLabelGeneratorService.generateCmoSampleLabel(sm2a,
                Arrays.asList(sm1, sm2), Arrays.asList(sm2));
        sm2a.setCmoSampleName(actualLabel2);
        Assertions.assertEquals("C-BRCD03-N002-d02", actualLabel2a);

        // sample with same alt id as above but is a cell line sample
        CmoLabelParts sm3 = initSmileSampleLabelParts("12345_C_13", "", "C-BRCD03", "AXF-97D", "Normal",
                "CellLine", NucleicAcid.DNA_AND_RNA, "Plasma", "", "12345_C", "Normal", Boolean.TRUE);
        sm3.setInvestigatorSampleId("CELLLINESAMPLE-123");
        String actualLabel3 = cmoLabelGeneratorService.generateCmoSampleLabel(sm3,
                Arrays.asList(sm1, sm2, sm2a), DEFAULT_SAMPLES_BY_ALT_ID);
        sm3.setCmoSampleName(actualLabel3);
        Assertions.assertEquals("CELLLINESAMPLE-123-12345C", actualLabel3);
    }

    /**
     * Tests if sample has label specific updates that would warrant running the label generator.
     * @throws Exception
     */
    @Test
    public void testHasApplicableMetadataUpdatesToIsCmoSampleStatus() throws Exception {
        CmoLabelParts sm1 = initSmileSampleLabelParts("12345_C_7", "", "C-BRCD03", "ABF-89D",
                "Normal", "cfDNA", NucleicAcid.DNA, "Plasma", "cfDNA", "12345_C", "Normal", Boolean.FALSE);
        CmoLabelParts sm1Updated = initSmileSampleLabelParts("12345_C_7", "", "C-BRCD03", "ABF-89D",
                "Normal", "cfDNA", NucleicAcid.DNA, "Plasma", "cfDNA", "12345_C", "Normal", Boolean.TRUE);
        // labels should be ignored during comparison
        Assertions.assertTrue(
                cmoLabelGeneratorService.sampleHasLabelSpecificUpdates(sm1Updated, Arrays.asList(sm1)));
        // assert true if no change at all metadata but existing sample does not already have a label
        CmoLabelParts sm1NoUpdatesNoLabel = initSmileSampleLabelParts("12345_C_7", "", "C-BRCD03", "ABF-89D",
                "Normal", "cfDNA", NucleicAcid.DNA, "Plasma", "cfDNA", "12345_C", "Normal", Boolean.FALSE);
        Assertions.assertTrue(cmoLabelGeneratorService.sampleHasLabelSpecificUpdates(sm1NoUpdatesNoLabel,
                Arrays.asList(sm1)));

        // assert false if no change at all in metadata and existing sample already has a label
        sm1.setCmoSampleName("C-BRCD03-L001-d01");
        CmoLabelParts sm1NoUpdatesWLabel = initSmileSampleLabelParts("12345_C_7", "", "C-BRCD03", "ABF-89D",
                "Normal", "cfDNA", NucleicAcid.DNA, "Plasma", "cfDNA", "12345_C", "Normal", Boolean.FALSE);
        Assertions.assertFalse(cmoLabelGeneratorService.sampleHasLabelSpecificUpdates(sm1NoUpdatesWLabel,
                Arrays.asList(sm1)));
    }

    @Test
    public void testExistingLabelInUse() throws Exception {
        CmoLabelParts sm1 = initSmileSampleLabelParts("12345_C_1", "C-BRCD03-X001-d01", "C-BRCD03",
                "ABF-89D", "Metastasis", "PDX", NucleicAcid.DNA, "", "DNA", "12345_C",
                "Tumor", Boolean.TRUE);
        CmoLabelParts sm2 = initSmileSampleLabelParts("12345_C_2", "C-BRCD03-X002-d01", "C-BRCD03",
                "ABF-D98", "Metastasis", "PDX", NucleicAcid.DNA, "", "DNA", "12345_C",
                "Tumor", Boolean.TRUE);

        CmoLabelParts sm1dup = initSmileSampleLabelParts("12345_B_1", "C-BRCD03-X001-d01", "C-BRCD03",
                "ABF-123", "Primary", "PDX", NucleicAcid.DNA, "", "DNA", "12345_C",
                "Tumor", Boolean.TRUE);
        CmoLabelParts sm2dup = initSmileSampleLabelParts("12345_B_2", "C-BRCD03-X002-d01", "C-BRCD03",
                "ABF-321", "Primary", "PDX", NucleicAcid.DNA, "", "DNA", "12345_C",
                "Tumor", Boolean.TRUE);

        CmoLabelParts sm1NeedsNewLabel = initSmileSampleLabelParts("12345_C_1", "", "C-BRCD03", "ABF-89D",
                "Metastasis", "PDX", NucleicAcid.DNA, "", "DNA", "12345_C", "Tumor", Boolean.TRUE);

        List<CmoLabelParts> existingPatientSamples = Arrays.asList(sm1, sm2, sm1dup, sm2dup);
        Boolean generateLabel = cmoLabelGeneratorService.sampleHasLabelSpecificUpdates(sm1NeedsNewLabel,
                existingPatientSamples);
        Assertions.assertTrue(generateLabel);
    }

    private CmoLabelParts getSampleWithPrimaryIdAndLabel(String primaryId, String cmoSampleName)
            throws JsonProcessingException {
        return initSmileSampleLabelParts(primaryId, cmoSampleName, null, null,
                null, null, NucleicAcid.DNA, null, null, null, null, Boolean.TRUE);
    }

    private CmoLabelParts initSmileSampleLabelParts(String primaryId, String cmoSampleName,
            String cmoPatientId, String altId, String sampleType, String sampleClass,
            NucleicAcid naToExtract, String sampleOrigin, String sampleTypeDetailed,
            String requestId, String tumorOrNormal, Boolean isCmoSample)
            throws JsonProcessingException {
        Map<String, Object> sample = new HashMap<>();
        sample.put("primaryId", primaryId);
        sample.put("cmoPatientId", cmoPatientId);
        sample.put("cmoSampleName", cmoSampleName);
        sample.put("sampleType", sampleType);
        sample.put("sampleClass", sampleClass);
        sample.put("sampleOrigin", sampleOrigin);
        sample.put("tumorOrNormal", tumorOrNormal);

        Map<String, Object> additionalProperties = new HashMap<>();
        additionalProperties.put("altId", altId);
        additionalProperties.put("isCmoSample", isCmoSample);
        sample.put("additionalProperties", additionalProperties);

        Map<String, Object> cmoSampleIdFields = new HashMap<>();
        cmoSampleIdFields.put("naToExtract", naToExtract.getValue());
        cmoSampleIdFields.put("normalizedPatientId", cmoPatientId);
        cmoSampleIdFields.put("sampleType", sampleTypeDetailed);
        sample.put("cmoSampleIdFields", cmoSampleIdFields);
        return new CmoLabelParts(sample, requestId, null);
    }

    private CmoLabelParts initIgoSampleLabelParts(String igoId, String cmoPatientId,
            SpecimenType specimenType, NucleicAcid naToExtract, String investigatorSampleId, String altId,
            String requestId, String tumorOrNormal, Boolean isCmoSample) throws JsonProcessingException {
        Map<String, Object> sample = new HashMap<>();
        sample.put("igoId", igoId);
        sample.put("cmoPatientId", cmoPatientId);
        sample.put("specimenType", specimenType.getValue());
        sample.put("investigatorSampleId", investigatorSampleId);
        sample.put("cmoSampleClass", "Tumor");
        sample.put("altid", altId);
        sample.put("tumorOrNormal", tumorOrNormal);

        Map<String, Object> cmoSampleIdFields = new HashMap<>();
        cmoSampleIdFields.put("naToExtract", naToExtract.getValue());
        cmoSampleIdFields.put("normalizedPatientId", cmoPatientId);
        cmoSampleIdFields.put("sampleType", specimenType.getValue());
        sample.put("cmoSampleIdFields", cmoSampleIdFields);
        return new CmoLabelParts(sample, requestId, isCmoSample);
    }

    private List<CmoLabelParts> getPatientSamplesFromRequestJson(String mockedRequestId,
            String patientId) throws JsonProcessingException {
        MockJsonTestData requestJson = mockedRequestJsonDataMap
                .get(mockedRequestId);
        Map<String, String> requestJsonMap = mapper.readValue(requestJson.getJsonString(), Map.class);
        Object isCmoRequest = requestJsonMap.get("isCmoRequest");
        List<Object> sampleObjList = mapper.convertValue(requestJsonMap.get("samples"),
                List.class);

        List<CmoLabelParts> existingSamples = new ArrayList<>();
        for (Object s : sampleObjList) {
            Map<String, Object> sm = mapper.convertValue(s, Map.class);
            CmoLabelParts sampleLabelParts = new CmoLabelParts(sm, mockedRequestId, isCmoRequest);
            if (sampleLabelParts.getCmoPatientId().equals(patientId)) {
                existingSamples.add(sampleLabelParts);
            }
        }
        return existingSamples;
    }
}

package org.mskcc.smile;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    List<SampleMetadata> SAMPLES_BY_ALT_ID = new ArrayList<>();

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
     * @throws JsonProcessingException
     * @throws CloneNotSupportedException
     * @throws IllegalArgumentException
     */
    @Test
    public void testCmoLabelGenForSampleUpdate() throws JsonProcessingException,
            IllegalArgumentException, CloneNotSupportedException {
        MockJsonTestData requestJson = mockedRequestJsonDataMap
                .get("mockPublishedRequest1JsonDataWith2T2N");
        // Parse requestMap and sampleList from mockPublishedRequest1JsonDataWith2T2N json
        Map<String, String> mappedRequestJson = mapper.readValue(requestJson.getJsonString(), Map.class);
        SampleMetadata[] samples = mapper.convertValue(mappedRequestJson.get("samples"),
                SampleMetadata[].class);

        // existing sample for patientId: C-MP789JR
        List<SampleMetadata> existingSamples = new ArrayList<>();
        existingSamples.add(samples[0]);
        existingSamples.add(samples[1]);

        // updated SampleMetadata
        SampleMetadata updatedSample = mapper.convertValue(
                samples[0].clone(), SampleMetadata.class);
        updatedSample.setSampleClass("NewSpecimen");
        updatedSample.setPrimaryId("newPrimaryId");

        // generate cmoLabel for sample with updates
        String newCmoLabel = cmoLabelGeneratorService.generateCmoSampleLabel(
                updatedSample, existingSamples, SAMPLES_BY_ALT_ID);
        // if the cmo label before the update is C-MP789JR-X001-d
        Assertions.assertEquals("C-MP789JR-P003-d02", newCmoLabel);

        Status sampleStatus = cmoLabelGeneratorService.generateSampleStatus(
                updatedSample, existingSamples);
        Assertions.assertEquals(Boolean.TRUE, sampleStatus.getValidationStatus());
        Assertions.assertEquals(sampleStatus.getValidationReport(), (new HashMap()).toString());
    }

    /**
     * Tests if a new cmoLabel is properly generated for
     *     a sample with metadata with updated cmoPatientId and primaryId/igoId
     * Expected behavior: Should return a label with
     *     incremented sample count and new cmoPatientId
     * @throws JsonProcessingException
     * @throws JsonMappingException
     * @throws CloneNotSupportedException
     * @throws IllegalArgumentException
     */
    @Test
    public void testCmoLabelGenForSampleWithPatientCorrection()
            throws JsonMappingException, JsonProcessingException,
            IllegalArgumentException, CloneNotSupportedException {
        MockJsonTestData requestJson = mockedRequestJsonDataMap
                .get("mockPublishedRequest1JsonDataWith2T2N");
        // Parse requestMap and sampleList from mockPublishedRequest1JsonDataWith2T2N json
        Map<String, String> mappedRequestJson = mapper.readValue(
                requestJson.getJsonString(), Map.class);
        SampleMetadata[] samples = mapper.convertValue(mappedRequestJson.get("samples"),
                SampleMetadata[].class);

        // existing sample for patientId: C-MP789JR
        List<SampleMetadata> existingSamples = new ArrayList<>();
        existingSamples.add(samples[0]);
        existingSamples.add(samples[1]);

        // updated SampleMetadata
        SampleMetadata updatedSample = mapper.convertValue(
                samples[0].clone(), SampleMetadata.class);
        updatedSample.setCmoPatientId("C-newPatient");
        updatedSample.setPrimaryId("newIgoId");

        // generate cmoLabel for sample with updates
        String newCmoLabel = cmoLabelGeneratorService.generateCmoSampleLabel(
                updatedSample, existingSamples, SAMPLES_BY_ALT_ID);

        // if the cmo label before the update is C-MP789JR-X001-d
        Assertions.assertEquals("C-newPatient-X003-d02", newCmoLabel);

        Status sampleStatus = cmoLabelGeneratorService.generateSampleStatus(
                updatedSample, existingSamples);
        Assertions.assertEquals(Boolean.TRUE, sampleStatus.getValidationStatus());
        Assertions.assertEquals(sampleStatus.getValidationReport(), (new HashMap()).toString());
    }

    /**
     * Tests if a new cmoLabel is properly generated for
     *     a sample with metadata with updated cmoPatientId and primaryId/igoId
     * Expected behavior: Should return a label with
     *     only a new cmoPatientId change
     * @throws JsonProcessingException
     * @throws JsonMappingException
     * @throws CloneNotSupportedException
     * @throws IllegalArgumentException
     */
    @Test
    public void testCmoLabelGenForExistingSampleWithPatientCorrection()
            throws JsonMappingException, JsonProcessingException,
            IllegalArgumentException, CloneNotSupportedException {
        MockJsonTestData requestJson = mockedRequestJsonDataMap
                .get("mockPublishedRequest1JsonDataWith2T2N");
        // Parse requestMap and sampleList from mockPublishedRequest1JsonDataWith2T2N json
        Map<String, String> mappedRequestJson = mapper.readValue(
                requestJson.getJsonString(), Map.class);
        SampleMetadata[] samples = mapper.convertValue(mappedRequestJson.get("samples"),
                SampleMetadata[].class);

        // existing sample for patientId: C-MP789JR
        List<SampleMetadata> existingSamples = new ArrayList<>();
        existingSamples.add(samples[0]);
        existingSamples.add(samples[1]);

        // updated SampleMetadata
        SampleMetadata updatedSample = mapper.convertValue(
                samples[0].clone(), SampleMetadata.class);
        updatedSample.setCmoPatientId("C-newPatient");

        // generate cmoLabel for sample with updates
        String newCmoLabel = cmoLabelGeneratorService.generateCmoSampleLabel(
                updatedSample, existingSamples, SAMPLES_BY_ALT_ID);

        // if the cmo label before the update is C-MP789JR-X001-d
        Assertions.assertEquals("C-newPatient-X001-d02", newCmoLabel);


        Status sampleStatus = cmoLabelGeneratorService.generateSampleStatus(
                updatedSample, existingSamples);
        Assertions.assertEquals(Boolean.TRUE, sampleStatus.getValidationStatus());
        Assertions.assertEquals(sampleStatus.getValidationReport(), (new HashMap()).toString());
    }

    /**
     * Tests that label generation fails as expected for sample with the following:
     * - sample type (cmo sample class) = other
     * - sample origin = whole blood
     * - sample class (specimen type) = other
     * Expected behavior: Should return null label.
     * @throws JsonProcessingException
     * @throws JsonMappingException
     * @throws CloneNotSupportedException
     * @throws IllegalArgumentException
     */
    @Test
    public void testCmoLabelGenForSampleWithOtherSpecimenType()
            throws JsonMappingException, JsonProcessingException,
            IllegalArgumentException, CloneNotSupportedException {
        MockJsonTestData requestJson = mockedRequestJsonDataMap
                .get("mockPublishedRequest1JsonDataWith2T2N");
        // Parse requestMap and sampleList from mockPublishedRequest1JsonDataWith2T2N json
        Map<String, String> mappedRequestJson = mapper.readValue(
                requestJson.getJsonString(), Map.class);
        SampleMetadata[] samples = mapper.convertValue(mappedRequestJson.get("samples"),
                SampleMetadata[].class);

        // existing sample for patientId: C-MP789JR
        List<SampleMetadata> existingSamples = new ArrayList<>();

        // updated SampleMetadata
        SampleMetadata updatedSample = mapper.convertValue(
                samples[0].clone(), SampleMetadata.class);
        updatedSample.setSampleType("Other");
        updatedSample.setSampleOrigin("Whole Blood");
        updatedSample.setSampleClass("Other");

        // generate cmoLabel for sample with spec type (sample type) = other
        // should return null string
        String newCmoLabel = cmoLabelGeneratorService.generateCmoSampleLabel(
                updatedSample, existingSamples, SAMPLES_BY_ALT_ID);
        Assertions.assertEquals("C-MP789JR-F001-d01", newCmoLabel);

        Status sampleStatus = cmoLabelGeneratorService.generateSampleStatus(
                updatedSample, existingSamples);
        Assertions.assertEquals(Boolean.FALSE, sampleStatus.getValidationStatus());
        Assertions.assertNotSame(sampleStatus.getValidationReport(), (new HashMap()).toString());
    }

    @Test
    public void testCmoCelllineLabelGenerationUpdates() {
        String requestId = "86793_T";
        List<SampleMetadata> existingSamples = new ArrayList<>();

        // generate reference cell line sample label and assert it matches the expected value
        IgoSampleManifest sample = getSampleMetadata("86793_T_4", "C-76767",
                SpecimenType.CELLLINE, NucleicAcid.DNA, "AMP1");
        String sampleLabel =  cmoLabelGeneratorService.generateCmoSampleLabel(requestId,
                sample, existingSamples, SAMPLES_BY_ALT_ID);
        String sampleExpectedLabel = "AMP1-86793T";
        Assertions.assertEquals("AMP1-86793T", sampleLabel);

        // test label generated with new investigator id and assert
        // that the sample would require a label update
        IgoSampleManifest sampleUpdatedInvestigatorId = getSampleMetadata("86793_T_4", "C-76767",
                SpecimenType.CELLLINE, NucleicAcid.DNA, "MIP2");
        String sampleUpdatedInvestigatorIdLabel = cmoLabelGeneratorService.generateCmoSampleLabel(requestId,
                sampleUpdatedInvestigatorId, existingSamples, SAMPLES_BY_ALT_ID);
        String expectedLabelWithInvestiagorIdUpdate = "MIP2-86793T";
        Assertions.assertEquals(expectedLabelWithInvestiagorIdUpdate, sampleUpdatedInvestigatorIdLabel);
        // assert that the sample would require a label update
        Assertions.assertTrue(cmoLabelGeneratorService.igoSampleRequiresLabelUpdate(
                sampleUpdatedInvestigatorIdLabel, sampleLabel));

        // test label generated with same investigator id as original sample
        // but with a different nucleic acid value
        IgoSampleManifest sampleUpdatedNaExtract = getSampleMetadata("86793_T_4", "C-76767",
                SpecimenType.CELLLINE, NucleicAcid.CFDNA, "AMP1");
        String sampleUpdatedNaExtractLabel = cmoLabelGeneratorService.generateCmoSampleLabel(requestId,
                sampleUpdatedNaExtract, existingSamples, SAMPLES_BY_ALT_ID);
        Assertions.assertEquals(sampleExpectedLabel, sampleUpdatedNaExtractLabel);
        Assertions.assertFalse(cmoLabelGeneratorService.igoSampleRequiresLabelUpdate(
                sampleUpdatedNaExtractLabel, sampleExpectedLabel));
    }

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

    private IgoSampleManifest getSampleMetadata(String igoId, String cmoPatientId,
            SpecimenType specimenType, NucleicAcid naToExtract, String investigatorSampleId) {
        IgoSampleManifest sample = new IgoSampleManifest();
        sample.setIgoId(igoId);
        sample.setCmoPatientId(cmoPatientId);
        sample.setSpecimenType(specimenType.getValue());
        sample.setInvestigatorSampleId(investigatorSampleId);
        sample.setCmoSampleClass("Tumor");

        Map<String, String> cmoSampleIdFields = new HashMap<>();
        cmoSampleIdFields.put("naToExtract", naToExtract.getValue());
        cmoSampleIdFields.put("normalizedPatientId", cmoPatientId);
        sample.setCmoSampleIdFields(cmoSampleIdFields);
        return sample;
    }
}

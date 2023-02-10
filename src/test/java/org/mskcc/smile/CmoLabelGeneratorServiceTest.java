package org.mskcc.smile;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mskcc.smile.commons.enums.NucleicAcid;
import org.mskcc.smile.commons.enums.SpecimenType;
import org.mskcc.smile.config.TestConfiguration;
import org.mskcc.smile.model.MockJsonTestData;
import org.mskcc.smile.model.SampleMetadata;
import org.mskcc.smile.model.Status;
import org.mskcc.smile.model.igo.IgoSampleManifest;
import org.mskcc.smile.service.CmoLabelGeneratorService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@ContextConfiguration(classes = TestConfiguration.class)
@RunWith(SpringJUnit4ClassRunner.class)
@ComponentScan("org.mskcc.smile.service")
public class CmoLabelGeneratorServiceTest {
    private final ObjectMapper mapper = new ObjectMapper();

    @Autowired
    private CmoLabelGeneratorService cmoLabelGeneratorService;

    @Autowired
    private Map<String, MockJsonTestData> mockedRequestJsonDataMap;

    /**
     * Tests to ensure the mocked request json data map is not null
     */
    @Test
    public void testMockedRequestJsonDataLoading() {
        Assert.assertNotNull(mockedRequestJsonDataMap);
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
        Assert.assertNotNull(modifiedRequestJson);
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
                updatedSample, existingSamples);
        // if the cmo label before the update is C-MP789JR-X001-d
        Assert.assertEquals("C-MP789JR-P003-d02", newCmoLabel);

        Status sampleStatus = cmoLabelGeneratorService.generateSampleStatus(
                updatedSample, existingSamples);
        Assert.assertEquals(Boolean.TRUE, sampleStatus.getValidationStatus());
        Assert.assertEquals(sampleStatus.getValidationReport(), (new HashMap()).toString());
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
                updatedSample, existingSamples);

        // if the cmo label before the update is C-MP789JR-X001-d
        Assert.assertEquals("C-newPatient-X003-d02", newCmoLabel);

        Status sampleStatus = cmoLabelGeneratorService.generateSampleStatus(
                updatedSample, existingSamples);
        Assert.assertEquals(Boolean.TRUE, sampleStatus.getValidationStatus());
        Assert.assertEquals(sampleStatus.getValidationReport(), (new HashMap()).toString());
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
                updatedSample, existingSamples);

        // if the cmo label before the update is C-MP789JR-X001-d
        Assert.assertEquals("C-newPatient-X001-d02", newCmoLabel);

        
        Status sampleStatus = cmoLabelGeneratorService.generateSampleStatus(
                updatedSample, existingSamples);
        Assert.assertEquals(Boolean.TRUE, sampleStatus.getValidationStatus());
        Assert.assertEquals(sampleStatus.getValidationReport(), (new HashMap()).toString());
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
                updatedSample, existingSamples);
        Assert.assertNull(newCmoLabel);

        Status sampleStatus = cmoLabelGeneratorService.generateSampleStatus(
                updatedSample, existingSamples);
        Assert.assertEquals(Boolean.FALSE, sampleStatus.getValidationStatus());
        Assert.assertNotSame(sampleStatus.getValidationReport(), (new HashMap()).toString());
    }

    @Test
    public void testCmoCelllineLabelGenerationUpdates() {
        String requestId = "86793_T";
        List<SampleMetadata> existingSamples = new ArrayList<>();

        // generate reference cell line sample label and assert it matches the expected value
        IgoSampleManifest sample = getSampleMetadata("86793_T_4", "C-76767",
                SpecimenType.CELLLINE, NucleicAcid.DNA, "AMP1");
        String sampleLabel =  cmoLabelGeneratorService.generateCmoSampleLabel(requestId,
                sample, existingSamples);
        String sampleExpectedLabel = "AMP1-86793T";
        Assert.assertEquals("AMP1-86793T", sampleLabel);

        // test label generated with new investigator id and assert
        // that the sample would require a label update
        IgoSampleManifest sampleUpdatedInvestigatorId = getSampleMetadata("86793_T_4", "C-76767",
                SpecimenType.CELLLINE, NucleicAcid.DNA, "MIP2");
        String sampleUpdatedInvestigatorIdLabel = cmoLabelGeneratorService.generateCmoSampleLabel(requestId,
                sampleUpdatedInvestigatorId, existingSamples);
        String expectedLabelWithInvestiagorIdUpdate = "MIP2-86793T";
        Assert.assertEquals(expectedLabelWithInvestiagorIdUpdate, sampleUpdatedInvestigatorIdLabel);
        // assert that the sample would require a label update
        Assert.assertTrue(cmoLabelGeneratorService.igoSampleRequiresLabelUpdate(
                sampleUpdatedInvestigatorIdLabel, sampleLabel));

        // test label generated with same investigator id as original sample
        // but with a different nucleic acid value
        IgoSampleManifest sampleUpdatedNaExtract = getSampleMetadata("86793_T_4", "C-76767",
                SpecimenType.CELLLINE, NucleicAcid.CFDNA, "AMP1");
        String sampleUpdatedNaExtractLabel = cmoLabelGeneratorService.generateCmoSampleLabel(requestId,
                sampleUpdatedNaExtract, existingSamples);
        Assert.assertEquals(sampleExpectedLabel, sampleUpdatedNaExtractLabel);
        Assert.assertFalse(cmoLabelGeneratorService.igoSampleRequiresLabelUpdate(
                sampleUpdatedNaExtractLabel, sampleExpectedLabel));
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

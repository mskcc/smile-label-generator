package org.mskcc.cmo.metadb;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mskcc.cmo.metadb.config.TestConfiguration;
import org.mskcc.cmo.metadb.model.MockJsonTestData;
import org.mskcc.cmo.metadb.model.SampleMetadata;
import org.mskcc.cmo.metadb.service.CmoLabelGeneratorService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@ContextConfiguration(classes = TestConfiguration.class)
@RunWith(SpringJUnit4ClassRunner.class)
@ComponentScan("org.mskcc.cmo.metadb.service")
public class CmoLabelGeneratorServiceTest {
    private final ObjectMapper mapper = new ObjectMapper();

    @Autowired
    CmoLabelGeneratorService cmoLabelGeneratorService;

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
     * testCmoLabelGenerationForSampleUpdate
     * Tests if a new cmoLabel is properly generated for
     *      a sample with metadata updates including igoId/primaryId
     *  Expected behavior: Should return a label with
     *      incremented sample count and new Sample Type Abbreviation
     * @throws JsonProcessingException 
     */
    @Test
    public void testCmoLabelGenForSampleUpdate() throws JsonProcessingException {
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
        SampleMetadata updatedSample = new SampleMetadata(samples[0]);
        updatedSample.setSampleClass("NewSpecimen");
        updatedSample.setPrimaryId("newPrimaryId");
        
        // generate cmoLabel for sample with updates
        String newCmoLabel = cmoLabelGeneratorService.generateCmoSampleLabel(
                updatedSample, existingSamples);

        Assert.assertEquals("C-MP789JR-P003-d02", newCmoLabel);
    }

    /**
     * testCmoLabelGenForSampleWithPatientCorrection
     * Tests if a new cmoLabel is properly generated for
     *     a sample with metadata with updated cmoPatientId and primaryId/igoId
     * Expected behavior: Should return a label with
     *     incremented sample count and new cmoPatientId
     * @throws JsonProcessingException 
     * @throws JsonMappingException 
     */
    @Test
    public void testCmoLabelGenForSampleWithPatientCorrection()
            throws JsonMappingException, JsonProcessingException {
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
        SampleMetadata updatedSample = new SampleMetadata(samples[0]);
        updatedSample.setCmoPatientId("C-newPatient");
        updatedSample.setPrimaryId("newIgoId");
        
        // generate cmoLabel for sample with updates
        String newCmoLabel = cmoLabelGeneratorService.generateCmoSampleLabel(
                updatedSample, existingSamples);

        Assert.assertEquals("C-newPatient-X003-d02", newCmoLabel);
        
    }
    
    /**
     * testCmoLabelGenForExistingSampleWithPatientCorrection
     * Tests if a new cmoLabel is properly generated for
     *     a sample with metadata with updated cmoPatientId and primaryId/igoId
     * Expected behavior: Should return a label with
     *     only a new cmoPatientId change
     * @throws JsonProcessingException 
     * @throws JsonMappingException 
     */
    @Test
    public void testCmoLabelGenForExistingSampleWithPatientCorrection()
            throws JsonMappingException, JsonProcessingException {
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
        SampleMetadata updatedSample = new SampleMetadata(samples[0]);
        updatedSample.setCmoPatientId("C-newPatient");
        
        // generate cmoLabel for sample with updates
        String newCmoLabel = cmoLabelGeneratorService.generateCmoSampleLabel(
                updatedSample, existingSamples);

        Assert.assertEquals("C-newPatient-X002-d02", newCmoLabel);
    }
}

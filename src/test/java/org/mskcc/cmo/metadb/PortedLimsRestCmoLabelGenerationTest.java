package org.mskcc.cmo.metadb;

import java.util.*;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mskcc.cmo.metadb.config.TestConfiguration;
import org.mskcc.cmo.metadb.model.SampleMetadata;
import org.mskcc.cmo.metadb.service.CmoLabelGeneratorService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.mskcc.cmo.metadb.service.MetadbService;

/**
 * Ported tests from
 *
 * SampleTypeCorrectedCmoSampleViewGeneratorTest.java
 * https://bit.ly/2Sqfxi3
 *
 * Reference code:
 * - https://github.com/mskcc/LimsRest : SampleTypeCorrectedCmoSampleViewGeneratorTest.java
 * - https://github.com/mskcc/common-domain : SpecimenType.java, NucleicAcid.java, etc.
 *
 * Notes:
 * - Looks like all samples for a given patient id are returned.
 * - Each sample has its corresponding request id assigned to it.
 * @author ochoaa
 */
@ContextConfiguration(classes = TestConfiguration.class)
@RunWith(SpringJUnit4ClassRunner.class)
public class PortedLimsRestCmoLabelGenerationTest {

    @Autowired
    private CmoLabelGeneratorService cmoLabelGeneratorService;

    @Autowired
    private MetadbService metadbRestService;

    /**
     * Test CMO label generation for a patient with no samples.
     * Returned label should be the CMO ID with number 1.
     * Ported from test:
     *   whenThereAreNoPatientSamples_shouldReturnCmoIdWithNumber1
     * @throws java.lang.Exception
     */
    @Test
    public void testPatientNoExistingSamples() throws Exception {
        String requestId = "5432_P";
        String cmoPatientId = "C-1235";

        // change to enum SpecimenType.Xenograft("Xenograft")
        // change to enum NucleicAcid.DNA("DNA")
        SampleMetadata sample = getSampleMetadata(requestId, "4324", cmoPatientId, "Xenograft", "DNA");
        Mockito.when(metadbRestService.getSampleMetadataListByCmoPatientId(cmoPatientId))
                .thenReturn(new ArrayList<>());

        String cmoId = cmoLabelGeneratorService.generateCmoSampleLabel(sample);
        Assert.assertEquals("C-1235-X001-d", cmoId);
    }


    /**
     * Test CMO label generation for a patient with existing sample of same request,
     * same igo id, same specimen type, and same nucleic acid.
     * Returned label should be the CMO ID with number 2.
     * Ported from test:
     *   whenThereIsOnePatientSampleFromSameRequestWithCount1_shouldReturnCmoIdWithNumber2
     * @throws java.lang.Exception
     */
    @Test
    public void testPatientOneExistingSample() throws Exception {
        String requestId = "5432_P";
        String cmoPatientId = "C-1235";

        SampleMetadata sample = getSampleMetadata(requestId, "4324", cmoPatientId, "Xenograft", "DNA");

        String existingSampleId = "C-1235-X001-d";
        SampleMetadata existingSample = getSampleMetadataWithCmoLabel(requestId, "5656", cmoPatientId, "Xenograft", existingSampleId);
        // get existing samples for given igo id and request id
        List<SampleMetadata> existingSamples = Arrays.asList(existingSample);
        // fetch existing samples for given cmo patient id
        Mockito.when(metadbRestService.getSampleMetadataListByCmoPatientId(cmoPatientId))
                .thenReturn(existingSamples);

        String cmoId = cmoLabelGeneratorService.generateCmoSampleLabel(sample);
        Assert.assertEquals("C-1235-X002-d", cmoId);
    }

    /**
     * Test CMO label generation for a patient with existing sample of same igoId
     * and request. Returns label with CMO ID incremented by one.
     * Ported from test:
     *   whenThereIsOnePatientSampleFromSameRequestWithSomeCount_shouldReturnCmoIdWithThisCountPlusOne
     * @throws java.lang.Exception
     */
    @Test
    public void testPatientOneSampleNextIncrement() throws Exception {
        String requestId = "5432_P";
        String cmoPatientId = "C-1235";

        SampleMetadata sample = getSampleMetadata(requestId, "4324", cmoPatientId, "Xenograft", "DNA");

        String existingSampleId = "C-1235-X012-d";
        SampleMetadata existingSample = getSampleMetadataWithCmoLabel(requestId, "5656", cmoPatientId, "Xenograft", existingSampleId);
        // get existing samples for given igo id and request id
        List<SampleMetadata> existingSamples = Arrays.asList(existingSample);
        // fetch existing samples for given cmo patient id
        Mockito.when(metadbRestService.getSampleMetadataListByCmoPatientId(cmoPatientId))
                .thenReturn(existingSamples);

        String cmoId = cmoLabelGeneratorService.generateCmoSampleLabel(sample);
        Assert.assertEquals("C-1235-X013-d", cmoId);
    }

   /**
    * Test CMO label generation for a patient sample from a different request.
    * Sample also is of a different specimen type.
    * Returns label with CMO ID incremented.
    * Ported from test:
    *   whenThereIsOnePatientSampleFromDifferentRequest_shouldReturnCmoIdWithNumber2
     * @throws java.lang.Exception
    */
    @Test
    public void testPatientOneSampleDiffRequest() throws Exception {
        String requestId = "5432_P";
        String cmoPatientId = "C-1235";

        SampleMetadata sample = getSampleMetadata(requestId, "4324", cmoPatientId, "PDX", "RNA");

        String existingSampleId = "C-1235-X001-d";
        String diffRequestId = "1234_A";
        SampleMetadata existingSample = getSampleMetadataWithCmoLabel(diffRequestId, "5656", cmoPatientId, "Xenograft", existingSampleId);
        // get existing samples for given igo id and request id
        List<SampleMetadata> existingSamples = Arrays.asList(existingSample);
        // fetch existing samples for given cmo patient id
        Mockito.when(metadbRestService.getSampleMetadataListByCmoPatientId(cmoPatientId))
                .thenReturn(existingSamples);

        String cmoId = cmoLabelGeneratorService.generateCmoSampleLabel(sample);
        Assert.assertEquals("C-1235-X002-r", cmoId);
    }

    /**
     * Test CMO label generation for a patient with no samples but sample(s) already
     * exist of the same specimen type and nucleic acid.
     * Returns label with CMO ID incremented by one.
     * Ported from test:
     *   whenThereAreNoPatientSamplesButOneWasAlreadyCreatedFromSameRequestSameSpecimen ...
     *   ... SampleNucl_shouldReturnCmoIdWithNumber2
     * @throws java.lang.Exception
     */
    @Test
    public void testPatientTwoSamplesSameSpecimenTypeNucleicAcid() throws Exception {
        String requestId = "5432_P";
        String cmoPatientId = "C-1235";

        SampleMetadata sample1 = getSampleMetadata(requestId, "4324_1", cmoPatientId, "Xenograft", "DNA");
        Mockito.when(metadbRestService.getSampleMetadataListByCmoPatientId(cmoPatientId))
                .thenReturn(new ArrayList<>());
        String cmoId1 = cmoLabelGeneratorService.generateCmoSampleLabel(sample1);
        Assert.assertEquals("C-1235-X001-d", cmoId1);

        // now we have one existing sample in this request for the same patient
        SampleMetadata existingSample = getSampleMetadataWithCmoLabel(requestId, "4324_1", cmoPatientId, "Xenograft", cmoId1);
        // get existing samples for given igo id and request id
        List<SampleMetadata> existingSamples = Arrays.asList(existingSample);
        // fetch existing samples for given cmo patient id
        Mockito.when(metadbRestService.getSampleMetadataListByCmoPatientId(cmoPatientId))
                .thenReturn(existingSamples);

        SampleMetadata sample2 = getSampleMetadata(requestId, "4324_2", cmoPatientId, "Xenograft", "DNA");
        String cmoId2 = cmoLabelGeneratorService.generateCmoSampleLabel(sample2);
        Assert.assertEquals("C-1235-X002-d", cmoId2);
    }

    /**
     * Test CMO label generation for a patient with no samples but sample(s) already
     * exist of the same specimen type but different nucleic acid.
     * Returns label with CMO ID incremented by one.
     * Ported from test:
     *   whenThereAreNoPatientSamplesButOneWasAlreadyCreatedFromSameRequestSameSpecimenDifferent ...
     *   ... Nucl_shouldReturnCmoIdWithNumber2
     * @throws java.lang.Exception
     */
    @Test
    public void testPatientTwoSamplesSameSpecimenTypeDiffNucleicAcid() throws Exception {
        String requestId = "5432_P";
        String cmoPatientId = "C-1235";

        SampleMetadata sample1 = getSampleMetadata(requestId, "4324_1", cmoPatientId, "Xenograft", "DNA");
        Mockito.when(metadbRestService.getSampleMetadataListByCmoPatientId(cmoPatientId))
                .thenReturn(new ArrayList<>());
        String cmoId1 = cmoLabelGeneratorService.generateCmoSampleLabel(sample1);
        Assert.assertEquals("C-1235-X001-d", cmoId1);

        // now we have one existing sample in this request for the same patient
        SampleMetadata existingSample = getSampleMetadataWithCmoLabel(requestId, "4324_1", cmoPatientId, "Xenograft", cmoId1);
        // get existing samples for given igo id and request id
        List<SampleMetadata> existingSamples = Arrays.asList(existingSample);
        // fetch existing samples for given cmo patient id
        Mockito.when(metadbRestService.getSampleMetadataListByCmoPatientId(cmoPatientId))
                .thenReturn(existingSamples);

        SampleMetadata sample2 = getSampleMetadata(requestId, "4324_2", cmoPatientId, "Xenograft", "RNA");
        String cmoId2 = cmoLabelGeneratorService.generateCmoSampleLabel(sample2);
        Assert.assertEquals("C-1235-X002-r", cmoId2);
    }

    /**
     * Test CMO label generation for patient with no samples but one was already created from same request,
     * different specimen type, and same nucleic acid.
     * Returns label with CMO ID incremented by one.
     * Ported from test:
     *   whenThereAreNoPatientSamplesButOneWasAlreadyCreatedFromSameRequestSameSampleType ...
     *   ... _shouldReturnCmoIdWithNumber2
     * @throws Exception
     */
    @Test
    public void testPatientTwoSamplesDiffSpecimenTypeSameRequestNucleicAcid()
            throws Exception {
        String requestId = "5432_P";
        String cmoPatientId = "C-1235";

        SampleMetadata sample1 = getSampleMetadata(requestId, "4324_1", cmoPatientId, "Xenograft", "DNA");
        Mockito.when(metadbRestService.getSampleMetadataListByCmoPatientId(cmoPatientId))
                .thenReturn(new ArrayList<>());
        String cmoId1 = cmoLabelGeneratorService.generateCmoSampleLabel(sample1);
        Assert.assertEquals("C-1235-X001-d", cmoId1);

        // now we have one existing sample in this request for the same patient
        SampleMetadata existingSample = getSampleMetadataWithCmoLabel(requestId, "4324_1", cmoPatientId, "Xenograft", cmoId1);
        // get existing samples for given igo id and request id
        List<SampleMetadata> existingSamples = Arrays.asList(existingSample);
        // fetch existing samples for given cmo patient id
        Mockito.when(metadbRestService.getSampleMetadataListByCmoPatientId(cmoPatientId))
                .thenReturn(existingSamples);

        SampleMetadata sample2 = getSampleMetadata(requestId, "4324_2", cmoPatientId, "PDX", "DNA");
        String cmoId2 = cmoLabelGeneratorService.generateCmoSampleLabel(sample2);
        Assert.assertEquals("C-1235-X002-d", cmoId2);
    }

    /**
     * Test CMO label generation for patient with no samples but one already created from different
     * request, same sample specimen type, same nucleic acid.
     * Returns label with CMO ID incremented by one.
     * Ported from test:
     *   whenThereAreNoPatientSamplesButOneWasAlreadyCreatedFromDifferentRequestsSampleSpecimen ...
     *   ... _shouldReturnCmoIdWithNumber2
     * @throws Exception
     */
    @Test
    public void testPatientTwoSamplesDiffRequestSameeSpecimenTypeNucleicAcid() throws Exception {
        String requestId = "5432_P";
        String cmoPatientId = "C-1235";

        SampleMetadata sample1 = getSampleMetadata(requestId, "4324_1", cmoPatientId, "Organoid", "DNA");
        Mockito.when(metadbRestService.getSampleMetadataListByCmoPatientId(cmoPatientId))
                .thenReturn(new ArrayList<>());
        String cmoId1 = cmoLabelGeneratorService.generateCmoSampleLabel(sample1);
        Assert.assertEquals("C-1235-G001-d", cmoId1);

        // now we have one existing sample in this request for the same patient
        SampleMetadata existingSample = getSampleMetadataWithCmoLabel(requestId, "4324_1", cmoPatientId, "Organoid", cmoId1);
        // get existing samples for given igo id and request id
        List<SampleMetadata> existingSamples = Arrays.asList(existingSample);
        // fetch existing samples for given cmo patient id
        Mockito.when(metadbRestService.getSampleMetadataListByCmoPatientId(cmoPatientId))
                .thenReturn(existingSamples);

        String diffRequestId = "0789_R";
        SampleMetadata sample2 = getSampleMetadata(diffRequestId, "4324_2", cmoPatientId, "Organoid", "DNA");
        String cmoId2 = cmoLabelGeneratorService.generateCmoSampleLabel(sample2);
        Assert.assertEquals("C-1235-G002-d", cmoId2);
    }

    private SampleMetadata getSampleMetadataWithCmoLabel(String requestId, String igoId, String cmoPatientId, String specimenType, String cmoSampleName) {
        SampleMetadata sample = new SampleMetadata();
        sample.setRequestId(requestId);
        sample.setIgoId(igoId);
        sample.setCmoPatientId(cmoPatientId);
        sample.setSpecimenType(specimenType);
        sample.setCmoSampleName(cmoSampleName);
        return sample;
    }

    private SampleMetadata getSampleMetadata(String requestId, String igoId, String cmoPatientId,
            String specimenType, String naToExtract) {
        SampleMetadata sample = new SampleMetadata();
        sample.setRequestId(requestId);
        sample.setIgoId(igoId);
        sample.setCmoPatientId(cmoPatientId);
        sample.setSpecimenType(specimenType);

        Map<String, String> cmoSampleIdFields = new HashMap<>();
        cmoSampleIdFields.put("naToExtract", naToExtract);
        sample.setCmoSampleIdFields(cmoSampleIdFields);
        return sample;
    }
}

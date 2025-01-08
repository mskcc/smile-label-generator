package org.mskcc.smile;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mskcc.smile.commons.enums.NucleicAcid;
import org.mskcc.smile.commons.enums.SpecimenType;
import org.mskcc.smile.config.TestConfiguration;
import org.mskcc.smile.model.SampleMetadata;
import org.mskcc.smile.model.igo.IgoSampleManifest;
import org.mskcc.smile.service.CmoLabelGeneratorService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

/**
 * Ported tests from SampleTypeCorrectedCmoSampleViewGeneratorTest.java
 * https://bit.ly/2Sqfxi3
 *
 * <p>Reference code:
 * - https://github.com/mskcc/LimsRest : SampleTypeCorrectedCmoSampleViewGeneratorTest.java
 * - https://github.com/mskcc/common-domain : SpecimenType.java, NucleicAcid.java, etc.
 *
 * <p>Notes:
 * - Looks like all samples for a given patient id are returned.
 * - Each sample has its corresponding request id assigned to it.
 * @author ochoaa
 */
@SpringBootTest(classes = LabelGeneratorTestApp.class)
@Import(TestConfiguration.class)
public class PortedLimsRestCmoLabelGenerationTest {
    // setting as empty for now until alt id is fully supported
    List<SampleMetadata> SAMPLES_BY_ALT_ID = new ArrayList<>();

    @Autowired
    private CmoLabelGeneratorService cmoLabelGeneratorService;

    /**
     * Test CMO label generation for a patient with no samples.
     * Returned label should be the CMO ID with number 1.
     * Ported from test:
     *   whenThereAreNoPatientSamples_shouldReturnCmoIdWithNumber1
     * @throws Exception
     */
    @Test
    public void testPatientNoExistingSamples() throws Exception {
        String requestId = "5432_P";
        String cmoPatientId = "C-1235";

        // change to enum SpecimenType.Xenograft("Xenograft")
        // change to enum NucleicAcid.DNA("DNA")
        IgoSampleManifest sample = getSampleMetadata("4324", cmoPatientId,
                SpecimenType.XENOGRAFT, NucleicAcid.DNA);

        String cmoId = cmoLabelGeneratorService.generateCmoSampleLabel(requestId, sample,
                new ArrayList<>(), SAMPLES_BY_ALT_ID);
        Assertions.assertEquals("C-1235-X001-d01", cmoId);
    }

    /**
     * Test CMO label generation for a patient with existing sample of same request,
     * same igo id, same specimen type, and same nucleic acid.
     * Returned label should be the CMO ID with number 2.
     * Ported from test:
     *   whenThereIsOnePatientSampleFromSameRequestWithCount1_shouldReturnCmoIdWithNumber2
     * @throws Exception
     */
    @Test
    public void testPatientOneExistingSample() throws Exception {
        String requestId = "5432_P";
        String cmoPatientId = "C-1235";

        IgoSampleManifest sample = getSampleMetadata("4324", cmoPatientId,
                SpecimenType.XENOGRAFT, NucleicAcid.DNA);

        String existingSampleId = "C-1235-X001-d";
        SampleMetadata existingSample = getSampleMetadataWithCmoLabel("5656",
                cmoPatientId, "Xenograft", existingSampleId);
        // get existing samples for given igo id and request id
        List<SampleMetadata> existingSamples = Arrays.asList(existingSample);

        String cmoId = cmoLabelGeneratorService.generateCmoSampleLabel(requestId, sample,
                existingSamples, SAMPLES_BY_ALT_ID);

        // keeping this "NotEquals" comparison for provenance - nucleic acid counter is now resolved
        // on a unique sample basis (i.e., alt id) and not by total patient samples of a given nuc acid type
        Assertions.assertNotEquals("C-1235-X002-d02", cmoId);
        Assertions.assertEquals("C-1235-X002-d01", cmoId);
    }

    /**
     * Test CMO label generation for a patient with existing sample of same request,
     * same igo id, same specimen type, and same nucleic acid.
     * Returned label should be the CMO ID with number 2.
     * This is the same test as directly above with the added nucleic acid abbreviation
     * counter.
     * Ported from test:
     *   whenThereIsOnePatientSampleFromSameRequestWithCount1_shouldReturnCmoIdWithNumber2
     * @throws Exception
     */
    @Test
    public void testPatientOneExistingSampleNucAcidCounter() throws Exception {
        String requestId = "5432_P";
        String cmoPatientId = "C-1235";

        IgoSampleManifest sample = getSampleMetadata("4324", cmoPatientId,
                SpecimenType.XENOGRAFT, NucleicAcid.DNA);

        String existingSampleId = "C-1235-X001-d01";
        SampleMetadata existingSample = getSampleMetadataWithCmoLabel("5656",
                cmoPatientId, "Xenograft", existingSampleId);
        // get existing samples for given igo id and request id
        List<SampleMetadata> existingSamples = Arrays.asList(existingSample);

        String cmoId = cmoLabelGeneratorService.generateCmoSampleLabel(requestId, sample,
                existingSamples, SAMPLES_BY_ALT_ID);

        // keeping this "NotEquals" comparison for provenance - nucleic acid counter is now resolved
        // on a unique sample basis (i.e., alt id) and not by total patient samples of a given nuc acid type
        Assertions.assertNotEquals("C-1235-X002-d02", cmoId);
        Assertions.assertEquals("C-1235-X002-d01", cmoId);
    }

    /**
     * Test CMO label generation for a patient with existing sample of same igoId
     * and request. Returns label with CMO ID incremented by one.
     * Ported from test:
     *   whenThereIsOnePatientSampleFromSameRequestWithSomeCount_shouldReturnCmoIdWithThisCountPlusOne
     * @throws Exception
     */
    @Test
    public void testPatientOneSampleNextIncrement() throws Exception {
        String requestId = "5432_P";
        String cmoPatientId = "C-1235";

        IgoSampleManifest sample = getSampleMetadata("4324", cmoPatientId,
                SpecimenType.XENOGRAFT, NucleicAcid.DNA);

        String existingSampleId = "C-1235-X012-d01";
        SampleMetadata existingSample = getSampleMetadataWithCmoLabel("5656",
                cmoPatientId, "Xenograft", existingSampleId);
        // get existing samples for given igo id and request id
        List<SampleMetadata> existingSamples = Arrays.asList(existingSample);

        String cmoId = cmoLabelGeneratorService.generateCmoSampleLabel(requestId, sample,
                existingSamples, SAMPLES_BY_ALT_ID);

        // keeping this "NotEquals" comparison for provenance - nucleic acid counter is now resolved
        // on a unique sample basis (i.e., alt id) and not by total patient samples of a given nuc acid type
        Assertions.assertNotEquals("C-1235-X013-d02", cmoId);
        Assertions.assertEquals("C-1235-X013-d01", cmoId);
    }

    /**
    * Test CMO label generation for a patient sample from a different request.
    * Sample also is of a different specimen type.
    * Returns label with CMO ID incremented.
    * Ported from test:
    *   whenThereIsOnePatientSampleFromDifferentRequest_shouldReturnCmoIdWithNumber2
     * @throws Exception
    */
    @Test
    public void testPatientOneSampleDiffRequest() throws Exception {
        String requestId = "5432_P";
        String cmoPatientId = "C-1235";

        IgoSampleManifest sample = getSampleMetadata("4324", cmoPatientId,
                SpecimenType.PDX, NucleicAcid.RNA);

        String existingSampleId = "C-1235-X001-d01";
        String diffRequestId = "1234_A";
        SampleMetadata existingSample = getSampleMetadataWithCmoLabel("5656",
                cmoPatientId, "Xenograft", existingSampleId);
        // get existing samples for given igo id and request id
        List<SampleMetadata> existingSamples = Arrays.asList(existingSample);

        // since nucleic acid abbreviation is different from existing sample, the
        // nucleic acid counter is also '01'
        String cmoId = cmoLabelGeneratorService.generateCmoSampleLabel(requestId, sample,
                existingSamples, SAMPLES_BY_ALT_ID);

        // this test result remains as is since the nuc acid is different from existing ones and would
        // recieve a nuc acid count of 1 anyway
        Assertions.assertEquals("C-1235-X002-r01", cmoId);
    }

    /**
     * Test CMO label generation for a patient with no samples but sample(s) already
     * exist of the same specimen type and nucleic acid.
     * Returns label with CMO ID incremented by one.
     * Ported from test:
     *   whenThereAreNoPatientSamplesButOneWasAlreadyCreatedFromSameRequestSameSpecimen ...
     *   ... SampleNucl_shouldReturnCmoIdWithNumber2
     * @throws Exception
     */
    @Test
    public void testPatientTwoSamplesSameSpecimenTypeNucleicAcid() throws Exception {
        String requestId = "5432_P";
        String cmoPatientId = "C-1235";

        IgoSampleManifest sample1 = getSampleMetadata("4324_1", cmoPatientId,
                SpecimenType.XENOGRAFT, NucleicAcid.DNA);
        String cmoId1 = cmoLabelGeneratorService.generateCmoSampleLabel(requestId,
                sample1, new ArrayList<>(), SAMPLES_BY_ALT_ID);
        Assertions.assertEquals("C-1235-X001-d01", cmoId1);

        // now we have one existing sample in this request for the same patient
        SampleMetadata existingSample = getSampleMetadataWithCmoLabel("4324_1",
                cmoPatientId, "Xenograft", cmoId1);
        // get existing samples for given igo id and request id
        List<SampleMetadata> existingSamples = Arrays.asList(existingSample);

        IgoSampleManifest sample2 = getSampleMetadata("4324_2", cmoPatientId,
                SpecimenType.XENOGRAFT, NucleicAcid.DNA);
        String cmoId2 = cmoLabelGeneratorService.generateCmoSampleLabel(requestId, sample2,
                existingSamples, SAMPLES_BY_ALT_ID);

        // keeping this "NotEquals" comparison for provenance - nucleic acid counter is now resolved
        // on a unique sample basis (i.e., alt id) and not by total patient samples of a given nuc acid type
        Assertions.assertNotEquals("C-1235-X002-d02", cmoId2);
        Assertions.assertEquals("C-1235-X002-d01", cmoId2);
    }

    /**
     * Test CMO label generation for a patient with no samples but sample(s) already
     * exist of the same specimen type but different nucleic acid.
     * Returns label with CMO ID incremented by one.
     * Ported from test:
     *   whenThereAreNoPatientSamplesButOneWasAlreadyCreatedFromSameRequestSameSpecimenDifferent ...
     *   ... Nucl_shouldReturnCmoIdWithNumber2
     * @throws Exception
     */
    @Test
    public void testPatientTwoSamplesSameSpecimenTypeDiffNucleicAcid() throws Exception {
        String requestId = "5432_P";
        String cmoPatientId = "C-1235";

        IgoSampleManifest sample1 = getSampleMetadata("4324_1", cmoPatientId,
                SpecimenType.XENOGRAFT, NucleicAcid.DNA);
        String cmoId1 = cmoLabelGeneratorService.generateCmoSampleLabel(requestId,
                sample1, new ArrayList<>(), SAMPLES_BY_ALT_ID);
        Assertions.assertEquals("C-1235-X001-d01", cmoId1);

        // now we have one existing sample in this request for the same patient
        SampleMetadata existingSample = getSampleMetadataWithCmoLabel("4324_1",
                cmoPatientId, "Xenograft", cmoId1);
        // get existing samples for given igo id and request id
        List<SampleMetadata> existingSamples = Arrays.asList(existingSample);

        IgoSampleManifest sample2 = getSampleMetadata("4324_2", cmoPatientId,
                SpecimenType.XENOGRAFT, NucleicAcid.RNA);
        String cmoId2 = cmoLabelGeneratorService.generateCmoSampleLabel(requestId, sample2,
                existingSamples, SAMPLES_BY_ALT_ID);

        // this test result remains as is since the nuc acid is different from existing ones and would
        // recieve a nuc acid count of 1 anyway
        Assertions.assertEquals("C-1235-X002-r01", cmoId2);
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

        IgoSampleManifest sample1 = getSampleMetadata("4324_1", cmoPatientId,
                SpecimenType.XENOGRAFT, NucleicAcid.DNA);
        String cmoId1 = cmoLabelGeneratorService.generateCmoSampleLabel(requestId,
                sample1, new ArrayList<>(), SAMPLES_BY_ALT_ID);
        Assertions.assertEquals("C-1235-X001-d01", cmoId1);

        // now we have one existing sample in this request for the same patient
        SampleMetadata existingSample = getSampleMetadataWithCmoLabel("4324_1",
                cmoPatientId, "Xenograft", cmoId1);
        // get existing samples for given igo id and request id
        List<SampleMetadata> existingSamples = Arrays.asList(existingSample);

        IgoSampleManifest sample2 = getSampleMetadata("4324_2", cmoPatientId,
                SpecimenType.PDX, NucleicAcid.DNA);
        String cmoId2 = cmoLabelGeneratorService.generateCmoSampleLabel(requestId,
                sample2, existingSamples, SAMPLES_BY_ALT_ID);

        // keeping this "NotEquals" comparison for provenance - nucleic acid counter is now resolved
        // on a unique sample basis (i.e., alt id) and not by total patient samples of a given nuc acid type
        Assertions.assertNotEquals("C-1235-X002-d02", cmoId2);
        Assertions.assertEquals("C-1235-X002-d01", cmoId2);
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

        IgoSampleManifest sample1 = getSampleMetadata("4324_1", cmoPatientId,
                SpecimenType.ORGANOID, NucleicAcid.DNA);
        String cmoId1 = cmoLabelGeneratorService.generateCmoSampleLabel(requestId,
                sample1, new ArrayList<>(), SAMPLES_BY_ALT_ID);
        Assertions.assertEquals("C-1235-G001-d01", cmoId1);

        // now we have one existing sample in this request for the same patient
        SampleMetadata existingSample = getSampleMetadataWithCmoLabel("4324_1",
                cmoPatientId, "Organoid", cmoId1);
        // get existing samples for given igo id and request id
        List<SampleMetadata> existingSamples = Arrays.asList(existingSample);

        IgoSampleManifest sample2 = getSampleMetadata("4324_2", cmoPatientId,
                SpecimenType.ORGANOID, NucleicAcid.DNA);
        String cmoId2 = cmoLabelGeneratorService.generateCmoSampleLabel(requestId,
                sample2, existingSamples, SAMPLES_BY_ALT_ID);

        // keeping this "NotEquals" comparison for provenance - nucleic acid counter is now resolved
        // on a unique sample basis (i.e., alt id) and not by total patient samples of a given nuc acid type
        Assertions.assertNotEquals("C-1235-G002-d02", cmoId2);
        Assertions.assertEquals("C-1235-G002-d01", cmoId2);
    }

    /**
     * Tests assertion of IGO sample NOT requiring a cmo label update since
     * all of the relevant metadata fields that are used to generate the label
     * match the existing IGO sample metadata used for the label.
     * Note: Even if the metadata does not contain updates that affect the label
     *  generated, there may still be updates to other metadata fields that are
     *  not used for generating or regenerating a cmo label.
     * @throws Exception
     */
    @Test
    public void testIgoSampleWithNoLabelChangingMetadataUpdates() throws Exception {
        String requestId = "5432_P";
        String cmoPatientId = "C-1235";
        List<SampleMetadata> existingSamples = new ArrayList<>();

        // sample 1 for patient w/igo id 4324_1
        String cmoId1 = "C-1235-G001-d01";
        SampleMetadata existingSample1 = getSampleMetadataWithCmoLabel("4324_1",
                cmoPatientId, "Organoid", cmoId1);
        existingSamples.add(existingSample1);

        // sample 2 for patient w/igo id 4324_2
        String cmoId2 = "C-1235-G002-d02";
        SampleMetadata existingSample2 = getSampleMetadataWithCmoLabel("4324_2",
                cmoPatientId, "Organoid", cmoId2);
        existingSamples.add(existingSample2);

        // sample 1 with an update that does not affect the generated cmo label
        IgoSampleManifest updatedSample = getSampleMetadata("4324_1", cmoPatientId,
                SpecimenType.ORGANOID, NucleicAcid.DNA);
        String updatedCmoLabel = cmoLabelGeneratorService.generateCmoSampleLabel(requestId,
                updatedSample, existingSamples, SAMPLES_BY_ALT_ID);

        // NOTE: now that the nucleic acid counter doesn't increment for unless it's
        // another sample of the same alt id, the 'updatedCmoLabel' returned should
        // have a nuc acid counter of 01
        // confirm that the label generated would still increment even though it will
        // be determined that the sample cmo label does NOT need to be updated for this sample
        Assertions.assertEquals("C-1235-G001-d01", updatedCmoLabel);
        Boolean needsUpdates =
                cmoLabelGeneratorService.igoSampleRequiresLabelUpdate(updatedCmoLabel, cmoId1);
        Assertions.assertFalse(needsUpdates);
    }

    /**
     * Tests assertion of IGO sample THAT DOES require a cmo label update since
     * at least one of the relevant metadata fields that are used to generate the label
     * differs from the corresponding metadata used for the label of the existing IGO sample.
     * @throws Exception
     */
    @Test
    public void testIgoSampleWithLabelChangingMetadataUpdates() throws Exception {
        String requestId = "5432_P";
        String cmoPatientId = "C-1235";
        List<SampleMetadata> existingSamples = new ArrayList<>();

        // sample 1 for patient w/igo id 4324_1
        String cmoId1 = "C-1235-G001-d01";
        SampleMetadata existingSample1 = getSampleMetadataWithCmoLabel("4324_1",
                cmoPatientId, "Organoid", cmoId1);
        existingSamples.add(existingSample1);

        // sample 2 for patient w/igo id 4324_2
        String cmoId2 = "C-1235-G002-d02";
        SampleMetadata existingSample2 = getSampleMetadataWithCmoLabel("4324_2",
                cmoPatientId, "Organoid", cmoId2);
        existingSamples.add(existingSample2);

        // sample 1 with an update that does affect the generated cmo label
        IgoSampleManifest updatedSample = getSampleMetadata("4324_1", cmoPatientId,
                SpecimenType.ORGANOID, NucleicAcid.RNA);
        String updatedCmoLabel = cmoLabelGeneratorService.generateCmoSampleLabel(requestId,
                updatedSample, existingSamples, SAMPLES_BY_ALT_ID);
        Assertions.assertEquals("C-1235-G001-r01", updatedCmoLabel);
        Boolean needsUpdates = cmoLabelGeneratorService.igoSampleRequiresLabelUpdate(updatedCmoLabel, cmoId1);
        Assertions.assertTrue(needsUpdates);
    }

    /**
     * Not ported from LIMS
     * Tests if igoSampleRequiresLabelUpdate returns true even when existing label is empty
     */
    @Test
    public void testRequiresUpdateWhenExistingLabelIsEmpty() throws Exception {
        Boolean needsUpdates1 = cmoLabelGeneratorService.igoSampleRequiresLabelUpdate("C-1235-G001-d01", "");
        Assertions.assertTrue(needsUpdates1);
    }

    private SampleMetadata getSampleMetadataWithCmoLabel(String igoId, String cmoPatientId,
            String specimenType, String cmoSampleName) {
        SampleMetadata sample = new SampleMetadata();
        sample.setPrimaryId(igoId);
        sample.setCmoPatientId(cmoPatientId);
        sample.setSampleClass(specimenType);
        sample.setCmoSampleName(cmoSampleName);
        return sample;
    }

    private IgoSampleManifest getSampleMetadata(String igoId, String cmoPatientId,
            SpecimenType specimenType, NucleicAcid naToExtract) {
        IgoSampleManifest sample = new IgoSampleManifest();
        sample.setIgoId(igoId);
        sample.setCmoPatientId(cmoPatientId);
        sample.setSpecimenType(specimenType.getValue());

        Map<String, String> cmoSampleIdFields = new HashMap<>();
        cmoSampleIdFields.put("naToExtract", naToExtract.getValue());
        sample.setCmoSampleIdFields(cmoSampleIdFields);
        return sample;
    }
}

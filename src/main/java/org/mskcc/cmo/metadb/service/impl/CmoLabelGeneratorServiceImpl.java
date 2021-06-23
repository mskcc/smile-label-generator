package org.mskcc.cmo.metadb.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mskcc.cmo.common.enums.CmoSampleClass;
import org.mskcc.cmo.common.enums.NucleicAcid;
import org.mskcc.cmo.common.enums.SampleOrigin;
import org.mskcc.cmo.common.enums.SampleType;
import org.mskcc.cmo.common.enums.SpecimenType;
import org.mskcc.cmo.metadb.model.SampleMetadata;
import org.mskcc.cmo.metadb.service.CmoLabelGeneratorService;
import org.mskcc.cmo.metadb.service.MetadbRestService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 *
 * @author ochoaa
 */
@Service
public class CmoLabelGeneratorServiceImpl implements CmoLabelGeneratorService {
    private static final Log LOG = LogFactory.getLog(CmoLabelGeneratorServiceImpl.class);
    // example: C-1235-X001-d
    public static final Pattern CMO_SAMPLE_ID_REGEX =
            Pattern.compile("^C-([a-zA-Z0-9]+)-([NTRMLUPSGX])([0-9]{3})-([dr])$");
    // example: JH123-12345T
    public static final Pattern CMO_CELLLINE_ID_REGEX =
            Pattern.compile("^([A-Za-z0-9]+)-([A-Za-z0-9]+)$");
    public static final String CMO_LABEL_SEPARATOR = "-";

    @Autowired
    private MetadbRestService metadbRestService;

    // globals for mapping sample type abbreviations
    private static Map<SpecimenType, String> SPECIMEN_TYPE_ABBREV_MAP = new HashMap<>();
    private static Map<SampleOrigin, String> SAMPLE_ORIGIN_ABBREV_MAP = new HashMap<>();
    private static Map<CmoSampleClass, String> SAMPLE_CLASS_ABBREV_MAP = new HashMap<>();
    private static final List<SampleOrigin> KNOWN_CFDNA_SAMPLE_ORIGINS =
            Arrays.asList(SampleOrigin.URINE,
                    SampleOrigin.CEREBROSPINAL_FLUID,
                    SampleOrigin.PLASMA,
                    SampleOrigin.WHOLE_BLOOD);
    private static final String SAMPLE_ORIGIN_ABBREV_DEFAULT = "T";

    @Autowired
    private void initSampleTypeAbbreviationMapping() {
        // set up mapping by specimen type
        SPECIMEN_TYPE_ABBREV_MAP.put(SpecimenType.PDX, "X");
        SPECIMEN_TYPE_ABBREV_MAP.put(SpecimenType.XENOGRAFT, "X");
        SPECIMEN_TYPE_ABBREV_MAP.put(SpecimenType.XENOGRAFTDERIVEDCELLLINE, "X");
        SPECIMEN_TYPE_ABBREV_MAP.put(SpecimenType.ORGANOID, "G");

        // set up mapping by sample origin
        SAMPLE_ORIGIN_ABBREV_MAP.put(SampleOrigin.URINE, "U");
        SAMPLE_ORIGIN_ABBREV_MAP.put(SampleOrigin.CEREBROSPINAL_FLUID, "S");
        SAMPLE_ORIGIN_ABBREV_MAP.put(SampleOrigin.PLASMA, "L");
        SAMPLE_ORIGIN_ABBREV_MAP.put(SampleOrigin.WHOLE_BLOOD, "L");

        // set up mapping by sample class
        SAMPLE_CLASS_ABBREV_MAP.put(CmoSampleClass.UNKNOWN_TUMOR, "T");
        SAMPLE_CLASS_ABBREV_MAP.put(CmoSampleClass.LOCAL_RECURRENCE, "R");
        SAMPLE_CLASS_ABBREV_MAP.put(CmoSampleClass.PRIMARY, "P");
        SAMPLE_CLASS_ABBREV_MAP.put(CmoSampleClass.RECURRENCE, "R");
        SAMPLE_CLASS_ABBREV_MAP.put(CmoSampleClass.METASTASIS, "M");
        SAMPLE_CLASS_ABBREV_MAP.put(CmoSampleClass.NORMAL, "N");
        SAMPLE_CLASS_ABBREV_MAP.put(CmoSampleClass.ADJACENT_NORMAL, "N");
        SAMPLE_CLASS_ABBREV_MAP.put(CmoSampleClass.ADJACENT_TISSUE, "T");
    }

    @Override
    public String generateCmoSampleLabel(SampleMetadata sampleMetadata) {
        // if sample is a cellline sample then generate a cmo cellline label
        if (isCmoCelllineSample(sampleMetadata)) {
            return generateCmoCelllineSampleLabel(sampleMetadata);
        }

        List<SampleMetadata> existingSamples = new ArrayList<>();
        try {
            existingSamples =
                    metadbRestService.getSampleMetadataListByCmoPatientId(sampleMetadata.getCmoPatientId());
        } catch (Exception ex) {
            LOG.error("Error during attempt to fetch existing samples by CMO Patient ID", ex);
        }
        String patientId = sampleMetadata.getCmoPatientId();

        // resolve sample type abbreviation
        String sampleTypeAbbreviation = resolveSampleTypeAbbreviation(sampleMetadata);
        if (sampleTypeAbbreviation == null) {
            throw new RuntimeException("Could not resolve sample type abbreviation from specimen type,"
                    + " sample origin, or sample class: " + sampleMetadata.toString());
        }

        // get next incremement value for cmo sample counter
        Integer nextIncrement = getNextSampleIncrement(existingSamples);
        String paddedIncrementString = getPaddedNextSampleIncrement(nextIncrement);

        // resolve nucleic acid abbreviation
        String nucleicAcidAbbreviation = resolveNucleicAcidAbbreviation(sampleMetadata);
        if (nucleicAcidAbbreviation == null) {
            throw new RuntimeException("Could not resolve nucleic acid abbreviation from sample "
                    + "type or naToExtract: " + sampleMetadata.toString());
        }

        String formattedCmoLabel = String.format("%s-%s%s-%s", patientId,
                sampleTypeAbbreviation, paddedIncrementString, nucleicAcidAbbreviation);
        return formattedCmoLabel;
    }

    /**
     * Resolve the nucleic acid abbreviation for the generated cmo sample label.
     * @param sampleMetadata
     * @return
     */
    private String resolveNucleicAcidAbbreviation(SampleMetadata sampleMetadata) {
        try {
            SampleType sampleType = SampleType.fromString(sampleMetadata.getSampleType());
            // resolve from sample type of not null
            // if pooled library then resolve value based on recipe
            switch (sampleType) {
                case POOLED_LIBRARY:
                    return sampleMetadata.getRecipe().equalsIgnoreCase("RNASeq")
                            ? "r" : "d";
                case DNA:
                case CFDNA:
                case DNA_LIBRARY:
                    return "d";
                case RNA:
                    return "r";
                default:
                    break;
            }
        } catch (Exception e) {
            LOG.warn("Could not resolve sample type acid from 'sampleType': " + sampleMetadata.toString());
        }
        // if nucleic acid abbreviation is still unknown then attempt to resolve from
        // sample metadata --> cmo sample id fields --> naToExtract
        try {
            NucleicAcid nucAcid = NucleicAcid.fromString(
                    sampleMetadata.getCmoSampleIdFields().get("naToExtract"));
            if (nucAcid != null) {
                switch (nucAcid) {
                    case DNA:
                    case DNA_AND_RNA:
                    case CFDNA:
                        return "d";
                    case RNA:
                        return "r";
                    default:
                        break;
                }
            }
        } catch (Exception e) {
            LOG.warn("Could not resolve nucleic acid from 'naToExtract': " + sampleMetadata.toString());
        }

        return null;
    }

    /**
     * Resolves the sample type abbreviation for the generated cmo sample label.
     * @param sampleMetadata
     * @return
     */
    private String resolveSampleTypeAbbreviation(SampleMetadata sampleMetadata) {
        try {
            SpecimenType specimenType = SpecimenType.fromValue(sampleMetadata.getSpecimenType());
            // if can be mapped directly from specimen type then use corresponding abbreviation
            if (SPECIMEN_TYPE_ABBREV_MAP.containsKey(specimenType)) {
                return SPECIMEN_TYPE_ABBREV_MAP.get(specimenType);
            }
            // if specimen type is cfDNA and sample origin is known type for cfDNA samples
            // then return corresponding abbreviation
            SampleOrigin sampleOrigin = SampleOrigin.fromValue(sampleMetadata.getSampleOrigin());
            if (sampleOrigin != null) {
                if (specimenType.equals(SpecimenType.CFDNA)
                        && KNOWN_CFDNA_SAMPLE_ORIGINS.contains(sampleOrigin)) {
                    return SAMPLE_ORIGIN_ABBREV_MAP.get(sampleOrigin);
                }
                // if specimen type is exosome then map abbreviation from sample origin or use default value
                if (specimenType.equals(SpecimenType.EXOSOME)) {
                    return SAMPLE_ORIGIN_ABBREV_MAP.getOrDefault(sampleOrigin, SAMPLE_ORIGIN_ABBREV_DEFAULT);
                }
            }
        } catch (Exception e) {
            LOG.warn("Could not resolve specimen type acid from 'specimenType': "
                    + sampleMetadata.toString());
        }
        // if abbreviation is still not resolved then try to resolve from sample class
        CmoSampleClass sampleClass = CmoSampleClass.fromValue(sampleMetadata.getCmoSampleClass());
        return SAMPLE_CLASS_ABBREV_MAP.get(sampleClass);
    }

    private String getPaddedNextSampleIncrement(Integer increment) {
        return StringUtils.leftPad(String.valueOf(increment), 3, "0");
    }

    private Integer getNextSampleIncrement(List<SampleMetadata> samples) {
        // return 1 if samples is empty
        if (samples.isEmpty()) {
            return 1;
        }
        // otherwise extract the max counter from the current set of samples
        // do not rely on the size of the list having the exact same counter
        // to prevent accidentally giving samples the same counter
        System.out.println("\n\nGetting next sample increment..");
        Integer maxIncrement = 0;
        for (SampleMetadata sample : samples) {

            System.out.println("Checking if CMO CELL LINE match on sample: " + sample.getCmoSampleName());
            if (CMO_CELLLINE_ID_REGEX.matcher(sample.getCmoSampleName()).find()) {
                System.out.println("CMO CELLLINE MATCH, CONTINUING");
                continue;
            }
            System.out.println("CHECKING CMO SAMPLE ID REGEX ON : " + sample.getCmoSampleName());
            Matcher matcher = CMO_SAMPLE_ID_REGEX.matcher(sample.getCmoSampleName());
            // increment assigned to the current sample is in group 3 of matcher
            if (matcher.find()) {
                Integer currentIncrement = Integer.valueOf(matcher.group(3));
                if (currentIncrement > maxIncrement) {
                    maxIncrement = currentIncrement;
                }
            }
        }
        return maxIncrement + 1;
    }

    private String generateCmoCelllineSampleLabel(SampleMetadata sample) {
        String formattedRequestId = sample.getRequestId().replaceAll("[-_]", "");
        return sample.getInvestigatorSampleId() + CMO_LABEL_SEPARATOR + formattedRequestId;
    }

    private Boolean isCmoCelllineSample(SampleMetadata sample) {
        // if specimen type is not cellline or cmo sample id fields are null then return false
        if (!sample.getSpecimenType().equalsIgnoreCase("CellLine")
                || sample.getCmoSampleIdFields() == null) {
            return  Boolean.FALSE;
        }
        String normalizedPatientId = sample.getCmoSampleIdFields().get("normalizedPatientId");
        return (!StringUtils.isBlank(normalizedPatientId)
                && !normalizedPatientId.equalsIgnoreCase("MRN_REDACTED"));
    }
}

package org.mskcc.cmo.metadb.service.impl;

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
import org.mskcc.cmo.metadb.model.igo.IgoSampleManifest;
import org.mskcc.cmo.metadb.service.CmoLabelGeneratorService;
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
            Pattern.compile("^C-([a-zA-Z0-9]+)-([NTRMLUPSGX])([0-9]{3})-([d|r])(.*$)");
    // example: JH123-12345T
    public static final Pattern CMO_CELLLINE_ID_REGEX =
            Pattern.compile("^([A-Za-z0-9]+)-([A-Za-z0-9]+)$");
    public static final String CMO_LABEL_SEPARATOR = "-";
    public static final Integer CMO_SAMPLE_COUNTER_GROUP = 3;
    public static final Integer CMO_SAMPLE_COUNTER_STRING_PADDING = 3;
    public static final Integer CMO_SAMPLE_NUCACID_ABBREV_GROUP = 4;
    public static final Integer CMO_SAMPLE_NUCACID_COUNTER_GROUP = 5;
    public static final Integer CMO_SAMPLE_NUCACID_COUNTER_PADDING = 2;

    // globals for mapping sample type abbreviations
    private static final Map<SpecimenType, String> SPECIMEN_TYPE_ABBREV_MAP = initSpecimenTypeAbbrevMap();
    private static final Map<SampleOrigin, String> SAMPLE_ORIGIN_ABBREV_MAP = initSampleOriginAbbrevMap();
    private static final Map<CmoSampleClass, String> SAMPLE_CLASS_ABBREV_MAP = initCmoSampleClassAbbrevMap();
    private static final List<SampleOrigin> KNOWN_CFDNA_SAMPLE_ORIGINS =
            Arrays.asList(SampleOrigin.URINE,
                    SampleOrigin.CEREBROSPINAL_FLUID,
                    SampleOrigin.PLASMA,
                    SampleOrigin.WHOLE_BLOOD);
    private static final String SAMPLE_ORIGIN_ABBREV_DEFAULT = "T";

    /**
     * Init specimen type abbreviation mappings.
     * @return
     */
    private static Map<SpecimenType, String> initSpecimenTypeAbbrevMap() {
        Map<SpecimenType, String> map = new HashMap<>();
        map.put(SpecimenType.PDX, "X");
        map.put(SpecimenType.XENOGRAFT, "X");
        map.put(SpecimenType.XENOGRAFTDERIVEDCELLLINE, "X");
        map.put(SpecimenType.ORGANOID, "G");
        return map;
    }

    /**
     * Init sample origin abbreviation mappings.
     * @return
     */
    private static Map<SampleOrigin, String> initSampleOriginAbbrevMap() {
        Map<SampleOrigin, String> map = new HashMap<>();
        map.put(SampleOrigin.URINE, "U");
        map.put(SampleOrigin.CEREBROSPINAL_FLUID, "S");
        map.put(SampleOrigin.PLASMA, "L");
        map.put(SampleOrigin.WHOLE_BLOOD, "L");
        return map;
    }

    /**
     * Init CMO sample class abbreviation mappings.
     * @return
     */
    private static Map<CmoSampleClass, String> initCmoSampleClassAbbrevMap() {
        Map<CmoSampleClass, String> map = new HashMap<>();
        map.put(CmoSampleClass.UNKNOWN_TUMOR, "T");
        map.put(CmoSampleClass.LOCAL_RECURRENCE, "R");
        map.put(CmoSampleClass.PRIMARY, "P");
        map.put(CmoSampleClass.RECURRENCE, "R");
        map.put(CmoSampleClass.METASTASIS, "M");
        map.put(CmoSampleClass.NORMAL, "N");
        map.put(CmoSampleClass.ADJACENT_NORMAL, "N");
        map.put(CmoSampleClass.ADJACENT_TISSUE, "T");
        return map;
    }

    @Override
    public String generateCmoSampleLabel(String requestId, IgoSampleManifest sampleManifest,
            List<SampleMetadata> existingSamples) {
        // if sample is a cellline sample then generate a cmo cellline label
        if (isCmoCelllineSample(sampleManifest)) {
            return generateCmoCelllineSampleLabel(requestId, sampleManifest);
        }
        String patientId = sampleManifest.getCmoPatientId();

        // resolve sample type abbreviation
        String sampleTypeAbbreviation = resolveSampleTypeAbbreviation(sampleManifest);
        if (sampleTypeAbbreviation == null) {
            throw new RuntimeException("Could not resolve sample type abbreviation from specimen type,"
                    + " sample origin, or sample class: " + sampleManifest.toString());
        }

        // get next incremement value for cmo sample counter
        Integer nextSampleCounter = getNextSampleIncrement(existingSamples);
        String paddedSampleCounter = getPaddedIncrementString(nextSampleCounter,
                CMO_SAMPLE_COUNTER_STRING_PADDING);

        // resolve nucleic acid abbreviation
        String nucleicAcidAbbreviation = resolveNucleicAcidAbbreviation(sampleManifest);
        if (nucleicAcidAbbreviation == null) {
            throw new RuntimeException("Could not resolve nucleic acid abbreviation from sample "
                    + "type or naToExtract: " + sampleManifest.toString());
        }
        // get next increment for nucleic acid abbreviation
        Integer nextNucAcidCounter = getNextNucleicAcidIncrement(nucleicAcidAbbreviation, existingSamples);
        String paddedNucAcidCounter = getPaddedIncrementString(nextNucAcidCounter,
                CMO_SAMPLE_NUCACID_COUNTER_PADDING);

        String formattedCmoLabel = String.format("%s-%s%s-%s%s", patientId,
                sampleTypeAbbreviation, paddedSampleCounter, nucleicAcidAbbreviation,
                paddedNucAcidCounter);
        return formattedCmoLabel;
    }

    /**
     * Resolve the nucleic acid abbreviation for the generated cmo sample label.
     * @param sampleManifest
     * @return
     */
    private String resolveNucleicAcidAbbreviation(IgoSampleManifest sampleManifest) {
        try {
            String sampleTypeString = sampleManifest.getCmoSampleIdFields().get("sampleType");
            SampleType sampleType = SampleType.fromString(sampleTypeString);
            // resolve from sample type if not null
            // if pooled library then resolve value based on recipe
            switch (sampleType) {
                case POOLED_LIBRARY:
                    String recipe = sampleManifest.getCmoSampleIdFields().get("recipe");
                    return recipe.equalsIgnoreCase("RNASeq")
                            ? "r" : "d";
                case DNA:
                case CFDNA:
                case DNA_LIBRARY:
                    return "d";
                case RNA:
                    return "r";
                default:
                    return "d";
            }
        } catch (Exception e) {
            LOG.debug("Could not resolve sample type acid from 'sampleType' - using default 'd'");
        }
        // if nucleic acid abbreviation is still unknown then attempt to resolve from
        // sample metadata --> cmo sample id fields --> naToExtract
        try {
            NucleicAcid nucAcid = NucleicAcid.fromString(
                    sampleManifest.getCmoSampleIdFields().get("naToExtract"));
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
            LOG.debug("Could not resolve nucleic acid from 'naToExtract' - using default 'd'");
            return "d";
        }

        return null;
    }

    /**
     * Resolves the sample type abbreviation for the generated cmo sample label.
     * @param sampleManifest
     * @return
     */
    private String resolveSampleTypeAbbreviation(IgoSampleManifest sampleManifest) {
        try {
            SpecimenType specimenType = SpecimenType.fromValue(sampleManifest.getSpecimenType());
            // if can be mapped directly from specimen type then use corresponding abbreviation
            if (SPECIMEN_TYPE_ABBREV_MAP.containsKey(specimenType)) {
                return SPECIMEN_TYPE_ABBREV_MAP.get(specimenType);
            }
            // if specimen type is cfDNA and sample origin is known type for cfDNA samples
            // then return corresponding abbreviation
            SampleOrigin sampleOrigin = SampleOrigin.fromValue(sampleManifest.getSampleOrigin());
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
            LOG.debug("Could not resolve specimen type acid from 'specimenType': "
                    + sampleManifest.toString());
        }
        // if abbreviation is still not resolved then try to resolve from sample class
        CmoSampleClass sampleClass = CmoSampleClass.fromValue(sampleManifest.getCmoSampleClass());
        return SAMPLE_CLASS_ABBREV_MAP.get(sampleClass);
    }

    /**
     * Returns a padded string with the provided increment and padding size.
     * @param increment
     * @param padding
     * @return String
     */
    private String getPaddedIncrementString(Integer increment, Integer padding) {
        return StringUtils.leftPad(String.valueOf(increment), padding, "0");
    }

    /**
     * Returns the next sample increment.
     * @param samples
     * @return
     */
    private Integer getNextSampleIncrement(List<SampleMetadata> samples) {
        // return 1 if samples is empty
        if (samples.isEmpty()) {
            return 1;
        }
        // otherwise extract the max counter from the current set of samples
        // do not rely on the size of the list having the exact same counter
        // to prevent accidentally giving samples the same counter
        Integer maxIncrement = 0;
        for (SampleMetadata sample : samples) {
            // skip cell line samples
            if (CMO_CELLLINE_ID_REGEX.matcher(sample.getCmoSampleName()).find()) {
                continue;
            }
            Matcher matcher = CMO_SAMPLE_ID_REGEX.matcher(sample.getCmoSampleName());
            // increment assigned to the current sample is in group 3 of matcher
            if (matcher.find()) {
                Integer currentIncrement = Integer.valueOf(matcher.group(CMO_SAMPLE_COUNTER_GROUP));
                if (currentIncrement > maxIncrement) {
                    maxIncrement = currentIncrement;
                }
            }
        }
        return maxIncrement + 1;
    }

    /**
     * Returns the nucleic acid increment. Counter will be a 2 digit integer value range
     * from 01-99 (values less < 10 are filled in with zeros '0' to preserve 2-digit format).
     * From the time of implementation the first sample for a particular Nucleic Acid get 01.
     * @param nucleicAcidAbbreviation
     * @param samples
     * @return Integer
     */
    private Integer getNextNucleicAcidIncrement(String nucleicAcidAbbreviation,
            List<SampleMetadata> samples) {
        if (samples.isEmpty()) {
            return 1;
        }
        // otherwise extract the max counter from the current set of samples
        // do not rely on the size of the list having the exact same counter
        // to prevent accidentally giving samples the same counter
        Integer maxIncrement = 0;
        for (SampleMetadata sample : samples) {
            // skip cell line samples
            if (CMO_CELLLINE_ID_REGEX.matcher(sample.getCmoSampleName()).find()) {
                continue;
            }
            Matcher matcher = CMO_SAMPLE_ID_REGEX.matcher(sample.getCmoSampleName());
            // increment assigned to the current sample is in group 3 of matcher
            if (matcher.find()) {
                // nucleic acid abbreviation determines which counters we consider
                // when iterating through sample list
                if (!matcher.group(CMO_SAMPLE_NUCACID_ABBREV_GROUP)
                        .equalsIgnoreCase(nucleicAcidAbbreviation)) {
                    continue;
                }
                Integer currentIncrement;
                if (matcher.group(CMO_SAMPLE_NUCACID_COUNTER_GROUP) == null
                        || matcher.group(CMO_SAMPLE_NUCACID_COUNTER_GROUP).isEmpty()) {
                    currentIncrement = 1;
                } else {
                    currentIncrement = Integer.valueOf(matcher.group(CMO_SAMPLE_NUCACID_COUNTER_GROUP));
                }
                if (currentIncrement > maxIncrement) {
                    maxIncrement = currentIncrement;
                }
            }
        }
        return maxIncrement + 1;
    }

    private String generateCmoCelllineSampleLabel(String requestId, IgoSampleManifest sample) {
        String formattedRequestId = requestId.replaceAll("[-_]", "");
        return sample.getInvestigatorSampleId() + CMO_LABEL_SEPARATOR + formattedRequestId;
    }

    private Boolean isCmoCelllineSample(IgoSampleManifest sample) {
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

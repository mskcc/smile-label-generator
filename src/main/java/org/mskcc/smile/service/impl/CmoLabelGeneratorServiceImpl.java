package org.mskcc.smile.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.logging.log4j.util.Strings;
import org.mskcc.smile.commons.enums.CmoSampleClass;
import org.mskcc.smile.commons.enums.NucleicAcid;
import org.mskcc.smile.commons.enums.SampleOrigin;
import org.mskcc.smile.commons.enums.SampleType;
import org.mskcc.smile.commons.enums.SpecimenType;
import org.mskcc.smile.model.SampleMetadata;
import org.mskcc.smile.model.Status;
import org.mskcc.smile.model.igo.IgoSampleManifest;
import org.mskcc.smile.service.CmoLabelGeneratorService;
import org.springframework.stereotype.Service;

/**
 *
 * @author ochoaa
 */
@Service
public class CmoLabelGeneratorServiceImpl implements CmoLabelGeneratorService {
    private final ObjectMapper mapper = new ObjectMapper();
    private static final Log LOG = LogFactory.getLog(CmoLabelGeneratorServiceImpl.class);
    // example: C-1235-X001-d01
    public static final Pattern CMO_SAMPLE_ID_REGEX =
            Pattern.compile("^C-([a-zA-Z0-9]+)-([NTRMLUPSGXF])([0-9]{3})-([d|r])(.*$)");
    // example: JH123-12345T
    public static final Pattern CMO_CELLLINE_ID_REGEX =
            Pattern.compile("^([A-Za-z0-9]+[A-Za-z0-9_]+)-([A-Za-z0-9]+)$");
    public static final String CMO_LABEL_SEPARATOR = "-";
    public static final Integer CMO_PATIENT_ID_GROUP = 1;
    public static final Integer CMO_SAMPLE_TYPE_ABBREV_GROUP = 2;
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
        map.put(CmoSampleClass.LOCAL_RECURRENCE, "T");
        map.put(CmoSampleClass.PRIMARY, "T");
        map.put(CmoSampleClass.RECURRENCE, "T");
        map.put(CmoSampleClass.METASTASIS, "T");
        map.put(CmoSampleClass.NORMAL, "N");
        map.put(CmoSampleClass.ADJACENT_NORMAL, "A");
        map.put(CmoSampleClass.ADJACENT_TISSUE, "T");
        return map;
    }

    /**
     * Compares the regex groups for 2 CMO labels generated for the same IGO sample.
     * The padded counter strings encoded in the cmo labels being compared are ignored.
     * Note: the 'same' IGO sample is determined based on IGO sample ID matching,
     *  or primaryId if sample metadata provided as universal schema format.
     * Groups compared:
     *  1. cmo patient id prefix
     *  2. sample type abbreviation
     *  3. nucleic acid abbreviation
     * @param newCmoLabel
     * @param existingCmoLabel
     * @return Boolean
     */
    @Override
    public Boolean igoSampleRequiresLabelUpdate(String newCmoLabel, String existingCmoLabel) {
        // if the labels match then just return false
        if (newCmoLabel.equals(existingCmoLabel)) {
            return Boolean.FALSE;
        }
        // proceed with regular (non-cell line) cmo sample label checking
        Matcher matcherNewCelllineLabel = CMO_CELLLINE_ID_REGEX.matcher(newCmoLabel);
        Matcher matcherNewLabel = CMO_SAMPLE_ID_REGEX.matcher(newCmoLabel);
        Matcher matcherExistingLabel = CMO_SAMPLE_ID_REGEX.matcher(existingCmoLabel);

        // if we have a cell line sample and the existing and new label generated do not match
        // then return true so that we update to the new cmo label generated
        if (matcherNewCelllineLabel.find() && !matcherNewLabel.find()) {
            return Boolean.TRUE;
        }

        if (!matcherExistingLabel.find() || !matcherNewLabel.find()) {
            if (matcherNewLabel.find() && !matcherExistingLabel.find()) {
                return Boolean.TRUE;
            }
            throw new IllegalStateException("New CMO label and existing CMO label do not meet CMO ID "
                    + "regex requirements: new = " + newCmoLabel + ", existingLabel = " + existingCmoLabel);
        }

        // compare cmo patient id prefix
        if (!compareMatcherGroups(matcherNewLabel, matcherExistingLabel, CMO_PATIENT_ID_GROUP)) {
            LOG.info("CMO patient ID differs between incoming IGO sample and matching IGO sample "
                    + "from database. Sample will be published to IGO_SAMPLE_UPDATE topic.");
            return Boolean.TRUE;
        }
        // compare sample type abbreviation
        if (!compareMatcherGroups(matcherNewLabel, matcherExistingLabel, CMO_SAMPLE_TYPE_ABBREV_GROUP)) {
            LOG.info("Sample Type abbreviation differs between incoming IGO sample and matching IGO sample "
                    + "from database. Sample will be published to IGO_SAMPLE_UPDATE topic.");
            return Boolean.TRUE;
        }
        // compare sample counter (may change if alt id numbering corrections are being made)
        if (!compareMatcherGroups(matcherNewLabel, matcherExistingLabel, CMO_SAMPLE_COUNTER_GROUP)) {
            LOG.info("Sample Type counter differs between incoming IGO sample and matching IGO sample "
                    + "from database. Sample will be published to IGO_SAMPLE_UPDATE topic.");
            return Boolean.TRUE;
        }
        // compare nucleic acid abbreviation
        if (!compareMatcherGroups(matcherNewLabel, matcherExistingLabel, CMO_SAMPLE_NUCACID_ABBREV_GROUP)) {
            LOG.info("Nucleic Acid abbreviation differs between incoming IGO sample and matching IGO sample "
                    + "from database. Sample will be published to IGO_SAMPLE_UPDATE topic.");
            return Boolean.TRUE;
        }
        // compare nucleic acid counter (may change if alt id numbering corrections are being made)
        if (!compareNucleicAcidCounterGroups(matcherNewLabel, matcherExistingLabel)) {
            LOG.info("Nucleic Acid counter differs between incoming IGO sample and matching IGO sample "
                    + "from database. Sample will be published to IGO_SAMPLE_UPDATE topic.");
            return Boolean.TRUE;
        }
        return Boolean.FALSE;
    }

    /**
     * Resolves and compares the nucleic acid counter. Returns true if both Matchers are the same.
     * @param matcher1
     * @param matcher2
     * @return Boolean
     */
    private Boolean compareNucleicAcidCounterGroups(Matcher matcher1, Matcher matcher2) {
        // resolve value for matcher 1
        Integer matcher1Counter;
        if (matcher1.group(CMO_SAMPLE_NUCACID_COUNTER_GROUP) == null
                || matcher1.group(CMO_SAMPLE_NUCACID_COUNTER_GROUP).isEmpty()) {
            matcher1Counter = 1;
        } else {
            matcher1Counter = Integer.valueOf(matcher1.group(CMO_SAMPLE_NUCACID_COUNTER_GROUP));
        }

        // resolve value for matcher 2
        Integer matcher2Counter;
        if (matcher2.group(CMO_SAMPLE_NUCACID_COUNTER_GROUP) == null
                || matcher2.group(CMO_SAMPLE_NUCACID_COUNTER_GROUP).isEmpty()) {
            matcher2Counter = 1;
        } else {
            matcher2Counter = Integer.valueOf(matcher2.group(CMO_SAMPLE_NUCACID_COUNTER_GROUP));
        }
        return matcher1Counter.equals(matcher2Counter);
    }

    /**
     * Returns true if the group from both Matchers are the same.
     * @param matcher1
     * @param matcher2
     * @param group
     * @return Boolean
     */
    private Boolean compareMatcherGroups(Matcher matcher1, Matcher matcher2, Integer group) {
        return matcher1.group(group).equalsIgnoreCase(matcher2.group(group));
    }

    @Override
    public String generateCmoSampleLabel(String requestId, IgoSampleManifest sampleManifest,
            List<SampleMetadata> existingSamples, List<SampleMetadata> samplesByAltId) {
        // if sample is a cellline sample then generate a cmo cellline label
        if (isCmoCelllineSample(sampleManifest)) {
            return generateCmoCelllineSampleLabel(requestId, sampleManifest.getInvestigatorSampleId());
        }

        // resolve sample type abbreviation
        String sampleTypeAbbreviation = resolveSampleTypeAbbreviation(sampleManifest);

        // resolve the sample counter value to use for the cmo label
        Integer sampleCounter =  resolveSampleIncrementValue(sampleManifest.getIgoId(),
                existingSamples, samplesByAltId);
        String paddedSampleCounter = getPaddedIncrementString(sampleCounter,
                CMO_SAMPLE_COUNTER_STRING_PADDING);

        // resolve nucleic acid abbreviation
        String nucleicAcidAbbreviation = resolveNucleicAcidAbbreviation(sampleManifest);
        if (nucleicAcidAbbreviation == null) {
            LOG.error("Could not resolve nucleic acid abbreviation from sample "
                    + "type or naToExtract: " + sampleManifest.toString());
            return null;
        }
        // get next increment for nucleic acid abbreviation
        Integer nextNucAcidCounter = getNextNucleicAcidIncrement(sampleManifest.getIgoId(),
                nucleicAcidAbbreviation, existingSamples, samplesByAltId);
        String paddedNucAcidCounter = getPaddedIncrementString(nextNucAcidCounter,
                CMO_SAMPLE_NUCACID_COUNTER_PADDING);

        String patientId = sampleManifest.getCmoPatientId();

        return getFormattedCmoSampleLabel(patientId, sampleTypeAbbreviation, paddedSampleCounter,
                nucleicAcidAbbreviation, paddedNucAcidCounter);
    }

    @Override
    public String generateCmoSampleLabel(SampleMetadata sampleMetadata,
            List<SampleMetadata> existingSamples, List<SampleMetadata> samplesByAltId) {
        // if sample is a cellline sample then generate a cmo cellline label
        if (isCmoCelllineSample(sampleMetadata.getSampleClass(), sampleMetadata.getCmoSampleIdFields())) {
            return generateCmoCelllineSampleLabel(sampleMetadata.getIgoRequestId(),
                    sampleMetadata.getInvestigatorSampleId());
        }

        // resolve sample type abbreviation
        String sampleTypeAbbreviation = resolveSampleTypeAbbreviation(sampleMetadata.getSampleClass(),
                sampleMetadata.getSampleOrigin(), sampleMetadata.getSampleType());
        if (sampleTypeAbbreviation == null) {
            LOG.error("Could not resolve sample type abbreviation "
                    + "from specimen type ('sampleClass'), sample origin, or sample "
                    + "class ('sampleType'): " + sampleMetadata.toString());
            return null;
        }

        // resolve the sample counter value to use for the cmo label
        Integer sampleCounter =  resolveSampleIncrementValue(sampleMetadata.getPrimaryId(),
                existingSamples, samplesByAltId);
        String paddedSampleCounter = getPaddedIncrementString(sampleCounter,
                CMO_SAMPLE_COUNTER_STRING_PADDING);

        // resolve nucleic acid abbreviation
        String sampleTypeString = sampleMetadata.getCmoSampleIdFields().get("sampleType");
        String recipe = sampleMetadata.getCmoSampleIdFields().get("recipe");
        String naToExtract = sampleMetadata.getCmoSampleIdFields().get("naToExtract");
        String nucleicAcidAbbreviation =
                resolveNucleicAcidAbbreviation(sampleTypeString, recipe, naToExtract);
        if (nucleicAcidAbbreviation == null) {
            LOG.error("Could not resolve nucleic acid abbreviation from sample "
                    + "type or naToExtract: " + sampleMetadata.toString());
            return null;
        }
        // get next increment for nucleic acid abbreviation
        Integer nextNucAcidCounter = getNextNucleicAcidIncrement(sampleMetadata.getPrimaryId(),
                nucleicAcidAbbreviation, existingSamples, samplesByAltId);
        String paddedNucAcidCounter = getPaddedIncrementString(nextNucAcidCounter,
                CMO_SAMPLE_NUCACID_COUNTER_PADDING);

        String patientId = sampleMetadata.getCmoPatientId();

        return getFormattedCmoSampleLabel(patientId, sampleTypeAbbreviation, paddedSampleCounter,
                nucleicAcidAbbreviation, paddedNucAcidCounter);
    }

    @Override
    public Status generateSampleStatus(String requestId, IgoSampleManifest sampleManifest,
            List<SampleMetadata> existingSamples) throws JsonProcessingException {
        Status sampleStatus = new Status();
        Map<String, String> validationReport = new HashMap<>();

        String sampleTypeAbbreviation = resolveSampleTypeAbbreviation(sampleManifest);
        if (sampleTypeAbbreviation == null
                || sampleTypeAbbreviation.equals("F")) {
            validationReport.put("sample type abbreviation",
                    "could not resolve based on specimenType, sampleOrigin, or sampleClass");
        }
        if (resolveNucleicAcidAbbreviation(sampleManifest) == null) {
            validationReport.put("nucleic acid abbreviation",
                    "could not resolve based on sampleType or naToExtract");
        }
        if (validationReport.isEmpty()) {
            sampleStatus.setValidationStatus(Boolean.TRUE);
        } else {
            sampleStatus.setValidationStatus(Boolean.FALSE);
        }
        sampleStatus.setValidationReport(mapper.writeValueAsString(validationReport));
        return sampleStatus;
    }

    @Override
    public Status generateSampleStatus(SampleMetadata sampleMetadata,
            List<SampleMetadata> existingSamples) throws JsonProcessingException {
        Status sampleStatus = new Status();
        Map<String, String> validationReport = new HashMap<>();

        String sampleTypeAbbreviation = resolveSampleTypeAbbreviation(sampleMetadata.getSampleClass(),
                sampleMetadata.getSampleOrigin(), sampleMetadata.getSampleType());
        if (sampleTypeAbbreviation == null
                || sampleTypeAbbreviation.equals("F")) {
            validationReport.put("sample type abbreviation",
                    "could not resolve based on specimenType, sampleOrigin, or sampleClass");
        }
        String sampleTypeString = sampleMetadata.getCmoSampleIdFields().get("sampleType");
        String recipe = sampleMetadata.getCmoSampleIdFields().get("recipe");
        String naToExtract = sampleMetadata.getCmoSampleIdFields().get("naToExtract");
        if (resolveNucleicAcidAbbreviation(sampleTypeString, recipe, naToExtract) == null) {
            validationReport.put("nucleic acid abbreviation",
                    "could not resolve based on sampleType or naToExtract");
        }
        if (validationReport.isEmpty()) {
            sampleStatus.setValidationStatus(Boolean.TRUE);
        } else {
            sampleStatus.setValidationStatus(Boolean.FALSE);
        }
        sampleStatus.setValidationReport(mapper.writeValueAsString(validationReport));
        return sampleStatus;
    }

    private String getFormattedCmoSampleLabel(String patientId, String sampleTypeAbbreviation,
            String paddedSampleCounter, String nucleicAcidAbbreviation, String paddedNucAcidCounter) {
        return String.format("%s-%s%s-%s%s", patientId, sampleTypeAbbreviation, paddedSampleCounter,
                nucleicAcidAbbreviation, paddedNucAcidCounter);
    }

    private String resolveNucleicAcidAbbreviation(String sampleTypeString,
            String recipe, String naToExtract) {
        try {
            SampleType sampleType = SampleType.fromString(sampleTypeString);
            // resolve from sample type if not null
            // if pooled library then resolve value based on recipe
            switch (sampleType) {
                case POOLED_LIBRARY:
                    return (recipe.equalsIgnoreCase("RNASeq") || recipe.equalsIgnoreCase("User_RNA"))
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
            LOG.warn("Could not resolve nucleic acid from 'sampleType' - using default 'd'");
        }
        // if nucleic acid abbreviation is still unknown then attempt to resolve from
        // sample metadata --> cmo sample id fields --> naToExtract
        try {
            NucleicAcid nucAcid = NucleicAcid.fromString(naToExtract);
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
            LOG.warn("Could not resolve nucleic acid from 'naToExtract' - using default 'd'");
            return "d";
        }

        return null;
    }

    /**
     * Resolve the nucleic acid abbreviation for the generated cmo sample label.
     * @param sampleManifest
     * @return
     */
    private String resolveNucleicAcidAbbreviation(IgoSampleManifest sampleManifest) {
        String sampleTypeString = sampleManifest.getCmoSampleIdFields().get("sampleType");
        String recipe = sampleManifest.getCmoSampleIdFields().get("recipe");
        String naToExtract = sampleManifest.getCmoSampleIdFields().get("naToExtract");
        return resolveNucleicAcidAbbreviation(sampleTypeString, recipe, naToExtract);
    }

    @Override
    public String resolveSampleTypeAbbreviation(String specimenTypeValue, String sampleOriginValue,
            String cmoSampleClassValue) {
        try {
            SpecimenType specimenType = SpecimenType.fromValue(specimenTypeValue);
            // if can be mapped directly from specimen type then use corresponding abbreviation
            if (SPECIMEN_TYPE_ABBREV_MAP.containsKey(specimenType)) {
                return SPECIMEN_TYPE_ABBREV_MAP.get(specimenType);
            }
            // if specimen type is cfDNA and sample origin is known type for cfDNA samples
            // then return corresponding abbreviation
            SampleOrigin sampleOrigin = SampleOrigin.fromValue(sampleOriginValue);
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
            LOG.warn("Could not resolve specimen type from 'specimenType': "
                    + specimenTypeValue + ". Attempting from sample class.");
        }

        // if abbreviation is still not resolved then try to resolve from sample class
        String sampleTypeAbbreviation = "F";
        try {
            CmoSampleClass sampleClass = CmoSampleClass.fromValue(cmoSampleClassValue);
            if (SAMPLE_CLASS_ABBREV_MAP.containsKey(sampleClass)) {
                sampleTypeAbbreviation = SAMPLE_CLASS_ABBREV_MAP.get(sampleClass);
            }
        } catch (Exception e) {
            // happens if cmoSampleClassValue is not found in CmoSampleClass
            // nothing to do here since since sampleTypeAbbreviation
            // is initialized to default 'F'
        }

        if (sampleTypeAbbreviation.equalsIgnoreCase("F")) {
            LOG.warn("Could not resolve sample type abbreviation from specimen type,"
                     + " sample origin, or sample class - using default 'F': (" + specimenTypeValue
                    + ", " + sampleOriginValue + ", " + cmoSampleClassValue + ")");
        }
        return sampleTypeAbbreviation;
    }

    /**
     * Resolves the sample type abbreviation for the generated cmo sample label.
     * @param sampleManifest
     * @return
     */
    private String resolveSampleTypeAbbreviation(IgoSampleManifest sampleManifest) {
        return resolveSampleTypeAbbreviation(sampleManifest.getSpecimenType(),
                sampleManifest.getSampleOrigin(), sampleManifest.getCmoSampleClass());
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
     * Given a primaryId and list of existing samples, returns the increment to use
     * for the padded sample counter string embedded in the cmo sample label.
     * @param primaryId
     * @param existingSamples
     * @return Integer
     */
    private Integer resolveSampleIncrementValue(String primaryId, List<SampleMetadata> existingSamples,
            List<SampleMetadata> samplesByAltId) {
        if ((existingSamples == null || existingSamples.isEmpty())
                && (samplesByAltId == null || samplesByAltId.isEmpty())) {
            return 1;
        }

        // if match isn't found by primary id then attempt to resolve count by checking increments
        // of samples with matching alt ids
        if (!samplesByAltId.isEmpty()) {
            List<Integer> altIdSampleCounters = new ArrayList<>();
            for (SampleMetadata sample : samplesByAltId) {
                Matcher matcher = CMO_SAMPLE_ID_REGEX.matcher(sample.getCmoSampleName());
                if (matcher.find()) {
                    try {
                        Integer increment = Integer.valueOf(matcher.group(CMO_SAMPLE_COUNTER_GROUP));
                        altIdSampleCounters.add(increment);
                    } catch (NoSuchElementException e) {
                        LOG.error("Could not resolve sample counter from label: "
                                + sample.getCmoSampleName());
                    }
                }
            }
            if (altIdSampleCounters.isEmpty()) {
                LOG.warn("Could not resolve sample counters from any of the samples matching the same "
                        + "ALT ID - returning counter as 1 by default");
                return 1;
            }
            if (altIdSampleCounters.size() == 1) {
                return altIdSampleCounters.get(0);
            } else {
                return Collections.min(altIdSampleCounters);
            }
        }

        // if we find a match by the primary id then return the increment parsed from
        // the matching sample's current cmo label
        for (SampleMetadata sample : existingSamples) {
            if (sample.getPrimaryId().equalsIgnoreCase(primaryId)) {
                Matcher matcher = CMO_SAMPLE_ID_REGEX.matcher(sample.getCmoSampleName());
                if (matcher.find()) {
                    Integer currentIncrement = Integer.valueOf(matcher.group(CMO_SAMPLE_COUNTER_GROUP));
                    return currentIncrement;
                }
            }
        }

        // if there aren't any existing samples by the same alt id then this is a new sample specimen for the
        // current patient so the sample increment for the sample cmo label will be based on number of other
        // existing patient samples
        return getNextSampleIncrement(existingSamples);
    }

    /**
     * Returns the next sample increment.
     * @param samples
     * @return Integer
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
            // skip samples without a defined cmo sample label
            if (StringUtils.isBlank(sample.getCmoSampleName())) {
                continue;
            }
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
     * @param existingSamples
     * @return Integer
     */
    private Integer getNextNucleicAcidIncrement(String primaryId, String nucleicAcidAbbreviation,
            List<SampleMetadata> existingSamples, List<SampleMetadata> samplesByAltId) {
        if (existingSamples.isEmpty() && samplesByAltId.isEmpty()) {
            return 1;
        }

        if (!samplesByAltId.isEmpty()) {
            Integer maxIncrement = 0;
            Set<Integer> nucAcidCountersByAltId = new HashSet<>();
            Integer nucAcidCounterByMatchingPrimaryId = -1;
            for (SampleMetadata sample : samplesByAltId) {
                // ignore samples with empty cmo sample labels
                if (StringUtils.isBlank(sample.getCmoSampleName())) {
                    continue;
                }
                // skip cell line samples as well
                if (CMO_CELLLINE_ID_REGEX.matcher(sample.getCmoSampleName()).find()) {
                    continue;
                }

                // if sample cmo label does not meet matcher criteria then skip
                Matcher matcher = CMO_SAMPLE_ID_REGEX.matcher(sample.getCmoSampleName());
                if (!matcher.find()) {
                    continue;
                }

                // skip labels that do not match the current nucleic acid abbreviation
                String currentNucAcidAbbreviation = matcher.group(CMO_SAMPLE_NUCACID_ABBREV_GROUP);
                if (!currentNucAcidAbbreviation.equals(nucleicAcidAbbreviation)) {
                    continue;
                }

                Integer currentIncrement;
                if (matcher.group(CMO_SAMPLE_NUCACID_COUNTER_GROUP) == null
                        || matcher.group(CMO_SAMPLE_NUCACID_COUNTER_GROUP).isEmpty()) {
                    currentIncrement = 1;
                } else {
                    currentIncrement = Integer.valueOf(matcher.group(CMO_SAMPLE_NUCACID_COUNTER_GROUP));
                }
                nucAcidCountersByAltId.add(currentIncrement);

                // if primary id match is found then we know that this sample isn't actually being
                // reprocessed and likely received some other metadata update that should not affect
                // the label generation as far as the nuc acid counter is concerned
                if (sample.getPrimaryId().equals(primaryId)) {
                    nucAcidCounterByMatchingPrimaryId = currentIncrement;
                }

                // update max increment if needed
                if (currentIncrement > maxIncrement) {
                    maxIncrement = currentIncrement;
                }
            }

            // if nuc acid counters list is empty then return 1 by default
            if (nucAcidCountersByAltId.isEmpty()) {
                return 1;
            }

            // if the max increment does not match the length of the samples matching the alt id
            // then return the nucleic acid counter as the size of matching samples by alt id
            // note: there's a step in the message handler that will ensure that a unique cmo label
            // is assigned to the sample being interrogated so there's no need to worry about
            // assigning a used nucleic acid counter during this step
            if (maxIncrement != nucAcidCountersByAltId.size()) {
                return nucAcidCountersByAltId.size();
            }

            // figure out what the next consecutive nucleic acid counter is and return
            return getNextConsecutiveCounter(nucAcidCountersByAltId);
        }

        // always return 1 by default if there aren't any samples matching by alt id since the
        // nuc acid counter should be resolved on a per-unique sample basis and not
        // by total patient samples
        return 1;
    }

    private Integer getNextConsecutiveCounter(Set<Integer> counters) {
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

    private String generateCmoCelllineSampleLabel(String requestId, String sampleInvestigatorId) {
        String formattedRequestId = requestId.replaceAll("[-_]", "");
        return sampleInvestigatorId + CMO_LABEL_SEPARATOR + formattedRequestId;
    }

    private Boolean isCmoCelllineSample(String specimenType, Map<String, String> cmoSampleIdFields) {
        // if specimen type is not cellline or cmo sample id fields are null then return false
        if (!specimenType.equalsIgnoreCase("CellLine")
                || cmoSampleIdFields == null) {
            return  Boolean.FALSE;
        }
        String normalizedPatientId = cmoSampleIdFields.get("normalizedPatientId");
        return (!StringUtils.isBlank(normalizedPatientId)
                && !normalizedPatientId.equalsIgnoreCase("MRN_REDACTED"));
    }

    private Boolean isCmoCelllineSample(IgoSampleManifest sample) {
        return isCmoCelllineSample(sample.getSpecimenType(), sample.getCmoSampleIdFields());
    }

    @Override
    public String generateValidationReport(String originalJson, String filteredJson, Boolean isSample)
            throws JsonProcessingException {
        StringBuilder builder = new StringBuilder();
        String requestId = getRequestId(originalJson);
        Map<String, Object> filteredJsonMap = mapper.readValue(filteredJson, Map.class);
        // keeps track if there's anything to report or not. if still true after all checks
        // then return null
        Boolean allValid = Boolean.TRUE;

        // if request-level status is missing from the filtered json then
        // a critical error likely occurred, in which case the original json
        // would be more helpful to have as a reference when debugging the error
        if (!filteredJsonMap.containsKey("status")) {
            allValid = Boolean.FALSE;
            builder.append("[label-generator] Request JSON missing validation report ('status') ");
            builder.append("post-validation: Original JSON contents: ")
                    .append(originalJson).append(" Filtered JSON contents: ")
                    .append(filteredJson);
        } else {
            Map<String, Object> statusMap = (Map<String, Object>) filteredJsonMap.get("status");
            Boolean isEmptyValidationReport = Boolean.FALSE;
            if (!statusMap.get("validationReport").toString().equals("{}")) {
                isEmptyValidationReport = Boolean.TRUE;
            }

            // if request validation report is not empty then log for ddog
            if (!isEmptyValidationReport) {
                allValid = Boolean.FALSE;
                if (isSample) {
                    String sampleId = ObjectUtils.firstNonNull(
                            filteredJsonMap.get("igoId"), filteredJsonMap.get("primaryId")).toString();
                    builder.append("[label-generator] Validation report for sample '")
                            .append(sampleId)
                            .append("': ")
                            .append(mapper.writeValueAsString(statusMap));
                } else {
                    builder.append("[label-generator] Request-level status and validation report ")
                            .append("for request '")
                            .append(requestId)
                            .append("': ")
                            .append(mapper.writeValueAsString(statusMap));
                }
            }

            if (!isSample) {
                // check validation status for each sample individually as well and
                // add contents to report for ddog
                Object[] sampleList = mapper.convertValue(filteredJsonMap.get("samples"),
                    Object[].class);
                for (Object s : sampleList) {
                    Map<String, Object> sampleMap = mapper.convertValue(s, Map.class);
                    Map<String, Object> sampleStatusMap = mapper.convertValue(sampleMap.get("status"),
                            Map.class);
                    Map<String, String> sampleValidationReport = new HashMap<>();
                    if (!sampleStatusMap.get("validationReport").toString().equals("{}")) {
                        sampleValidationReport = mapper.readValue(
                                sampleStatusMap.get("validationReport").toString(), Map.class);
                    }

                    try {
                        String sampleId = ObjectUtils.firstNonNull(
                                sampleMap.get("igoId"), sampleMap.get("primaryId")).toString();
                        if (!sampleValidationReport.isEmpty()) {
                            allValid = Boolean.FALSE;
                            builder.append("\n[label-generator] Validation report for sample '")
                                    .append(sampleId)
                                    .append("': ")
                                    .append(mapper.writeValueAsString(sampleStatusMap));
                        }
                    } catch (NullPointerException e) {
                        builder.append("\n[label-generator] No known identifiers in current sample data: ")
                                .append(mapper.writeValueAsString(sampleMap))
                                .append(", Validation report for unknown sample: ")
                                .append(mapper.writeValueAsString(sampleStatusMap));
                    }
                }
            }
        }
        // if allValid is still true then there wasn't anything to report at the request
        // or sample level.. return null
        return allValid ? null : builder.toString();
    }

    private String getRequestId(String json) throws JsonProcessingException {
        if (isBlank(json)) {
            return null;
        }
        Map<String, Object> jsonMap = mapper.readValue(json, Map.class);
        return getRequestId(jsonMap);
    }

    private String getRequestId(Map<String, Object> jsonMap) throws JsonProcessingException {
        if (jsonMap.containsKey("requestId")) {
            return jsonMap.get("requestId").toString();
        }
        if (jsonMap.containsKey("igoRequestId")) {
            return jsonMap.get("igoRequestId").toString();
        }
        if (jsonMap.containsKey("additionalProperties")) {
            Map<String, String> additionalProperties = mapper.convertValue(
                    jsonMap.get("additionalProperties"), Map.class);
            if (additionalProperties.containsKey("requestId")) {
                return additionalProperties.get("requestId");
            }
            if (additionalProperties.containsKey("igoRequestId")) {
                return additionalProperties.get("igoRequestId");
            }
        }
        return null;
    }

    private Boolean isBlank(String value) {
        return (Strings.isBlank(value) || value.equals("null"));
    }

    @Override
    public String incrementNucleicAcidCounter(String cmoLabel) {
        Matcher matcher = CMO_SAMPLE_ID_REGEX.matcher(cmoLabel);
        // first make sure that we are dealing with a "C-" style label
        if (!matcher.find()) {
            return null;
        }

        // parse the nucleic acid counter group
        Integer nucAcidIncrement;
        if (matcher.group(CMO_SAMPLE_NUCACID_COUNTER_GROUP) == null
                || matcher.group(CMO_SAMPLE_NUCACID_COUNTER_GROUP).isEmpty()) {
            nucAcidIncrement = 1;
        } else {
            nucAcidIncrement = Integer.valueOf(matcher.group(CMO_SAMPLE_NUCACID_COUNTER_GROUP));
        }
        // we only call this function when the nuc acid counter needs to be incremented
        nucAcidIncrement++;
        String paddedNucAcidCounter = getPaddedIncrementString(nucAcidIncrement,
                CMO_SAMPLE_NUCACID_COUNTER_PADDING);

        // everything else in the label can remain as is
        String patientId = "C-" + matcher.group(CMO_PATIENT_ID_GROUP);
        String sampleTypeAbbreviation = matcher.group(CMO_SAMPLE_TYPE_ABBREV_GROUP);
        String paddedSampleCounter = matcher.group(CMO_SAMPLE_COUNTER_GROUP);
        String nucleicAcidAbbreviation = matcher.group(CMO_SAMPLE_NUCACID_ABBREV_GROUP);

        return getFormattedCmoSampleLabel(patientId, sampleTypeAbbreviation, paddedSampleCounter,
                nucleicAcidAbbreviation, paddedNucAcidCounter);
    }
}

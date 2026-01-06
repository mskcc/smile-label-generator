package org.mskcc.smile.service.util;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Serializable;
import java.util.Map;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang3.ObjectUtils;

/**
 *
 * @author ochoaa
 */
public class CmoLabelParts implements Serializable, Cloneable {
    @JsonIgnore
    private final ObjectMapper mapper = new ObjectMapper();
    private String primaryId; // igo => igoId, smile => primaryId
    private String altId; // igo => altId, smile => additionalProperties:altId
    private String cmoPatientId; // smile/igo => cmoPatientId
    private String sampleClass; // igo => specimenType, smile => sampleClass
    private String sampleOrigin; // smile/igo => sampleOrigin
    private String sampleType; // igo => cmoSampleClass, smile => sampleType
    private String detailedSampleType; // smile/igo => cmoSampleIdFields:sampleType
    private String naToExtract; // smile/igo => cmoSampleIdFields:naToExtract
    private String normalizedPatientId; // smile/igo => cmoSampleIdFields:normalizedPatientId
    private String recipe; // smile/igo => cmoSampleIdFields:recipe + smile => genePanel
    private String baitSet; // smile/igo => baitSet
    private String investigatorSampleId; // smile/igo => investigatorSampleId
    private String igoRequestId; // igo => requestId, smile => igoRequestId
    @JsonIgnore
    private String origSampleJsonStr;
    private String cmoSampleName; // igo/smile => cmoSampleName
    private String tumorOrNormal; // igo/smile => tumorOrNormal
    private Object isCmoSample; // igo => request:isCmoRequest, smile => additionalProperties:isCmoSample

    public CmoLabelParts() {}

    /**
     * Constructor for CmoLabelParts.
     * @param sampleMap
     * @param requestId
     * @param isCmoSample
     * @throws JsonProcessingException
     */
    public CmoLabelParts(Map<String, Object> sampleMap, String requestId, Object isCmoSample)
            throws JsonProcessingException {
        this.origSampleJsonStr = mapper.writeValueAsString(sampleMap);

        // fields common to both smile and igo sample data
        this.cmoPatientId = getString(sampleMap, "cmoPatientId");
        this.sampleOrigin = getString(sampleMap, "sampleOrigin");
        this.investigatorSampleId = getString(sampleMap, "investigatorSampleId");
        this.baitSet = getString(sampleMap, "baitSet");
        this.cmoSampleName = getString(sampleMap, "cmoSampleName");
        this.tumorOrNormal = getString(sampleMap, "tumorOrNormal");

        Map<String, Object> cmoSampleIdFields
                = mapper.convertValue(sampleMap.get("cmoSampleIdFields"), Map.class);
        this.detailedSampleType = getString(cmoSampleIdFields, "sampleType");
        this.naToExtract = getString(cmoSampleIdFields, "naToExtract");
        this.normalizedPatientId = getString(cmoSampleIdFields, "normalizedPatientId");
        this.recipe = getString(cmoSampleIdFields, "recipe"); // same value as smile => genePanel

        // resolve fields based on the map keys present
        this.primaryId = sampleMap.containsKey("igoId")
                ? getString(sampleMap, "igoId") : getString(sampleMap, "primaryId");
        this.sampleClass = sampleMap.containsKey("specimenType")
                ? getString(sampleMap, "specimenType") : getString(sampleMap, "sampleClass");
        this.sampleType = sampleMap.containsKey("cmoSampleClass")
                ? getString(sampleMap, "cmoSampleClass") : getString(sampleMap, "sampleType");

        Map<String, Object> additionalProperties
                = mapper.convertValue(sampleMap.get("additionalProperties"), Map.class);
        this.igoRequestId = requestId != null ? requestId : ObjectUtils.firstNonNull(
                getString(additionalProperties, "igoRequestId"),
                getString(additionalProperties, "requestId"));
        this.altId = sampleMap.containsKey("altid")
                ? getString(sampleMap, "altid") : getString(additionalProperties, "altId");
        if (isCmoSample != null) {
            this.isCmoSample = isCmoSample;
        } else if (additionalProperties != null) {
            this.isCmoSample = additionalProperties.get("isCmoSample");
        }
    }

    /**
     * @return the primaryId
     */
    public String getPrimaryId() {
        return primaryId;
    }

    /**
     * @param primaryId the primaryId to set
     */
    public void setPrimaryId(String primaryId) {
        this.primaryId = primaryId;
    }

    /**
     * @return the altId
     */
    public String getAltId() {
        return altId;
    }

    /**
     * @param altId the altId to set
     */
    public void setAltId(String altId) {
        this.altId = altId;
    }

    /**
     * @return the cmoPatientId
     */
    public String getCmoPatientId() {
        return cmoPatientId;
    }

    /**
     * @param cmoPatientId the cmoPatientId to set
     */
    public void setCmoPatientId(String cmoPatientId) {
        this.cmoPatientId = cmoPatientId;
    }

    /**
     * @return the sampleClass
     */
    public String getSampleClass() {
        return sampleClass;
    }

    /**
     * @param sampleClass the sampleClass to set
     */
    public void setSampleClass(String sampleClass) {
        this.sampleClass = sampleClass;
    }

    /**
     * @return the sampleOrigin
     */
    public String getSampleOrigin() {
        return sampleOrigin;
    }

    /**
     * @param sampleOrigin the sampleOrigin to set
     */
    public void setSampleOrigin(String sampleOrigin) {
        this.sampleOrigin = sampleOrigin;
    }

    /**
     * @return the sampleType
     */
    public String getSampleType() {
        return sampleType;
    }

    /**
     * @param sampleType the sampleType to set
     */
    public void setSampleType(String sampleType) {
        this.sampleType = sampleType;
    }

    /**
     * @return the detailedSampleType
     */
    public String getDetailedSampleType() {
        return detailedSampleType;
    }

    /**
     * @param detailedSampleType the detailedSampleType to set
     */
    public void setDetailedSampleType(String detailedSampleType) {
        this.detailedSampleType = detailedSampleType;
    }

    /**
     * @return the naToExtract
     */
    public String getNaToExtract() {
        return naToExtract;
    }

    /**
     * @param naToExtract the naToExtract to set
     */
    public void setNaToExtract(String naToExtract) {
        this.naToExtract = naToExtract;
    }

    /**
     * @return the normalizedPatientId
     */
    public String getNormalizedPatientId() {
        return normalizedPatientId;
    }

    /**
     * @param normalizedPatientId the normalizedPatientId to set
     */
    public void setNormalizedPatientId(String normalizedPatientId) {
        this.normalizedPatientId = normalizedPatientId;
    }

    /**
     * @return the recipe
     */
    public String getRecipe() {
        return recipe;
    }

    /**
     * @param recipe the recipe to set
     */
    public void setRecipe(String recipe) {
        this.recipe = recipe;
    }

    /**
     * @return the genePanel
     */
    public String getGenePanel() {
        return baitSet;
    }

    /**
     * @param genePanel the genePanel to set
     */
    public void setGenePanel(String genePanel) {
        this.baitSet = genePanel;
    }

    /**
     * @return the investigatorSampleId
     */
    public String getInvestigatorSampleId() {
        return investigatorSampleId;
    }

    /**
     * @param investigatorSampleId the investigatorSampleId to set
     */
    public void setInvestigatorSampleId(String investigatorSampleId) {
        this.investigatorSampleId = investigatorSampleId;
    }

    /**
     * @return the igoRequestId
     */
    public String getIgoRequestId() {
        return igoRequestId;
    }

    /**
     * @param igoRequestId the igoRequestId to set
     */
    public void setIgoRequestId(String igoRequestId) {
        this.igoRequestId = igoRequestId;
    }

    /**
     * @return the origSampleJsonStr
     */
    public String getOrigSampleJsonStr() {
        return origSampleJsonStr;
    }

    /**
     * @param origSampleJsonStr the origSampleJsonStr to set
     */
    public void setOrigSampleJsonStr(String origSampleJsonStr) {
        this.origSampleJsonStr = origSampleJsonStr;
    }

    /**
     * @return the cmoSampleName
     */
    public String getCmoSampleName() {
        return cmoSampleName;
    }

    /**
     * @param cmoSampleName the cmoSampleName to set
     */
    public void setCmoSampleName(String cmoSampleName) {
        this.cmoSampleName = cmoSampleName;
    }

    /**
     * @return the tumorOrNormal
     */
    public String getTumorOrNormal() {
        return tumorOrNormal;
    }

    /**
     * @param tumorOrNormal the tumorOrNormal to set
     */
    public void setTumorOrNormal(String tumorOrNormal) {
        this.tumorOrNormal = tumorOrNormal;
    }

    /**
     * @return the isCmoSample
     */
    public Object getIsCmoSample() {
        return isCmoSample;
    }

    /**
     * @param isCmoSample the isCmoSample to set
     */
    public void setIsCmoSample(Object isCmoSample) {
        this.isCmoSample = isCmoSample;
    }

    private String getString(Map<String, Object> map, String key) {
        return (map == null || !map.containsKey(key) || map.get(key) == null)
                ? null : map.get(key).toString();
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone(); // return shallow copy
    }
}

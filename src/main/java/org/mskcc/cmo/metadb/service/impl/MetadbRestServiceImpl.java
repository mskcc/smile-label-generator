package org.mskcc.cmo.metadb.service.impl;

import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContexts;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.mskcc.cmo.metadb.model.SampleMetadata;
import org.mskcc.cmo.metadb.service.MetadbRestService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;

/**
 *
 * @author ochoaa
 */
@Service
public class MetadbRestServiceImpl implements MetadbRestService {
    private static final Log LOG = LogFactory.getLog(MetadbRestServiceImpl.class);

    @Value("${metadb_server.base_url}")
    private String metadbBaseUrl;

    @Value("${metadb_server.pt_samples_endpoint}")
    private String metadbPtSamplesEndpoint;

    @Override
    public List<SampleMetadata> getSampleMetadataListByCmoPatientId(String cmoPatientId) throws Exception {
        String patientSamplesUrl = metadbBaseUrl + metadbPtSamplesEndpoint + cmoPatientId;

        RestTemplate restTemplate = getRestTemplate();
        HttpEntity<List<SampleMetadata>> requestEntity = getRequestEntity();
        List<SampleMetadata> samples = new ArrayList<>();
        try {
            ResponseEntity responseEntity = restTemplate.exchange(patientSamplesUrl,
                HttpMethod.GET, requestEntity, SampleMetadata[].class);
            samples = Arrays.asList((SampleMetadata[]) responseEntity.getBody());
        } catch (HttpServerErrorException e) {
            if (e.getStatusCode().equals(HttpStatus.INTERNAL_SERVER_ERROR)) {
                LOG.error("Error encountered during attempt to fetch sample metadata list for "
                        + "CMO Patient ID: '" + cmoPatientId + "'", e);
            }
        }
        return samples;
    }

    /**
     * Returns rest template that by passes SSL cert check.
     * @return RestTemplate
     * @throws Exception
     */
    private RestTemplate getRestTemplate() throws Exception {
        TrustStrategy acceptingTrustStrategy = (X509Certificate[] chain, String authType) -> true;
        HostnameVerifier hostnameVerifier = (s, sslSession) -> true;
        SSLContext sslContext = SSLContexts.custom().loadTrustMaterial(null, acceptingTrustStrategy).build();
        SSLConnectionSocketFactory csf = new SSLConnectionSocketFactory(sslContext, hostnameVerifier);
        CloseableHttpClient httpClient = HttpClients.custom().setSSLSocketFactory(csf).build();
        HttpComponentsClientHttpRequestFactory requestFactory = new HttpComponentsClientHttpRequestFactory();
        requestFactory.setHttpClient(httpClient);
        return new RestTemplate(requestFactory);
    }

    /**
     * Returns request entity.
     * @return HttpEntity
     */
    private HttpEntity getRequestEntity() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        return new HttpEntity<Object>(headers);
    }

}

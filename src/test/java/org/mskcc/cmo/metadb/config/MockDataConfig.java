package org.mskcc.cmo.metadb.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.*;
import java.util.*;
import org.mskcc.cmo.common.FileUtil;
import org.mskcc.cmo.metadb.model.MockJsonTestData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

/**
 *
 * @author ochoaa
 */
@Configuration
@ComponentScan(basePackages = "org.mskcc.cmo.common.*")
public class MockDataConfig {
    private final ObjectMapper mapper = new ObjectMapper();
    private final String MOCKED_REQUEST_DATA_DETAILS_FILEPATH = "data/mocked_request_data_details.txt";
    private final String MOCKED_JSON_DATA_DIR = "data";

    private ClassPathResource mockJsonTestDataResource;
    @Autowired
    private void setMockJsonTestDataResource() {
        this.mockJsonTestDataResource = new ClassPathResource(MOCKED_JSON_DATA_DIR);
    }

    @Autowired
    private FileUtil fileUtil;

    @Bean
    public FileUtil fileUtil() {
        return fileUtil;
    }

    private Map<String, MockJsonTestData> mockedRequestJsonDataMap;
    @Bean(name = "mockedRequestJsonDataMap")
    public Map<String, MockJsonTestData> mockedRequestJsonDataMap() throws IOException {
        this.mockedRequestJsonDataMap = new HashMap<>();
        ClassPathResource jsonDataDetailsResource = new ClassPathResource(MOCKED_REQUEST_DATA_DETAILS_FILEPATH);
        BufferedReader reader = new BufferedReader(new FileReader(jsonDataDetailsResource.getFile()));
        List<String> columns = new ArrayList<>();
        String line;
        while ((line = reader.readLine()) != null) {
            String[] data = line.split("\t");
            if (columns.isEmpty()) {
                columns = Arrays.asList(data);
                continue;
            }
            String identifier = data[columns.indexOf("identifier")];
            String filepath = data[columns.indexOf("filepath")];
            String description = data[columns.indexOf("description")];
            mockedRequestJsonDataMap.put(identifier, createMockJsonTestData(identifier, filepath, description));
        }
        reader.close();
        return mockedRequestJsonDataMap;
    }

    private MockJsonTestData createMockJsonTestData(String identifier, String filepath, String description) throws IOException {
        String jsonString = loadMockRequestJsonTestData(filepath);
        return new MockJsonTestData(identifier, filepath, description, jsonString);
    }

    private String loadMockRequestJsonTestData(String filepath) throws IOException {
        ClassPathResource res = new ClassPathResource(mockJsonTestDataResource.getPath() + File.separator + filepath);
        Map<String, Object> filedata = mapper.readValue(res.getFile(), Map.class);
        return mapper.writeValueAsString(filedata);
    }

}

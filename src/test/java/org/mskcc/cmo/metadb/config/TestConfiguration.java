package org.mskcc.cmo.metadb.config;

import org.mskcc.cmo.common.FileUtil;
import org.mskcc.cmo.metadb.service.CmoLabelGeneratorService;
import org.mskcc.cmo.metadb.service.impl.CmoLabelGeneratorServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

/**
 *
 * @author ochoaa
 */
@Configuration
@ComponentScan(basePackages = "org.mskcc.cmo.common.*")
public class TestConfiguration {

    @Autowired
    private FileUtil fileUtil;

    @Bean
    public FileUtil fileUtil() {
        return fileUtil;
    }

    @Bean
    public CmoLabelGeneratorService cmoLabelGeneratorService() {
        return new CmoLabelGeneratorServiceImpl();
    }

}

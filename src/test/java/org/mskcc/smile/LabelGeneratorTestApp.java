package org.mskcc.smile;

import org.mskcc.cmo.messaging.Gateway;
import org.mskcc.smile.service.impl.LabelGenMessageHandlingServiceImpl;
import org.mskcc.smile.service.impl.RequestReplyHandlingServiceImpl;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.mock.mockito.MockBean;

/**
 *
 * @author laptop
 */
@SpringBootApplication(scanBasePackages = {"org.mskcc.smile.service", "org.mskcc.smile.commons.*"})
public class LabelGeneratorTestApp {
    @MockBean
    public Gateway messagingGateway;

    @MockBean
    public LabelGenMessageHandlingServiceImpl labelGenMessageHandlingServiceImpl;

    @MockBean
    public RequestReplyHandlingServiceImpl requestReplyMessageHandlingServiceImpl;
}

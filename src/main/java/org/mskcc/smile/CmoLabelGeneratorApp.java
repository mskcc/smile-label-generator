package org.mskcc.smile;

import java.util.concurrent.CountDownLatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mskcc.cmo.messaging.Gateway;
import org.mskcc.smile.service.MessageHandlingService;
import org.mskcc.smile.service.RequestReplyHandlingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = {"org.mskcc.cmo.messaging",
    "org.mskcc.smile.commons.*", "org.mskcc.smile.*"})
public class CmoLabelGeneratorApp implements CommandLineRunner {
    private static final Log LOG = LogFactory.getLog(CmoLabelGeneratorApp.class);

    @Autowired
    private Gateway messagingGateway;

    @Autowired
    private MessageHandlingService messageHandlingService;

    @Autowired
    private RequestReplyHandlingService requestReplyHandlingService;

    private Thread shutdownHook;
    final CountDownLatch cmoLabelGeneratorAppClose = new CountDownLatch(1);

    @Override
    public void run(String... args) throws Exception {
        LOG.info("Starting up CMO Label Generator application...");
        try {
            installShutdownHook();
            messagingGateway.connect();
            messageHandlingService.initialize(messagingGateway);
            requestReplyHandlingService.initialize(messagingGateway);
            cmoLabelGeneratorAppClose.await();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            Runtime.getRuntime().removeShutdownHook(shutdownHook);
        }
    }

    private void installShutdownHook() {
        shutdownHook =
            new Thread() {
                public void run() {
                    System.err.printf("\nCaught CTRL-C, shutting down gracefully...\n");
                    try {
                        messagingGateway.shutdown();
                        messageHandlingService.shutdown();
                        requestReplyHandlingService.shutdown();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    cmoLabelGeneratorAppClose.countDown();
                }
            };
        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }

    public static void main(String[] args) {
        SpringApplication.run(CmoLabelGeneratorApp.class, args);
    }

}

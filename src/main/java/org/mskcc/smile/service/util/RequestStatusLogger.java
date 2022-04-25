package org.mskcc.smile.service.util;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import org.apache.commons.lang3.StringUtils;
import org.mskcc.smile.commons.FileUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Class for logging request statuses to the provided
 * Request Handling filepath.
 * @author ochoaa
 */
@Component
public class RequestStatusLogger {
    @Value("${cmo_label_generator.request_logger_filepath}")
    private String requestLoggerFilepath;

    @Autowired
    private FileUtil fileUtil;

    private File requestStatusLoggerFile;

    @Autowired
    private static final String[] REQUEST_LOGGER_FILE_HEADER = new String[]{"DATE", "STATUS", "MESSAGE"};

    /**
     * Request StatusType descriptions:
     * - REQ_SAMPLE_FAILED_LABEL_GENERATION: request contains samples for which a
     *        CMO label was not successfully generated
     */
    public enum StatusType {
        REQ_SAMPLE_FAILED_LABEL_GENERATION
    }

    /**
     * Writes request contents and status to the request status logger file.
     * @param message
     * @param status
     * @throws IOException
     */
    public void logRequestStatus(String message, StatusType status) throws IOException {
        if (requestStatusLoggerFile ==  null) {
            this.requestStatusLoggerFile = fileUtil.getOrCreateFileWithHeader(requestLoggerFilepath,
                    StringUtils.join(REQUEST_LOGGER_FILE_HEADER, "\t") + "\n");
        }
        String currentDate = LocalDate.now().format(DateTimeFormatter.ISO_LOCAL_DATE);
        StringBuilder builder = new StringBuilder();
        builder.append(currentDate)
                .append("\t")
                .append(status.toString())
                .append("\t")
                .append(message)
                .append("\n");
        fileUtil.writeToFile(requestStatusLoggerFile, builder.toString());
    }
}

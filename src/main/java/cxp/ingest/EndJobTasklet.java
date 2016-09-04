package cxp.ingest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

import java.io.File;

/**
 * Created by markmo on 14/04/15.
 */
public class EndJobTasklet implements Tasklet {

    private static final Log log = LogFactory.getLog(EndJobTasklet.class);

    private String inFolder;

    private String testFolder;

    private String processingFolder;

    private String testProcessingFolder;

    private boolean append = true;

    private boolean writePropertiesFile = false;

    private MetadataProvider metadataProvider;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        metadataProvider.endJob();
        boolean test = metadataProvider.isTest();
        String filename = metadataProvider.getFilename();
        String filepart = filename.substring(0, filename.lastIndexOf("."));
        String landingFolder = test ? testFolder : inFolder;
        String outputFolder  = test ? testProcessingFolder : processingFolder;

        // could be an issue if running on Windows
        // On Windows, renameTo() seems to fail if the target directory exists, even if it's empty.
        final File processedFile = new File(landingFolder + filename);
        final File doneFile = new File(landingFolder + filepart + ".cxp");
        boolean renameSuccess = processedFile.renameTo(doneFile);
        if (!renameSuccess) {
            log.error("Could not rename file " + processedFile.getAbsolutePath());
        }

        final File eventsFile;
        if (append) {
            eventsFile = new File(outputFolder + "events.filepart");
        } else {
            eventsFile = new File(outputFolder + filepart + "_events.filepart");
        }
        if (eventsFile.exists()) {
            final File targetEventsFile;
            if (append) {
                targetEventsFile = new File(outputFolder + "events.csv");
            } else {
                targetEventsFile = new File(outputFolder + filepart + "_events.csv");
            }
            if (!eventsFile.renameTo(targetEventsFile)) {
                log.error("Could not rename file " + eventsFile.getAbsolutePath());
            }
        }

        if (writePropertiesFile) {
            final File propertiesFile;
            if (append) {
                propertiesFile = new File(outputFolder + "properties.filepart");
            } else {
                propertiesFile = new File(outputFolder + filepart + "_properties.filepart");
            }
            if (propertiesFile.exists()) {
                final File targetPropertiesFile;
                if (append) {
                    targetPropertiesFile = new File(outputFolder + "properties.csv");
                } else {
                    targetPropertiesFile = new File(outputFolder + filepart + "_properties.csv");
                }
                if (!propertiesFile.renameTo(targetPropertiesFile)) {
                    log.error("Could not rename file " + propertiesFile.getAbsolutePath());
                }
            }
        }

        return RepeatStatus.FINISHED;
    }

    public void setInFolder(String inFolder) {
        this.inFolder = inFolder;
    }

    public void setTestFolder(String testFolder) {
        this.testFolder = testFolder;
    }

    public void setProcessingFolder(String processingFolder) {
        this.processingFolder = processingFolder;
    }

    public void setTestProcessingFolder(String testProcessingFolder) {
        this.testProcessingFolder = testProcessingFolder;
    }

    public void setAppend(boolean append) {
        this.append = append;
    }

    public void setWritePropertiesFile(boolean writePropertiesFile) {
        this.writePropertiesFile = writePropertiesFile;
    }

    public void setMetadataProvider(MetadataProvider metadataProvider) {
        this.metadataProvider = metadataProvider;
    }
}

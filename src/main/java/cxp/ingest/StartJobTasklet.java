package cxp.ingest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

/**
 * Created by markmo on 14/04/15.
 */
public class StartJobTasklet implements Tasklet {

    private static final Log log = LogFactory.getLog(StartJobTasklet.class);

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        if (log.isDebugEnabled()) {
            log.debug("StartJobTasklet called");
        }
        return RepeatStatus.FINISHED;
    }
}

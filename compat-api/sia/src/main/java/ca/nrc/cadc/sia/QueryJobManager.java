
package ca.nrc.cadc.sia;

import ca.nrc.cadc.auth.ACIdentityManager;
import ca.nrc.cadc.uws.server.JobExecutor;
import ca.nrc.cadc.uws.server.JobPersistence;
import ca.nrc.cadc.uws.server.SimpleJobManager;
import ca.nrc.cadc.uws.server.SyncJobExecutor;
import ca.nrc.cadc.uws.server.impl.PostgresJobPersistence;
import org.apache.log4j.Logger;

/**
 *
 * @author pdowler
 */
public class QueryJobManager extends SimpleJobManager
{
    private static final Logger log = Logger.getLogger(QueryJobManager.class);
    
   

    private static final Long MAX_EXEC_DURATION = new Long(600L);
    private static final Long MAX_DESTRUCTION = new Long(7*24*3600L); // 1 week
    private static final Long MAX_QUOTE = new Long(600L); // same as exec since we don't queue

    public QueryJobManager()
    {
        super();
        JobPersistence jobPersist = new PostgresJobPersistence(new ACIdentityManager());
        
        // exec jobs in in new thread using custom SiaRunner
        JobExecutor jobExec = new SyncJobExecutor(jobPersist, SiaRunner.class);

        super.setJobPersistence(jobPersist);
        super.setJobExecutor(jobExec);
        super.setMaxExecDuration(MAX_EXEC_DURATION);
        super.setMaxDestruction(MAX_DESTRUCTION);
        super.setMaxQuote(MAX_QUOTE);
    }

}

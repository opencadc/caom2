
package ca.nrc.cadc.sia2.impl;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.sia2.SiaRunner;
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
public class SiaJobManager extends SimpleJobManager
{
    private static final Logger log = Logger.getLogger(SiaJobManager.class);
    
    private static final Long MAX_EXEC_DURATION = 600L;
    private static final Long MAX_DESTRUCTION = 7 * 24 * 3600L; // 1 week
    private static final Long MAX_QUOTE = 600L; // same as exec since we don't queue

    public SiaJobManager()
    {
        super();
        JobPersistence jobPersist = new PostgresJobPersistence(AuthenticationUtil.getIdentityManager());
        
        // exec jobs in request thread using custom SiaRunner
        JobExecutor jobExec = new SyncJobExecutor(jobPersist, SiaRunner.class);

        super.setJobPersistence(jobPersist);
        super.setJobExecutor(jobExec);
        super.setMaxExecDuration(MAX_EXEC_DURATION);
        super.setMaxDestruction(MAX_DESTRUCTION);
        super.setMaxQuote(MAX_QUOTE);
    }

}

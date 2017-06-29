package ca.nrc.cadc.caom2.harvester;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;

import ca.nrc.cadc.caom2.DeletedEntity;
import ca.nrc.cadc.caom2.persistence.AbstractDAO;

public class ServiceDeletedEntityDAO<T extends DeletedEntity>
        extends
            AbstractDAO
{

    private static final Logger log = Logger
            .getLogger(ServiceDeletedEntityDAO.class);

    public ServiceDeletedEntityDAO()
    {
    }

    public List<T> getList(Class<? extends DeletedEntity> c,
            Date minLastModified, Date maxLastModified, Integer batchSize)
    {
        List<T> ret = new ArrayList<T>();
        log.debug("GET: " + batchSize);
        long t = System.currentTimeMillis();
        try
        {

        }
        finally
        {
            long dt = System.currentTimeMillis() - t;
            log.debug("GET: " + batchSize + " " + dt + "ms");
        }
        return ret;
    }

}

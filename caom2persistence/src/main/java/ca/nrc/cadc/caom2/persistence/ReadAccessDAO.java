/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2011.                            (c) 2011.
*  Government of Canada                 Gouvernement du Canada
*  National Research Council            Conseil national de recherches
*  Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
*  All rights reserved                  Tous droits réservés
*
*  NRC disclaims any warranties,        Le CNRC dénie toute garantie
*  expressed, implied, or               énoncée, implicite ou légale,
*  statutory, of any kind with          de quelque nature que ce
*  respect to the software,             soit, concernant le logiciel,
*  including without limitation         y compris sans restriction
*  any warranty of merchantability      toute garantie de valeur
*  or fitness for a particular          marchande ou de pertinence
*  purpose. NRC shall not be            pour un usage particulier.
*  liable in any event for any          Le CNRC ne pourra en aucun cas
*  damages, whether direct or           être tenu responsable de tout
*  indirect, special or general,        dommage, direct ou indirect,
*  consequential or incidental,         particulier ou général,
*  arising from the use of the          accessoire ou fortuit, résultant
*  software.  Neither the name          de l'utilisation du logiciel. Ni
*  of the National Research             le nom du Conseil National de
*  Council of Canada nor the            Recherches du Canada ni les noms
*  names of its contributors may        de ses  participants ne peuvent
*  be used to endorse or promote        être utilisés pour approuver ou
*  products derived from this           promouvoir les produits dérivés
*  software without specific prior      de ce logiciel sans autorisation
*  written permission.                  préalable et particulière
*                                       par écrit.
*
*  This file is part of the             Ce fichier fait partie du projet
*  OpenCADC project.                    OpenCADC.
*
*  OpenCADC is free software:           OpenCADC est un logiciel libre ;
*  you can redistribute it and/or       vous pouvez le redistribuer ou le
*  modify it under the terms of         modifier suivant les termes de
*  the GNU Affero General Public        la “GNU Affero General Public
*  License as published by the          License” telle que publiée
*  Free Software Foundation,            par la Free Software Foundation
*  either version 3 of the              : soit la version 3 de cette
*  License, or (at your option)         licence, soit (à votre gré)
*  any later version.                   toute version ultérieure.
*
*  OpenCADC is distributed in the       OpenCADC est distribué
*  hope that it will be useful,         dans l’espoir qu’il vous
*  but WITHOUT ANY WARRANTY;            sera utile, mais SANS AUCUNE
*  without even the implied             GARANTIE : sans même la garantie
*  warranty of MERCHANTABILITY          implicite de COMMERCIALISABILITÉ
*  or FITNESS FOR A PARTICULAR          ni d’ADÉQUATION À UN OBJECTIF
*  PURPOSE.  See the GNU Affero         PARTICULIER. Consultez la Licence
*  General Public License for           Générale Publique GNU Affero
*  more details.                        pour plus de détails.
*
*  You should have received             Vous devriez avoir reçu une
*  a copy of the GNU Affero             copie de la Licence Générale
*  General Public License along         Publique GNU Affero avec
*  with OpenCADC.  If not, see          OpenCADC ; si ce n’est
*  <http://www.gnu.org/licenses/>.      pas le cas, consultez :
*                                       <http://www.gnu.org/licenses/>.
*
*  $Revision: 5 $
*
************************************************************************
*/

package ca.nrc.cadc.caom2.persistence;

import ca.nrc.cadc.caom2.access.ReadAccess;
import ca.nrc.cadc.caom2.persistence.skel.Skeleton;
import java.net.URI;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 *
 * @author pdowler
 */
public class ReadAccessDAO extends AbstractCaomEntityDAO<ReadAccess>
{
    private static final Logger log = Logger.getLogger(ReadAccessDAO.class);

    public ReadAccessDAO() { }
    
    // need to expose this for caom2ac which has to cleanup tuples for caom2 
    // assets that became public
    public String getTable(Class c)
    {
        return gen.getTable(c);
    }
    
    public ReadAccess get(Class<? extends ReadAccess> c, UUID assetID, URI groupID)
    {
        checkInit();
        if (c == null || assetID == null || groupID == null)
            throw new IllegalArgumentException("args cannot be null");
        log.debug("GET: " + c.getSimpleName() + " " + assetID + "," + groupID);
        long t = System.currentTimeMillis();
        
        try
        {
            String sql = gen.getSelectSQL(c, assetID, groupID);
            if (log.isDebugEnabled())
                log.debug("GET SQL: " + Util.formatSQL(sql));

            JdbcTemplate jdbc = new JdbcTemplate(dataSource);
            Object result = jdbc.query(sql, gen.getReadAccessMapper(c));
            if (result == null)
                return null;
            if (result instanceof List)
            {
                List obs = (List) result;
                if (obs.isEmpty())
                    return null;
                if (obs.size() > 1)
                    throw new RuntimeException("BUG: get " + c.getSimpleName() + " " + assetID + "," + groupID + " query returned " + obs.size() + " ReadAccess tuples");
                Object o = obs.get(0);
                if (o instanceof ReadAccess)
                {
                    ReadAccess ret = (ReadAccess) obs.get(0);
                    return ret;
                }
                else
                    throw new RuntimeException("BUG: query returned an unexpected type " + o.getClass().getName());
            }
            throw new RuntimeException("BUG: query returned an unexpected type " + result.getClass().getName());
        }
        finally
        {
            long dt = System.currentTimeMillis() - t;
            log.debug("GET: " + c.getSimpleName() + " " + assetID + "," + groupID + " " + dt + "ms");
        }
    }

    public ReadAccess get(Class<? extends ReadAccess> c, UUID id)
    {
        checkInit();
        if (c == null || id == null)
            throw new IllegalArgumentException("args cannot be null");
        log.debug("GET: " + c.getSimpleName() + " " + id);
        long t = System.currentTimeMillis();

        try
        {
            String sql = gen.getSelectSQL(c, id);
            if (log.isDebugEnabled())
                log.debug("GET SQL: " + Util.formatSQL(sql));

            JdbcTemplate jdbc = new JdbcTemplate(dataSource);
            Object result = jdbc.query(sql, gen.getReadAccessMapper(c));
            if (result == null)
                return null;
            if (result instanceof List)
            {
                List obs = (List) result;
                if (obs.isEmpty())
                    return null;
                if (obs.size() > 1)
                    throw new RuntimeException("BUG: get " + c.getSimpleName() + " " + id + " query returned " + obs.size() + " ReadAccess tuples");
                Object o = obs.get(0);
                if (o instanceof ReadAccess)
                {
                    ReadAccess ret = (ReadAccess) obs.get(0);
                    return ret;
                }
                else
                    throw new RuntimeException("BUG: query returned an unexpected type " + o.getClass().getName());
            }
            throw new RuntimeException("BUG: query returned an unexpected type " + result.getClass().getName());
        }
        finally
        {
            long dt = System.currentTimeMillis() - t;
            log.debug("GET: " + c.getSimpleName() + " " + id + " " + dt + "ms");
        }
    }

    public void put(ReadAccess ra)
        throws DuplicateEntityException
    {
        checkInit();
        if (ra == null)
            throw new IllegalArgumentException("arg cannot be null");
        log.debug("PUT: " + ra);
        long t = System.currentTimeMillis();

        try
        {
            JdbcTemplate jdbc = new JdbcTemplate(dataSource);
            Class skel = gen.getSkeletonClass(ra.getClass());
            String sql = gen.getSelectSQL(skel, ra.getID());
            log.debug("PUT: " + sql);
            Skeleton cur = (Skeleton) jdbc.query(sql, gen.getSkeletonExtractor(skel));

            updateEntity(ra, cur);

            super.put(cur, ra, null, jdbc);
        }
        catch(DataIntegrityViolationException ex)
        {
            if (ex.toString().contains("duplicate key"))
                throw new DuplicateEntityException(ra.toString(), ex);
            throw ex;
        }
        catch(TransientDataAccessResourceException ex) // found this with jTDS driver
        {
            if (ex.toString().contains("duplicate key"))
                throw new DuplicateEntityException(ra.toString(), ex);
            throw ex;
        }
        finally
        {
            long dt = System.currentTimeMillis() - t;
            log.debug("PUT: " + ra + " " + dt + "ms");
        }
    }

    public void delete(Class<? extends ReadAccess> c, UUID id)
    {
        checkInit();
        if (c == null || id == null)
            throw new IllegalArgumentException("args cannot be null");
        log.debug("DELETE: " + c.getSimpleName() + " " + id);
        long t = System.currentTimeMillis();

        try
        {
            JdbcTemplate jdbc = new JdbcTemplate(dataSource);
            // get current tuple
            ReadAccess cur = get(c, id);
            if (cur != null)
            {
                EntityDelete op = gen.getEntityDelete(c, true);
                op.setID(id);
                op.setValue(cur);
                op.execute(jdbc);
            }
        }
        finally
        {
            long dt = System.currentTimeMillis() - t;
            log.debug("DELETE: " +  c.getSimpleName() + " " + id + " " + dt + "ms");
        }
    }

    private void updateEntity(ReadAccess ra, Skeleton s)
    {
        int nsc = ra.getStateCode();
        
        digest.reset();
        Util.assignMetaChecksum(ra, ra.computeMetaChecksum(digest), "metaChecksum");
        
        if (!computeLastModified)
            return;
        
        boolean delta = false;
        if (s == null)
            delta = true;
        else if (s.metaChecksum != null)
            delta = !ra.getMetaChecksum().equals(s.metaChecksum);
        else
            delta = (s.stateCode != nsc); // fallback
                
        if (delta)        
            Util.assignLastModified(ra, new Date(), "lastModified");
    }
}

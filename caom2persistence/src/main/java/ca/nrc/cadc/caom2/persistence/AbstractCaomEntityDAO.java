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

import ca.nrc.cadc.caom2.CaomEntity;
import ca.nrc.cadc.caom2.persistence.skel.Skeleton;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 *
 * @author pdowler
 */
abstract class AbstractCaomEntityDAO<T extends CaomEntity> extends AbstractDAO {

    private static final Logger log = Logger.getLogger(AbstractCaomEntityDAO.class);
    protected boolean origin = true;

    protected MessageDigest digest;

    protected AbstractCaomEntityDAO() {
        try {
            this.digest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException ex) {
            throw new RuntimeException("FATAL: no MD5 digest algorithm available", ex);
        }
    }

    // constructor for utility classes that share the same settings instead of being
    // configured
    protected AbstractCaomEntityDAO(SQLGenerator gen, boolean forceUpdate, boolean readOnly) {
        this();
        this.gen = gen;
        this.forceUpdate = forceUpdate;
        this.readOnly = readOnly;
    }

    /**
     * Set origin flag. When true, the persistence classes behave as origin metadata
     * repositories: they are responsible for assigning entity IDs and modification
     * timestamps when entities are put. When false (not an origin) the provided
     * IDs and timestamps are used directly.
     * 
     * @param origin 
     */
    public void setOrigin(boolean origin) {
        this.origin = origin;
    }

    public T get(UUID id) {
        throw new UnsupportedOperationException();
    }

    protected void put(Skeleton cur, T val, LinkedList<CaomEntity> parents, JdbcTemplate jdbc) {
        put(cur, val, parents, jdbc, false);
    }

    protected void put(Skeleton cur, T val, LinkedList<CaomEntity> parents, JdbcTemplate jdbc, boolean force) {
        if (readOnly) {
            throw new UnsupportedOperationException("put in readOnly mode");
        }
        checkInit();

        boolean delta = false;
        String cmp = " [new]";
        if (cur != null) {
            delta = !val.getMetaChecksum().equals(cur.metaChecksum);
            cmp = " " + cur.metaChecksum + " vs " + val.getMetaChecksum();

            // change in accMetaChecksum means maxLastModified changed
            // this correctly maintains accMetaChecksum and maxLastModified
            if (!delta) {
                delta = !val.getAccMetaChecksum().equals(cur.accMetaChecksum);
                cmp = cmp + " -- " + cur.accMetaChecksum + " vs " + val.getAccMetaChecksum();
            }
            
            // temporary(?) work-around for a caom2harvester bug that set origin=true and assigned new timestamps
            // if not origin but lastModified values are inconsistent: force an update
            if (!origin && !delta) {
                delta = !val.getLastModified().equals(cur.lastModified) || !val.getMaxLastModified().equals(cur.maxLastModified);
            }
            // end of work-around
            log.debug("PUT: " + val.getClass().getSimpleName() + cmp);
        } else {
            log.debug("PUT: " + val.getClass().getSimpleName() + cmp);
        }

        boolean isUpdate = (cur != null);

        // insert || forceUpdate mode || caller force || state changed
        if (cur == null || forceUpdate || force || delta) {
            if (isUpdate) {
                log.debug("PUT update: " + val.getClass().getSimpleName() + " " + val.getID());
            } else {
                log.debug("PUT insert: " + val.getClass().getSimpleName() + " " + val.getID());
            }
            EntityPut<T> op = gen.getEntityPut(val.getClass(), isUpdate);
            op.setValue(val, parents);
            op.execute(jdbc);
        } else {
            log.debug("PUT skip: " + val.getClass().getSimpleName() + " " + val.getID());
        }
    }

    protected void delete(Skeleton ce, JdbcTemplate jdbc) {
        if (readOnly) {
            throw new UnsupportedOperationException("delete in readOnly mode");
        }
        deleteChildren(ce, jdbc);
        deleteSelf(ce, jdbc);
    }

    protected void deleteSelf(Skeleton ce, JdbcTemplate jdbc) {
        if (readOnly) {
            throw new UnsupportedOperationException("delete in readOnly mode");
        }
        checkInit();
        // delete by PK
        EntityDelete op = gen.getEntityDelete(ce.targetClass, true);
        op.setID(ce.id);
        op.execute(jdbc);
    }

    protected void deleteChildren(Skeleton ce, JdbcTemplate jdbc) {
        if (readOnly) {
            throw new UnsupportedOperationException("delete in readOnly mode");
        }
        log.debug("deleteChildren no-op: " + ce.targetClass.getSimpleName());
    }

    protected class Pair<T> {

        public Skeleton cur;
        public T val;

        Pair(Skeleton s, T v) {
            this.cur = s;
            this.val = v;
        }
    }
}

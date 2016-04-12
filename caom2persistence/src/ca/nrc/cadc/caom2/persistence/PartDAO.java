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
import ca.nrc.cadc.caom2.Chunk;
import ca.nrc.cadc.caom2.Part;
import ca.nrc.cadc.caom2.persistence.skel.ChunkSkeleton;
import ca.nrc.cadc.caom2.persistence.skel.PartSkeleton;
import ca.nrc.cadc.caom2.persistence.skel.Skeleton;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import org.apache.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 *
 * @author pdowler
 */
class PartDAO extends AbstractCaomEntityDAO<Part>
{
    private static final Logger log = Logger.getLogger(PartDAO.class);

    private ChunkDAO chunkDAO;

    // package access for use by ObservationDAO only
    PartDAO(SQLGenerator gen, boolean forceUpdate, boolean readOnly)
    {
        super(gen, forceUpdate, readOnly);
        this.chunkDAO = new ChunkDAO(gen, forceUpdate, readOnly);
    }

    @Override
    public void put(Skeleton cur, Part p, LinkedList<CaomEntity> parents, JdbcTemplate jdbc)
    {
        if (p == null)
            throw new IllegalArgumentException("arg cannot be null");
        log.debug("PUT: " + p.getID());
        long t = System.currentTimeMillis();

        try
        {
            // delete obsolete children
            Pair<Chunk> pair = null;
            if (cur != null) // update, maybe delete
            {
                PartSkeleton cs = (PartSkeleton) cur;
                // delete the skeleton that are not in p.getChunks()
                if (cs.chunk != null) // current chunk exists
                {
                    // removed || replaced
                    if ( p.chunk == null || !p.chunk.getID().equals(cs.chunk.id) )
                    {
                        log.debug("put caused delete: " + cs.chunk);
                        chunkDAO.delete(cs.chunk, jdbc);
                    }
                }
                // pair up chunks and skeletons for update
                if (p.chunk != null)
                {
                    ChunkSkeleton s = null;
                    if (p.chunk.getID().equals(cs.chunk.id))
                        s = cs.chunk; // update
                    pair = new Pair<Chunk>(s, p.chunk); // null ok
                }
            }
            else // insert new
                if (p.chunk != null)
                    pair = new Pair<Chunk>(null, p.chunk);

            super.put(cur, p, parents, jdbc);

            parents.push(p);
            if (pair != null)
                chunkDAO.put(pair.cur, pair.val, parents, jdbc);
            parents.pop();
        }
        finally
        {
            long dt = System.currentTimeMillis() - t;
            log.debug("PUT: " + p.getID() + " " + dt + "ms");
        }
    }

    @Override
    protected void deleteChildren(Skeleton s, JdbcTemplate jdbc)
    {
        PartSkeleton p = (PartSkeleton) s;
        if (p.chunk != null)
        {
            // chunks have no children
            
            // delete chunk by FK
            EntityDelete op = gen.getEntityDelete(Chunk.class, false);
            op.setID(p.id);
            op.execute(jdbc);
            //String sql = gen.getDeleteSQL(Chunk.class, p.id, false);
            //log.debug("delete: " + sql);
            //jdbc.update(sql);
        }
    }
}

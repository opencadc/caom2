/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2025.                            (c) 2025.
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

package org.opencadc.caom2.db;

import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;
import org.opencadc.caom2.Chunk;
import org.opencadc.caom2.Part;
import org.opencadc.caom2.db.skel.ChunkSkeleton;
import org.opencadc.caom2.db.skel.PartSkeleton;
import org.opencadc.caom2.db.skel.Skeleton;
import org.opencadc.persist.Entity;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 *
 * @author pdowler
 */
class PartDAO extends AbstractCaomEntityDAO<Part> {
    private static final Logger log = Logger.getLogger(PartDAO.class);

    private final ChunkDAO chunkDAO;

    PartDAO(AbstractCaomEntityDAO dao) {
        super(dao);
        this.chunkDAO = new ChunkDAO(dao);
    }

    @Override
    public void put(Skeleton cur, Part p, Entity parent, JdbcTemplate jdbc) {
        if (p == null) {
            throw new IllegalArgumentException("arg cannot be null");
        }
        log.debug("PUT: " + p.getID());
        long t = System.currentTimeMillis();

        try {
            // delete obsolete children
            List<Pair<Chunk>> pairs = new ArrayList<Pair<Chunk>>();
            if (cur != null) {
                PartSkeleton cs = (PartSkeleton) cur;
                // delete the skeletons that are not in p.getChunks()
                for (ChunkSkeleton s : cs.chunks) {
                    Chunk c = Util.findChunk(p.getChunks(), s.id);
                    if (c == null) {
                        log.debug("put caused delete chunk: " + c);
                        chunkDAO.delete(s, jdbc);
                    }
                }
                // pair up chunks and skeletons for insert/update
                for (Chunk c : p.getChunks()) {
                    ChunkSkeleton s = Util.findChunkSkel(cs.chunks, c.getID());
                    pairs.add(new Pair<Chunk>(s, c)); // null ok
                }
            } else {
                for (Chunk c : p.getChunks()) {
                    pairs.add(new Pair<Chunk>(null, c));
                }
            }

            super.put(cur, p, parent, jdbc);

            for (Pair<Chunk> part : pairs) {
                chunkDAO.put(part.cur, part.val, p, jdbc);
            }
        } finally {
            long dt = System.currentTimeMillis() - t;
            log.debug("PUT: " + p.getID() + " " + dt + "ms");
        }
    }

    @Override
    protected void deleteChildren(Skeleton s, JdbcTemplate jdbc) {
        PartSkeleton p = (PartSkeleton) s;
        if (!p.chunks.isEmpty()) {
            // delete chunks by FK
            EntityDelete op = gen.getEntityDelete(Chunk.class, false);
            op.setID(p.id);
            op.execute(jdbc);
        }
    }
}

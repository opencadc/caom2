/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2024.                            (c) 2024.
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

import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.Chunk;
import ca.nrc.cadc.caom2.Part;
import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.db.mappers.JdbcMapUtil;
import java.net.URI;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.opencadc.caom2.db.skel.ArtifactSkeleton;
import org.opencadc.caom2.db.skel.ChunkSkeleton;
import org.opencadc.caom2.db.skel.PartSkeleton;
import org.opencadc.caom2.db.skel.PlaneSkeleton;

/**
 *
 * @author pdowler
 */
public class Util extends JdbcMapUtil {
    private static Logger log = Logger.getLogger(Util.class);

    public static String safeGetClassName(Object o) {
        if (o == null) {
            return null;
        }
        return o.getClass().getName();
    }

    public static String formatSQL(String[] sql) {
        StringBuilder sb = new StringBuilder();
        for (String s : sql) {
            sb.append("\n");
            sb.append(formatSQL(s));
        }
        return sb.toString();
    }

    public static String formatSQL(String sql) {
        sql = sql.replaceAll("SELECT ", "\nSELECT ");
        sql = sql.replaceAll("FROM ", "\nFROM ");
        sql = sql.replaceAll("LEFT ", "\n  LEFT ");
        sql = sql.replaceAll("RIGHT ", "\n  RIGHT ");
        sql = sql.replaceAll("WHERE ", "\nWHERE ");
        sql = sql.replaceAll("AND ", "\n  AND ");
        sql = sql.replaceAll("OR ", "\n  OR ");
        sql = sql.replaceAll("ORDER", "\nORDER");
        sql = sql.replaceAll("GROUP ", "\nGROUP ");
        sql = sql.replaceAll("HAVING ", "\nHAVING ");
        sql = sql.replaceAll("UNION ", "\nUNION ");

        // note: \\s* matches one or more whitespace chars
        //sql = sql.replaceAll("OUTER JOIN", "\n  OUTER JOIN");
        return sql;
    }
    
    public static String extractGroupNames(Collection<URI> uris) {
        StringBuilder sb = new StringBuilder();
        for (URI u : uris) {
            sb.append(u.getQuery()).append(" ");
        }
        return sb.toString();
    }

    public static void sqlLog(String[] sql, boolean format) {
        if (sql == null) {
            return;
        }
        if (format) {
            for (int i = 0; i < sql.length; i++) {
                log.debug(Util.formatSQL(sql[i]));
            }
        } else {
            for (int i = 0; i < sql.length; i++) {
                log.debug(sql[i]);
            }
        }
    }

    public static Plane findPlane(Set<Plane> set, UUID id) {
        for (Plane e : set) {
            if (e.getID().equals(id)) {
                return e;
            }
        }
        return null;
    }

    public static PlaneSkeleton findPlaneSkel(List<PlaneSkeleton> set, UUID id) {
        for (PlaneSkeleton e : set) {
            if (e.id.equals(id)) {
                return e;
            }
        }
        return null;
    }

    public static Artifact findArtifact(Set<Artifact> set, UUID id) {
        for (Artifact e : set) {
            if (e.getID().equals(id)) {
                return e;
            }
        }
        return null;
    }

    public static ArtifactSkeleton findArtifactSkel(List<ArtifactSkeleton> set, UUID id) {
        for (ArtifactSkeleton e : set) {
            if (e.id.equals(id)) {
                return e;
            }
        }
        return null;
    }

    public static Part findPart(Set<Part> set, UUID id) {
        for (Part e : set) {
            if (e.getID().equals(id)) {
                return e;
            }
        }
        return null;
    }

    public static PartSkeleton findPartSkel(List<PartSkeleton> set, UUID id) {
        for (PartSkeleton e : set) {
            if (e.id.equals(id)) {
                return e;
            }
        }
        return null;
    }

    public static Chunk findChunk(Set<Chunk> set, UUID id) {
        for (Chunk e : set) {
            if (e.getID().equals(id)) {
                return e;
            }
        }
        return null;
    }

    public static ChunkSkeleton findChunkSkel(List<ChunkSkeleton> set, UUID id) {
        for (ChunkSkeleton e : set) {
            if (e.id.equals(id)) {
                return e;
            }
        }
        return null;
    }
}

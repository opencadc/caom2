/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2018.                            (c) 2018.
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

import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.caom2.ReleaseType;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.util.StringUtil;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import org.apache.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

/**
 * Utility class to support evaluation of read access.
 * 
 * @author pdowler
 */
public class ReadAccessDAO extends AbstractDAO {

    private static final Logger log = Logger.getLogger(ReadAccessDAO.class);

    private final Calendar utcCalendar = Calendar.getInstance(DateUtil.UTC);
    
    public ReadAccessDAO() {
    }

    //public List<ArtifactAccess> getArtifactAccess(ObservationURI obsURI) {
    //    List<ArtifactAccess> ret = new ArrayList<>();
    // TODO: query for all artifact++ in the observation and populate AA list
    //    return ret;
    //}
    
    //public List<ArtifactAccess> getArtifactAccess(PublisherID pubID) {
    //    List<ArtifactAccess> ret = new ArrayList<>();
    // TODO: query for all artifact++ in the plane and populate AA list
    //    return ret;
    //}
    
    public RawArtifactAccess getArtifactAccess(URI artifactURI) {
        checkInit();
        if (artifactURI == null) {
            throw new IllegalArgumentException("arg cannot be null");
        }
        log.debug("getArtifactAccess: " + artifactURI);
        long t = System.currentTimeMillis();

        try {
            String pa = gen.getAlias(Plane.class);
            String aa = gen.getAlias(Artifact.class);
            
            StringBuilder sb = new StringBuilder();
            sb.append("SELECT ");
            sb.append(gen.getColumns(Artifact.class, aa)).append(",");
            sb.append(pa).append(".metaRelease").append(",");
            sb.append(pa).append(".dataRelease").append(",");
            sb.append(pa).append(".metaReadGroups").append(",");
            sb.append(pa).append(".dataReadGroups");
            sb.append(" FROM ");
            sb.append(gen.getFrom(Plane.class, 2, false));
            sb.append(" WHERE ").append(aa).append(".uri = ?");
            String sql = sb.toString();
            if (log.isDebugEnabled()) {
                log.debug("GET SQL: " + Util.formatSQL(sql));
            }

            JdbcTemplate jdbc = new JdbcTemplate(dataSource);
            Object[] args = new Object[1];
            args[0] = artifactURI.toASCIIString();
            Object result = jdbc.query(sql, args, new ArtifactAccessMapper());
            if (result == null) {
                return null;
            }
            if (result instanceof List) {
                List obs = (List) result;
                if (obs.isEmpty()) {
                    return null;
                }
                //if (obs.size() > 1) {
                //    throw new RuntimeException("BUG: get ArtifactAccess " + artifactURI + " query returned " + obs.size() + " rows");
                //}
                Object o = obs.get(0);
                if (o instanceof RawArtifactAccess) {
                    RawArtifactAccess ret = (RawArtifactAccess) o;
                    return ret;
                } else {
                    throw new RuntimeException("BUG: query returned an unexpected type " + o.getClass().getName());
                }
            }
            throw new RuntimeException("BUG: query returned an unexpected list type " + result.getClass().getName());
        } finally {
            long dt = System.currentTimeMillis() - t;
            log.debug("getArtifactAccess: " + artifactURI + " " + dt + "ms");
        }
    }
    
    public static class RawArtifactAccess {
        public Artifact artifact;
        public ReleaseType releaseType;
        public Date metaRelease;
        public Date dataRelease;
        public final List<URI> metaReadAccessGroups = new ArrayList<>();
        public final List<URI> dataReadAccessGroups = new ArrayList<>();
    }
    
    private class ArtifactAccessMapper implements RowMapper {
        @Override
        public Object mapRow(ResultSet rs, int i) throws SQLException {
            
            PartialRowMapper<Artifact> am = gen.getArtifactMapper();

            RawArtifactAccess ret = new RawArtifactAccess();
            ret.artifact = am.mapRow(rs, i, 1);
            int col = am.getColumnCount();
            col++;
            
            ret.metaRelease = Util.getDate(rs, col++, utcCalendar);
            ret.dataRelease = Util.getDate(rs, col++, utcCalendar);
            
            String mgs = rs.getString(col++);
            String dgs = rs.getString(col++);
            
            if (StringUtil.hasText(mgs)) {
                String[] ss = mgs.split(" ");
                for (String suri : ss) {
                    try {
                        ret.metaReadAccessGroups.add(new URI(suri));
                    } catch (URISyntaxException ex) {
                        throw new RuntimeException("invalid content: " + suri + " not a valid URI", ex);
                    }
                }
            }
            log.debug("raw: " + mgs + " -> " + ret.metaReadAccessGroups.size());
            
            if (StringUtil.hasText(dgs)) {
                String[] ss = dgs.split(" ");
                for (String suri : ss) {
                    try {
                        ret.dataReadAccessGroups.add(new URI(suri));
                    } catch (URISyntaxException ex) {
                        throw new RuntimeException("invalid content: " + suri + " not a valid URI", ex);
                    }
                }
            }
            log.debug("raw: " + dgs + " -> " + ret.dataReadAccessGroups.size());
            
            return ret;
        }
        
    }
    
    // need to expose this for caom2ac which has to cleanup tuples for caom2 
    // assets that became public
    public String getTable(Class c) {
        return gen.getTable(c);
    }
}

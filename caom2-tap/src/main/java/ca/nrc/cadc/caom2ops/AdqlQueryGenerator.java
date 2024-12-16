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

package ca.nrc.cadc.caom2ops;

import ca.nrc.cadc.caom2.ObservationURI;
import ca.nrc.cadc.caom2.PlaneURI;
import ca.nrc.cadc.caom2.PublisherID;
import java.net.URI;
import org.apache.log4j.Logger;

/**
 * Generates an ADQL query to select necessary metadata for all artifacts planes.
 * @author pdowler
 */
public class AdqlQueryGenerator {
    private static final Logger log = Logger.getLogger(AdqlQueryGenerator.class);

    // use the obsID FK column and an alias because FK columns don't have a utype 
    // and thus we won't accidentally effect result parsing
    private static final String SELECT_READABLE = "Plane.metaRelease, Plane.metaReadGroups, Plane.dataRelease, Plane.dataReadGroups";
    
    private static final String SELECT_ARTIFACT = "Plane.publisherID, Artifact.*";
    private static final String SELECT_ARTIFACT2CHUNK = SELECT_ARTIFACT + ", Part.*, Chunk.*";
    private static final String SELECT_OBS2CHUNK = "Observation.*, Plane.*, Artifact.*, Part.*, Chunk.*";
    
    private static final String ARTIFACT2CHUNK =
        "caom2.Artifact AS Artifact"
        + " LEFT OUTER JOIN caom2.Part AS Part ON Part.artifactID = Artifact.artifactID"
        + " LEFT OUTER JOIN caom2.Chunk AS Chunk ON Part.partID = Chunk.partID";
    
    private static final String PLANE2CHUNK =
        "caom2.Plane AS Plane"
        + " LEFT OUTER JOIN caom2.Artifact AS Artifact ON Plane.planeID = Artifact.planeID"
        + " LEFT OUTER JOIN caom2.Part AS Part ON Part.artifactID = Artifact.artifactID"
        + " LEFT OUTER JOIN caom2.Chunk AS Chunk ON Part.partID = Chunk.partID";
    
    private static final String OBS2CHUNK = 
        "caom2.Observation AS Observation"
        + " LEFT OUTER JOIN caom2.Plane AS Plane ON Observation.obsID = Plane.obsID"
        + " LEFT OUTER JOIN caom2.Artifact AS Artifact ON Plane.planeID = Artifact.planeID"
        + " LEFT OUTER JOIN caom2.Part AS Part ON Part.artifactID = Artifact.artifactID"
        + " LEFT OUTER JOIN caom2.Chunk AS Chunk ON Part.partID = Chunk.partID";
    
    private static final String PLANE2ARTIFACT =
        "caom2.Plane AS Plane"
        + " LEFT OUTER JOIN caom2.Artifact AS Artifact ON Plane.planeID = Artifact.planeID";
    
    // used by meta
    public String getADQL(final ObservationURI uri) {
        StringBuilder sb = new StringBuilder("SELECT ");
        sb.append(SELECT_OBS2CHUNK);
        sb.append(" FROM ");
        sb.append(OBS2CHUNK);
        sb.append(" WHERE Observation.collection = '").append(uri.getCollection()).append("'");
        sb.append(" AND Observation.observationID = '").append(uri.getObservationID()).append("'");
        sb.append(" ORDER BY Plane.planeID, Artifact.artifactID, Part.PartID");
        return sb.toString();
    }
    
    // used by datalink
    public String getADQL(final PublisherID uri, boolean artifactOnly) {
        StringBuilder sb = new StringBuilder("SELECT ");
        sb.append(SELECT_READABLE).append(",");
        if (artifactOnly) {
            sb.append(SELECT_ARTIFACT);
            sb.append(" FROM ");
            sb.append(PLANE2ARTIFACT);
        } else {
            sb.append(SELECT_ARTIFACT2CHUNK);
            sb.append(" FROM ");
            sb.append(PLANE2CHUNK);
        }
        
        sb.append(" WHERE Plane.publisherID = '");
        sb.append(uri.getURI().toASCIIString());
        sb.append("'");
        if (!artifactOnly) {
            sb.append(" ORDER BY Artifact.artifactID, Part.partID");
        }
        
        String ret = sb.toString();
        log.debug(ret);
        return ret;
    }
    
    // used by cutout
    public String getArtifactADQL(final URI uri) {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");
        sb.append(SELECT_ARTIFACT2CHUNK);
        sb.append(" FROM ");
        sb.append(PLANE2CHUNK); // need Plane.publisherID
        sb.append(" WHERE Artifact.uri = '");
        sb.append(uri.toString());
        sb.append("'");
        sb.append(" ORDER BY Artifact.artifactID, Part.partID");
        return sb.toString();
    }
}
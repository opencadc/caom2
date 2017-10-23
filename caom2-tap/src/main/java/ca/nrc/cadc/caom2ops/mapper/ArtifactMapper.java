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

package ca.nrc.cadc.caom2ops.mapper;

import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.ProductType;
import ca.nrc.cadc.caom2.ReleaseType;
import ca.nrc.cadc.caom2ops.Util;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.log4j.Logger;

/**
*
* @author pdowler
*/
public class ArtifactMapper implements VOTableRowMapper<Artifact>
{
    private static final Logger log = Logger.getLogger(ArtifactMapper.class);
    
    private Map<String,Integer> map;

    public ArtifactMapper(Map<String,Integer> map)
    {
            this.map = map;
    }

    /**
     * Map columns from the current row into an Artifact, starting at the 
     * specified column offset.
     * 
     * @param data
     * @param dateFormat
     * @return an artifact
     */
    public Artifact mapRow(List<Object> data, DateFormat dateFormat)
    {
        log.debug("mapping Artifact");
        UUID id = Util.getUUID(data, map.get("caom2:Artifact.id"));
        if (id == null)
            return null;

        String suri = Util.getString(data, map.get("caom2:Artifact.uri"));
        try
        {
            URI uri = new URI(suri);
            ProductType pt = null;
            String pts = Util.getString(data, map.get("caom2:Artifact.productType"));
            if (pts != null)
                pt = ProductType.toValue(pts);
            else
            {
                pt = ProductType.SCIENCE;
                log.warn("assigning default Artifact.productType = " + pt + " for " + uri);
            }
            ReleaseType rt = null;
            String rts = Util.getString(data, map.get("caom2:Artifact.releaseType"));
            if (rts != null)
                rt = ReleaseType.toValue(rts);
            else
            {
                rt = ReleaseType.DATA;
                log.warn("assigning default Artifact.releaseType = " + rt + " for "+uri);
            }
            
            Artifact artifact = new Artifact(uri, pt, rt);

            artifact.contentType = Util.getString(data, map.get("caom2:Artifact.contentType"));
            artifact.contentLength = Util.getLong(data, map.get("caom2:Artifact.contentLength"));
            artifact.contentChecksum = Util.getURI(data, map.get("caom2:Artifact.contentChecksum"));

            Date lastModified = Util.getDate(data, map.get("caom2:Artifact.lastModified"));
            Date maxLastModified = Util.getDate(data, map.get("caom2:Artifact.maxLastModified"));
            Util.assignLastModified(artifact, lastModified, "lastModified");
            Util.assignLastModified(artifact, maxLastModified, "maxLastModified");
            
            URI metaChecksum = Util.getURI(data, map.get("caom2:Artifact.metaChecksum"));
            URI accMetaChecksum = Util.getURI(data, map.get("caom2:Artifact.accMetaChecksum"));
            Util.assignMetaChecksum(artifact, metaChecksum, "metaChecksum");
            Util.assignMetaChecksum(artifact, accMetaChecksum, "accMetaChecksum");
            
            Util.assignID(artifact, id);

            return artifact;
        }
        catch(URISyntaxException ex)
        {
            throw new UnexpectedContentException("invalid URI", ex);
        }
    }
}

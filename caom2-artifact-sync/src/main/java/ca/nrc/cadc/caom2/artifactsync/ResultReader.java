/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2019.                            (c) 2019.
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


package ca.nrc.cadc.caom2.artifactsync;

import ca.nrc.cadc.caom2.ProductType;
import ca.nrc.cadc.caom2.ReleaseType;
import ca.nrc.cadc.caom2.artifact.ArtifactMetadata;
import ca.nrc.cadc.caom2.artifact.ArtifactStore;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.net.InputStreamWrapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.util.TreeSet;

import org.apache.log4j.Logger;

/**
 * Class that reads the results of a query.
 * 
 * @author majorb
 *
 */
public class ResultReader implements InputStreamWrapper {
    
    private static final Logger log = Logger.getLogger(ResultReader.class);
    
    TreeSet<ArtifactMetadata> metadata;
    private ArtifactStore artifactStore;
    
    public ResultReader(ArtifactStore artifactStore) 
            throws NoSuchAlgorithmException {
        metadata = new TreeSet<>(ArtifactMetadata.getComparator());
        this.artifactStore = artifactStore;
    }

    @Override
    public void read(InputStream inputStream) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        String line;
        String[] parts;
        ArtifactMetadata am = null;
        boolean firstLine = true;
        DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
        while ((line = reader.readLine()) != null) {
            if (firstLine) {
                // first line is a header
                firstLine = false;
            } else {
                try {
                    parts = line.split("\t");
                    if (parts.length == 0) {
                        // empty line
                    } else {
                        am = new ArtifactMetadata();
                        am.artifactURI = new URI(parts[0]);
                        am.storageID = this.artifactStore.toStorageID(am.artifactURI.toString());
                        
                        // read lastModified
                        String dateString = parts[1];
                        if (dateString == null || dateString.length() == 0) {
                            am.lastModified = null;
                        } else {
                            am.lastModified = df.parse(dateString);
                        }
                        
                        if (parts.length > 2) {
                            String checksum = parts[2];
                            int colon = checksum.indexOf(":");
                            am.checksum = checksum.substring(colon + 1, checksum.length());
                        }
                        if (parts.length > 3) {
                            am.contentLength = parts[3];
                        }
                        if (parts.length > 4) {
                            am.contentType = parts[4];
                        }
                        if (parts.length > 5) {
                            am.observationID = parts[5];
                        }
                        if (parts.length > 6) {
                            am.productType = ProductType.toValue(parts[6]);
                        }
                        if (parts.length > 7) {
                            am.releaseType = ReleaseType.toValue(parts[7]);
                        }
                        if (parts.length > 8) {
                            dateString = parts[8];
                            if (dateString == null || dateString.length() == 0) {
                                am.dataRelease = null;
                            } else {
                                am.dataRelease = df.parse(dateString);
                            }
                        }
                        if (parts.length > 9) {
                            dateString = parts[9];
                            if (dateString == null || dateString.length() == 0) {
                                am.metaRelease = null;
                            } else {
                                am.metaRelease = df.parse(dateString);
                            }
                        }
                        metadata.add(am);
                    }
                } catch (Exception e) {
                    log.warn("Failed to read logical artifact: " + line, e);
                }
            }
        }
        log.debug("Finished reading logical artifacts.");
    }
}

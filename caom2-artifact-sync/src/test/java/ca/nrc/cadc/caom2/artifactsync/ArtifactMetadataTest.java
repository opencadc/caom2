/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2017.                            (c) 2017.
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
************************************************************************
*/

package ca.nrc.cadc.caom2.artifactsync;


import ca.nrc.cadc.caom2.artifact.ArtifactMetadata;
import ca.nrc.cadc.caom2.artifact.ArtifactStore;

import java.net.URI;
import java.util.Date;
import java.util.TreeSet;

import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author pdowler
 */
public class ArtifactMetadataTest
{
    @Test
    public void testValidateCompareMetadata() throws Exception
    {
        boolean reportOnly = false;
        testCompareMetadata(reportOnly);
    }
    
    @Test 
    public void testDiffCompareMetadata() throws Exception
    {
        boolean reportOnly = true;
        testCompareMetadata(reportOnly);
    }
    
    private void testCompareMetadata(boolean reportOnly) throws Exception {
        URI caomTapResourceID = null;
        String collection = "HST";
        ArtifactStore artifactStore = null;

        // 1. test validation
        ArtifactValidator validator = new ArtifactValidator(caomTapResourceID, collection, reportOnly, artifactStore);
        // a. logicalArtifacts is empty, physicalArtifacts is empty
        TreeSet<ArtifactMetadata> logicalArtifacts = new TreeSet<ArtifactMetadata>(ArtifactMetadata.getComparator());
        TreeSet<ArtifactMetadata> physicalArtifacts = new TreeSet<ArtifactMetadata>(ArtifactMetadata.getComparator());
        long start = System.currentTimeMillis();
        validator.compareMetadata(logicalArtifacts, physicalArtifacts, start);
        
        // b. logicalArtifacts is empty, physicalArtifacts is not empty
        ArtifactMetadata metadata = new ArtifactMetadata();
        logicalArtifacts = new TreeSet<ArtifactMetadata>(ArtifactMetadata.getComparator());
        physicalArtifacts = new TreeSet<ArtifactMetadata>(ArtifactMetadata.getComparator());
        try {
            physicalArtifacts.add(metadata);
            Assert.fail("Failed to detect null metadata.storageID in physicalArtifacts.");
        } catch (NullPointerException ex) {
            // expected
        }
        
        // c. logicalArtifacts is not empty, physicalArtifacts is empty
        logicalArtifacts = new TreeSet<ArtifactMetadata>(ArtifactMetadata.getComparator());
        physicalArtifacts = new TreeSet<ArtifactMetadata>(ArtifactMetadata.getComparator());
        try {
            logicalArtifacts.add(metadata);
        Assert.fail("Failed to detect null metadata.storageID in logicalArtifacts.");
        } catch (NullPointerException ex) {
            // expected
        }
        
        // d. logicalArtifacts.storageID is not null, physicalArtifacts.storageID is not null
        logicalArtifacts = new TreeSet<ArtifactMetadata>(ArtifactMetadata.getComparator());
        physicalArtifacts = new TreeSet<ArtifactMetadata>(ArtifactMetadata.getComparator());
        ArtifactMetadata logicalMetadata = new ArtifactMetadata();
        ArtifactMetadata physicalMetadata = new ArtifactMetadata();
        logicalMetadata.storageID = "0000219ecaea66ea0b69d94570418f0444753cca538ab3e39835964041919e232806743c7815a4c3360f3acdfffbd7a2d06318a330af1f43a7add53621a842a9";
        physicalMetadata.storageID = "000080f6ffced3b719c0247b8d891de29c78cb5a1da2a9c765c857c55453fea25a52786c82f3281d0cd35b909b045d3f54b16409ed6efa6082c5aeba73c8ba04";
        logicalArtifacts.add(logicalMetadata);
        physicalArtifacts.add(physicalMetadata);
        validator.compareMetadata(logicalArtifacts, physicalArtifacts, start);
        
        // e. logicalArtifacts attributes are not null, physicalArtifacts.storageID is not null
        logicalArtifacts = new TreeSet<ArtifactMetadata>(ArtifactMetadata.getComparator());
        physicalArtifacts = new TreeSet<ArtifactMetadata>(ArtifactMetadata.getComparator());
        logicalMetadata = new ArtifactMetadata();
        physicalMetadata = new ArtifactMetadata();
        logicalMetadata.storageID = "0000219ecaea66ea0b69d94570418f0444753cca538ab3e39835964041919e232806743c7815a4c3360f3acdfffbd7a2d06318a330af1f43a7add53621a842a9";
        logicalMetadata.artifactURI = "mast:HST/product/id5n04lfq_drc.fits";
        logicalMetadata.checksum = "1043fe4c1a259a610fa9fb7ebff5833f";
        logicalMetadata.contentLength = "10";
        logicalMetadata.contentType = "logicalType";
        logicalMetadata.collection = "HST";
        logicalMetadata.lastModified = new Date();
        logicalMetadata.releaseDate = new Date();
        physicalMetadata.storageID = "000080f6ffced3b719c0247b8d891de29c78cb5a1da2a9c765c857c55453fea25a52786c82f3281d0cd35b909b045d3f54b16409ed6efa6082c5aeba73c8ba04";
        logicalArtifacts.add(logicalMetadata);
        physicalArtifacts.add(physicalMetadata);
        validator.compareMetadata(logicalArtifacts, physicalArtifacts, start);
        
        // f. logicalArtifacts storageID is not null, physicalArtifacts.attributes are not null
        logicalArtifacts = new TreeSet<ArtifactMetadata>(ArtifactMetadata.getComparator());
        physicalArtifacts = new TreeSet<ArtifactMetadata>(ArtifactMetadata.getComparator());
        logicalMetadata = new ArtifactMetadata();
        physicalMetadata = new ArtifactMetadata();
        logicalMetadata.storageID = "0000219ecaea66ea0b69d94570418f0444753cca538ab3e39835964041919e232806743c7815a4c3360f3acdfffbd7a2d06318a330af1f43a7add53621a842a9";
        physicalMetadata.artifactURI = "mast:HST/product/id5n04lfq_drc.fits";
        physicalMetadata.checksum = "1043fe4c1a259a610fa9fb7ebff5833f";
        physicalMetadata.contentLength = "10";
        physicalMetadata.contentType = "logicalType";
        physicalMetadata.collection = "HST";
        physicalMetadata.lastModified = new Date();
        physicalMetadata.releaseDate = new Date();
        physicalMetadata.storageID = "000080f6ffced3b719c0247b8d891de29c78cb5a1da2a9c765c857c55453fea25a52786c82f3281d0cd35b909b045d3f54b16409ed6efa6082c5aeba73c8ba04";
        logicalArtifacts.add(logicalMetadata);
        physicalArtifacts.add(physicalMetadata);
        validator.compareMetadata(logicalArtifacts, physicalArtifacts, start);
    }
}

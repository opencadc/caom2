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
*  $Revision: 5 $
*
************************************************************************
*/

package ca.nrc.cadc.caom2.artifactsync;

import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;

import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.harvester.state.HarvestSkipURI;
import ca.nrc.cadc.caom2.harvester.state.HarvestSkipURIDAO;
import ca.nrc.cadc.caom2.harvester.state.HarvestState;
import ca.nrc.cadc.caom2.harvester.state.HarvestStateDAO;
import ca.nrc.cadc.caom2.harvester.state.PostgresqlHarvestStateDAO;
import ca.nrc.cadc.caom2.persistence.ArtifactDAO;

public class ArtifactHarvester implements PrivilegedExceptionAction<Integer>
{

    public static final Integer DEFAULT_BATCH_SIZE = Integer.valueOf(1000);
    public static final String STATE_CLASS = Artifact.class.getSimpleName();

    private static final Logger log = Logger.getLogger(ArtifactHarvester.class);

    private ArtifactDAO artifactDAO;
    private ArtifactStore artifactStore;
    private HarvestStateDAO harvestStateDAO;
    private HarvestSkipURIDAO harvestSkipURIDAO;
    private String collection; // Will be used in the future
    private boolean dryrun;
    private int batchSize;
    private String source;


    public ArtifactHarvester(ArtifactDAO artifactDAO, String[] dbInfo, ArtifactStore artifactStore, String collection, boolean dryrun, int batchSize)
    {
        this.artifactDAO = artifactDAO;
        this.artifactStore = artifactStore;
        this.collection = collection;
        this.dryrun = dryrun;
        this.batchSize = batchSize;

        this.source = dbInfo[0] + "." + dbInfo[1] + "." + dbInfo[2];

        this.harvestStateDAO = new PostgresqlHarvestStateDAO(artifactDAO.getDataSource(), dbInfo[1], dbInfo[2]);
        this.harvestSkipURIDAO = new HarvestSkipURIDAO(artifactDAO.getDataSource(), dbInfo[1], dbInfo[2], batchSize);
    }

    @Override
    public Integer run() throws Exception
    {

        long downloadCount = 0;
        long newDownloadCount = 0;
        int num = 0;
        Date now = new Date();

        try
        {
            // Determine the state of the last run
            HarvestState state = harvestStateDAO.get(source, STATE_CLASS);

            // Use the ArtifactDAO to find artifacts with lastModified > last artifact processed
            List<Artifact> artifacts = artifactDAO.getList(Artifact.class, state.curLastModified, now, batchSize);
            num = artifacts.size();
            log.info("Found " + num + " artifacts to process.");

            for (Artifact artifact : artifacts)
            {

                // TEMPORARY:  For now, to keep the data volume low, only harvest
                // MAST files that begin with a W, X, Y, or Z
                List<String> acceptedFilePrefixes = Arrays.asList("w", "x", "y", "z");
                String path = artifact.getURI().getSchemeSpecificPart();
                log.debug("Path: " + path);
                int lastSlashIdx = path.lastIndexOf("/");
                String fileID = path.substring(lastSlashIdx + 1, path.length());
                log.debug("FileID: " + fileID);
                String firstChar = fileID.substring(0, 1).toLowerCase();
                if (!acceptedFilePrefixes.contains(firstChar))
                {
                    log.debug("Artifact " + artifact.getURI() + " skipped by temporary data volume restriction");
                }

                else
                {
                    try
                    {
                        // only process mast artifacts for now
                        if ("mast".equalsIgnoreCase(artifact.getURI().getScheme()))
                        {

                            boolean exists = artifactStore.contains(artifact.getURI(), artifact.contentChecksum);
                            log.debug("Artifact " + artifact.getURI() +
                                    " with MD5 " + artifact.contentChecksum + " exists: " + exists);
                            if (!exists)
                            {

                                // see if there's already an entry
                                HarvestSkipURI skip = harvestSkipURIDAO.get(source, STATE_CLASS, artifact.getURI());
                                if (skip == null)
                                {
                                    if (!dryrun)
                                    {
                                        log.info("--> Adding artifact to skip table: " + artifact.getURI());
                                        // set the message to be an empty string
                                        skip = new HarvestSkipURI(
                                                source, STATE_CLASS, artifact.getURI(), "");
                                        harvestSkipURIDAO.put(skip);
                                    }
                                    else
                                    {
                                        log.info("--> Artifact eligible for harvesting: " + artifact.getURI());
                                    }
                                    newDownloadCount++;
                                }
                                else
                                {
                                    log.debug("Artifact already exists in skip table.");
                                }
                                downloadCount++;
                            }

                            if (!dryrun)
                            {
                                state.curLastModified = artifact.getLastModified();
                                harvestStateDAO.put(state);
                                log.debug("Updated artifact harvest state.  Date: " + state.curLastModified);
                            }
                        }
                        else
                        {
                            log.debug("Skipping non-MAST artifact: " + artifact.getURI());
                        }
                    }
                    catch (Throwable t)
                    {
                        log.error("Failed to determine if artifact " + artifact.getURI() + " exists.", t);
                        if (!dryrun)
                        {
                            log.info("--> Adding artifact to skip table: " + artifact.getURI());
                            // set the message to be an empty string
                            HarvestSkipURI skip = new HarvestSkipURI(
                                    source, STATE_CLASS, artifact.getURI(), "");
                            harvestSkipURIDAO.put(skip);
                        }
                    }
                }
            }
            return num;
        }
        finally
        {
            log.info("Discovered " + downloadCount + " total artifacts eligible for download. (" +
                    newDownloadCount + " new)");
        }

    }

}

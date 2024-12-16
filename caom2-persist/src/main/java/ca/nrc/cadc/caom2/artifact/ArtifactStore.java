/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2023.                            (c) 2023.
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

package ca.nrc.cadc.caom2.artifact;

import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.util.FileMetadata;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.security.AccessControlException;
import java.util.Set;

/**
 * An interface to a CAOM2 artifact storage system.
 *
 * @author majorb
 */
public interface ArtifactStore {

    /**
     * Get the artifact metadata for the specified artifact.
     *
     * @param artifactURI
     *            The artifact identifier.
     * @return archive metadata object, null if none found.
     *
     * @throws ResourceNotFoundException
     *             If the artifact is not in storage
     * @throws UnsupportedOperationException
     *             If the checksum algorith is not supported.
     * @throws IllegalArgumentException
     *             If an aspect of the artifact uri is incorrect.
     * @throws AccessControlException
     *             If the calling user is not allowed to perform the query.
     * @throws TransientException
     *             If an unexpected runtime error occurs.
     * @throws RuntimeException
     *             If an unrecovarable error occurs.
     */
    public ArtifactMetadata get(URI artifactURI)
            throws TransientException, ResourceNotFoundException, 
            IllegalArgumentException, AccessControlException, IllegalStateException;

    /**
     * Get the storage policy based on the collection provided. TODO: this method will be
     * removed in a later version of the API (once tools that need it are directly configurable).
     *
     * @param collection
     *            The collection containing the files.
     * @return Storage policy.
     */
    public StoragePolicy getStoragePolicy(String collection);


    /**
     * Saves an artifact. The artifact will be replaced if artifact already exists 
     * with a different checksum.
     *
     * @param artifactURI
     *            The artifact identifier.
     * @param src
     *            URL of the source to access the data; should support HTTP Range requests
     * @param metadata
     *            Artifact metadata, including md5sum, contentLength and contentType
     *
     * @throws UnsupportedOperationException
     *             If the artifact uri cannot be resolved.
     * @throws UnsupportedOperationException
     *             If the checksum algorith is not supported.
     * @throws IllegalArgumentException
     *             If an aspect of the artifact uri is incorrect.
     * @throws AccessControlException
     *             If the calling user is not allowed to upload the artifact.
     * @throws IllegalStateException
     *             If the artifact already exists.
     * @throws InterruptedException
     *             If the transfer gets interrupted
     * @throws IOException
     *             If an IO exception occurs
     * @throws ResourceNotFoundException
     *             If the src URL points to a non-existent resource
     * @throws TransientException
     *             If an unexpected runtime error occurs.
     * @throws RuntimeException
     *             If an unrecovarable error occurs.
     */
    public void store(URI artifactURI, URL src, FileMetadata metadata)
            throws TransientException, UnsupportedOperationException, IllegalArgumentException, AccessControlException,
            IllegalStateException, InterruptedException, IOException, ResourceNotFoundException;

    /**
     * Get the list of all artifacts in a certain archive. TODO: this method will be
     * replaced by an iterator method in a later version of the API.
     *
     * @param namespace storage namespace (Artifact.uri prefix)
     * @return A set of archive metadata objects
     * @throws java.io.IOException
     * @throws java.lang.InterruptedException
     * @throws ca.nrc.cadc.net.ResourceNotFoundException
     */
    public Set<ArtifactMetadata> list(String namespace) throws IOException, InterruptedException,
            ResourceNotFoundException, TransientException, AccessControlException;

    /**
     * Convert an artifact URI to a storage ID.
     *
     * @param artifactURI
     *            The artifact URI to be converted.
     * @return A string representing the storage ID
     * @throws IllegalArgumentException
     *             If an aspect of the artifact uri is incorrect.
     */
    @Deprecated
    public String toStorageID(String artifactURI) throws IllegalArgumentException;

    /**
     * Process results from a batch of files downloaded.
     *
     * @param total
     *            Number of files processed.
     * @param successes
     *            Number of files actually downloaded.
     * @param totalElapsedTime
     *            Total elapsed time in ms.
     * @param totalBytes
     *            Total bytes downloaded.
     * @param threads
     *            Threads used.
     */
    public void processResults(long total, long successes, long totalElapsedTime, long totalBytes, int threads);

}

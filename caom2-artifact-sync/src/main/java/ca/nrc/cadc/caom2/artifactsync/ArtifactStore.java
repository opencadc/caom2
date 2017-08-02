package ca.nrc.cadc.caom2.artifactsync;

import java.io.InputStream;
import java.net.URI;

import ca.nrc.cadc.net.TransientException;

/**
 * An interface to a CAOM2 artifact storage system.
 *
 * @author majorb
 */
public interface ArtifactStore
{

    /**
     * Checks for artifact existence.
     *
     * @param artifactURI The artifact identifier.
     * @param checksum The checksum of the artifact.
     * @returns True in the artifact exists with the given checksum.
     * 
     * @throws UnsupportedOperationException If the artifact uri cannot be resolved.
     * @throws UnsupportedOperationException If the checksum algorith is not supported.
     * @throws TransientException If an unexpected runtime error occurs.
     */
    public boolean contains(URI artifactURI, URI checksum) throws TransientException, UnsupportedOperationException;

    /**
     * Saves an artifact.  The artifact will be replaced if artifact already exists with a
     * different checksum.
     *
     * @param artifactURI The artifact identifier.
     * @param checksum The checksum of the artifact.
     * @param data The artifact data.
     * 
     * @throws UnsupportedOperationException If the artifact uri cannot be resolved.
     * @throws UnsupportedOperationException If the checksum algorith is not supported.
     * @throws TransientException If an unexpected runtime error occurs.
     */
    public void store(URI artifactURI, URI checksum, InputStream data) throws TransientException;

}

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
     */
    public boolean contains(URI artifactURI, URI checksum) throws TransientException;

    /**
     * Saves an artifact.  The artifact will be replaced if artifact already exists with a
     * different checksum.
     *
     * @param artifactURI The artifact identifier.
     * @param checksum The checksum of the artifact.
     * @param data The artifact data.
     */
    public void store(URI artifactURI, URI checksum, InputStream data) throws TransientException;

}

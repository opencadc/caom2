package ca.nrc.cadc.caom2.artifactsync;

import ca.nrc.cadc.caom2.repo.client.RepoClient;

public class DetectMissingArtifacts implements Runnable
{

    public DetectMissingArtifacts(RepoClient client, ArtifactStore store, String collection, boolean dryrun)
    {
        store.contains(artifactURI, checksum)
    }

    @Override
    public void run()
    {
    }

}

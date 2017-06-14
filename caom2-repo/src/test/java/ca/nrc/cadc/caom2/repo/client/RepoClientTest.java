package ca.nrc.cadc.caom2.repo.client;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.ObservationState;
import ca.nrc.cadc.caom2.ObservationURI;
import ca.nrc.cadc.util.Log4jInit;

public class RepoClientTest {

    private static final Logger log = Logger.getLogger(RepoClientTest.class);

    static {
        Log4jInit.setLevel("ca.nrc.cadc.caom2.repo.client.RepoClient", Level.DEBUG);

    }

    // @Test
    public void testTemplate() {
        try {

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testGetObservationList() {
        try {

            RepoClient repoC = null;
            try {
                repoC = new RepoClient(new URI("ivo://cadc.nrc.ca/caom2repo"), "IRIS", 8);
            } catch (URISyntaxException e) {
                throw new RuntimeException(
                        "Unable to create RepoClient instance for URI ivo://cadc.nrc.ca/caom2repo and collection IRIS");
            }

            List<ObservationState> list = repoC.getObservationList("IRIS", null, null, 5);

            Assert.assertEquals(list.size(), 5);
            Assert.assertEquals(list.get(0).getObservationID(), "f001h000");
            Assert.assertEquals(list.get(1).getObservationID(), "f002h000");
            Assert.assertEquals(list.get(2).getObservationID(), "f003h000");
            Assert.assertEquals(list.get(3).getObservationID(), "f004h000");
            Assert.assertEquals(list.get(4).getObservationID(), "f005h000");

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testGetList() {
        try {

            RepoClient repoC = null;
            try {
                repoC = new RepoClient(new URI("ivo://cadc.nrc.ca/caom2repo"), "IRIS", 8);
            } catch (URISyntaxException e) {
                throw new RuntimeException(
                        "Unable to create RepoClient instance for URI ivo://cadc.nrc.ca/caom2repo and collection IRIS");
            }

            List<Observation> list = repoC.getList(Observation.class, null, null, 5, 1);

            Assert.assertEquals(list.size(), 5);

            Assert.assertEquals(list.get(0).getID().toString(),
                    "00000000-0000-0000-897c-013ac26a8f32");
            Assert.assertEquals(list.get(1).getID().toString(),
                    "00000000-0000-0000-31a6-013ac26a8284");
            Assert.assertEquals(list.get(2).getID().toString(),
                    "00000000-0000-0000-7a3c-013ac28ebeca");
            Assert.assertEquals(list.get(3).getID().toString(),
                    "00000000-0000-0000-95c3-01394aa6a272");
            Assert.assertEquals(list.get(4).getID().toString(),
                    "00000000-0000-0000-8117-013ac28ddb9b");
            ;

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testGet() {
        try {

            RepoClient repoC = null;
            try {
                repoC = new RepoClient(new URI("ivo://cadc.nrc.ca/caom2repo"), "IRIS", 8);
            } catch (URISyntaxException e) {
                throw new RuntimeException(
                        "Unable to create RepoClient instance for URI ivo://cadc.nrc.ca/caom2repo and collection IRIS");
            }

            Observation obs = repoC.get(new ObservationURI("IRIS", "f001h000"));

            Assert.assertEquals(obs.getID().toString(), "00000000-0000-0000-897c-013ac26a8f32");

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

}

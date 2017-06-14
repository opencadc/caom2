package ca.nrc.cadc.caom2.repo.client;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import ca.nrc.cadc.caom2.ObservationState;
import ca.nrc.cadc.util.Log4jInit;

public class RepoClientTest {

    private static final Logger log = Logger.getLogger(RepoClientTest.class);

    static {
        Log4jInit.setLevel("ca.nrc.cadc.caom2", Level.INFO);
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

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

}

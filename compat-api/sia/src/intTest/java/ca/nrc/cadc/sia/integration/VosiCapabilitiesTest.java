package ca.nrc.cadc.sia.integration;

import ca.nrc.cadc.vosi.CapabilitiesTest;
import org.apache.log4j.Logger;

import java.net.URI;

public class VosiCapabilitiesTest extends CapabilitiesTest
{
    private static final Logger log = Logger.getLogger(VosiCapabilitiesTest.class);

    public VosiCapabilitiesTest()
    {
        super(URI.create("ivo://cadc.nrc.ca/sia"));
    }
}

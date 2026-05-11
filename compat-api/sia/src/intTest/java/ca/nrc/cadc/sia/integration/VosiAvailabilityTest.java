package ca.nrc.cadc.sia.integration;

import ca.nrc.cadc.vosi.AvailabilityTest;
import org.apache.log4j.Logger;

import java.net.URI;

public class VosiAvailabilityTest extends AvailabilityTest
{
    private static final Logger log = Logger.getLogger(VosiAvailabilityTest.class);

    public VosiAvailabilityTest()
    {
        super(URI.create("ivo://cadc.nrc.ca/sia"));
    }
}

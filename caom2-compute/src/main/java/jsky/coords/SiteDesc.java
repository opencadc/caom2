/*
 * Copyright 2003 Association for Universities for Research in Astronomy, Inc.,
 * Observatory Control System, Gemini Telescopes Project.
 *
 * $Id: SiteDesc.java,v 1.1.1.1 2009/02/17 22:50:08 abrighto Exp $
 */

package jsky.coords;

import java.util.TimeZone;


/**
 * A simple class containing a telescope site name, it's coordinates, and
 * time zone.
 *
 * @version $Revision: 1.1.1.1 $
 * @author Allan Brighton
 */
public class SiteDesc {

    private String _name;
    private double _longitude;
    private double _latitude;
    private TimeZone _timeZone;

    public SiteDesc(String name, double longitude, double latitude, TimeZone timeZone) {
        _name = name;
        _longitude = longitude;
        _latitude = latitude;
        _timeZone = timeZone;
    }

    public String getName() {
        return _name;
    }

    public String toString() {
        return _name;
    }

    public double getLongitude() {
        return _longitude;
    }

    public double getLatitude() {
        return _latitude;
    }

    public TimeZone getTimeZone() {
        return _timeZone;
    }
}


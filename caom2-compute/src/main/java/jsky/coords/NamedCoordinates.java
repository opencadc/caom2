/*
 * Copyright 2001 Association for Universities for Research in Astronomy, Inc.,
 * Observatory Control System, Gemini Telescopes Project.
 *
 * $Id: NamedCoordinates.java,v 1.1.1.1 2009/02/17 22:50:11 abrighto Exp $
 */

package jsky.coords;


/**
 * Simple class containing an object name, its coordinates, and the brightness,
 * if known. The brightness is an optional string (for display) describing the
 * object's brightness, for example: "mag: 13.2", or "12.3B, 12.1V".
 * The brightness may also be null or empty, if not known.
 *
 * @version $Revision: 1.1.1.1 $
 * @author Allan Brighton
 */
public class NamedCoordinates {

    private String name;
    private Coordinates coords;
    private String brightness;

    public NamedCoordinates(String name, Coordinates coords) {
        this.name = name;
        this.coords = coords;
        brightness = null;
    }

    public NamedCoordinates(String name, Coordinates coords, String brightness) {
        this.name = name;
        this.coords = coords;
        this.brightness = brightness;
    }

    public String getName() {
        return name;
    }

    public String toString() {
        return name;
    }

    public Coordinates getCoordinates() {
        return coords;
    }

    public String getBrightness() {
        return brightness;
    }
}


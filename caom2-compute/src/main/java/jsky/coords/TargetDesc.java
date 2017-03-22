/*
 * Copyright 2003 Association for Universities for Research in Astronomy, Inc.,
 * Observatory Control System, Gemini Telescopes Project.
 *
 * $Id: TargetDesc.java,v 1.1.1.1 2009/02/17 22:50:02 abrighto Exp $
 */

package jsky.coords;


/**
 * A simple class describing a target object and some related information.
 *
 * @version $Revision: 1.1.1.1 $
 * @author Allan Brighton
 */
public class TargetDesc {

    private String _name;
    private WorldCoords _coords;
    private String _description;
    private String _priority;
    private String _category;

    public TargetDesc(String name, WorldCoords coords) {
        _name = name;
        _coords = coords;
        _description = name;
        _priority = "";
        _category = "";
    }

    public TargetDesc(String name, WorldCoords coords, String description, String priority, String category) {
        _name = name;
        _coords = coords;
        _description = description;
        _priority = priority;
        _category = category;
    }

    public String getName() {
        return _name;
    }

    public String toString() {
        return _name;
    }

    public WorldCoords getCoordinates() {
        return _coords;
    }

    public String getDescription() {
        return _description;
    }

    public String getPriority() {
        return _priority;
    }

    public String getCategory() {
        return _category;
    }

    /** Return an array of one or more Strings describing the target */
    public String[] getDescriptionFields() {
        return new String[]{_name};
    }
}


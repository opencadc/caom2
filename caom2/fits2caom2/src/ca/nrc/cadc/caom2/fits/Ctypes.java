/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2011.                            (c) 2011.
*  Government of Canada                 Gouvernement du Canada
*  National Research Council            Conseil national de recherches
*  Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
*  All rights reserved                  Tous droits réservés
*
*  NRC disclaims any warranties,        Le CNRC dénie toute garantie
*  expressed, implied, or               énoncée, implicite ou légale,
*  statutory, of any kind with          de quelque nature que ce
*  respect to the software,             soit, concernant le logiciel,
*  including without limitation         y compris sans restriction
*  any warranty of merchantability      toute garantie de valeur
*  or fitness for a particular          marchande ou de pertinence
*  purpose. NRC shall not be            pour un usage particulier.
*  liable in any event for any          Le CNRC ne pourra en aucun cas
*  damages, whether direct or           être tenu responsable de tout
*  indirect, special or general,        dommage, direct ou indirect,
*  consequential or incidental,         particulier ou général,
*  arising from the use of the          accessoire ou fortuit, résultant
*  software.  Neither the name          de l'utilisation du logiciel. Ni
*  of the National Research             le nom du Conseil National de
*  Council of Canada nor the            Recherches du Canada ni les noms
*  names of its contributors may        de ses  participants ne peuvent
*  be used to endorse or promote        être utilisés pour approuver ou
*  products derived from this           promouvoir les produits dérivés
*  software without specific prior      de ce logiciel sans autorisation
*  written permission.                  préalable et particulière
*                                       par écrit.
*
*  This file is part of the             Ce fichier fait partie du projet
*  OpenCADC project.                    OpenCADC.
*
*  OpenCADC is free software:           OpenCADC est un logiciel libre ;
*  you can redistribute it and/or       vous pouvez le redistribuer ou le
*  modify it under the terms of         modifier suivant les termes de
*  the GNU Affero General Public        la “GNU Affero General Public
*  License as published by the          License” telle que publiée
*  Free Software Foundation,            par la Free Software Foundation
*  either version 3 of the              : soit la version 3 de cette
*  License, or (at your option)         licence, soit (à votre gré)
*  any later version.                   toute version ultérieure.
*
*  OpenCADC is distributed in the       OpenCADC est distribué
*  hope that it will be useful,         dans l’espoir qu’il vous
*  but WITHOUT ANY WARRANTY;            sera utile, mais SANS AUCUNE
*  without even the implied             GARANTIE : sans même la garantie
*  warranty of MERCHANTABILITY          implicite de COMMERCIALISABILITÉ
*  or FITNESS FOR A PARTICULAR          ni d’ADÉQUATION À UN OBJECTIF
*  PURPOSE.  See the GNU Affero         PARTICULIER. Consultez la Licence
*  General Public License for           Générale Publique GNU Affero
*  more details.                        pour plus de détails.
*
*  You should have received             Vous devriez avoir reçu une
*  a copy of the GNU Affero             copie de la Licence Générale
*  General Public License along         Publique GNU Affero avec
*  with OpenCADC.  If not, see          OpenCADC ; si ce n’est
*  <http://www.gnu.org/licenses/>.      pas le cas, consultez :
*                                       <http://www.gnu.org/licenses/>.
*
*  $Revision: 5 $
*
************************************************************************
*/
package ca.nrc.cadc.caom2.fits;

/**
 * Class to determine whether a CTYPE is spatial, spectral, temporal, or polarization.
 * 
 * @author jburke
 */
public abstract class Ctypes
{
    public static final int UNKNOWN_WCS = 0;
    public static final int POSITION_WCS = 1;
    public static final int ENERGY_WCS = 2;
    public static final int TIME_WCS = 3;
    public static final int POLARIZATION_WCS = 4;
    
    // Position CYTPE's.
    public static String[] position = new String[]
    {
        "RA",   "DEC",
        "GLON", "GLAT",
        "ELON", "ELAT",
        "HLON", "HLAT",
        "SLON", "SLAT",
        "CUBEFACE"
    };
        
    // Energy CTYPE's.
    public static String[] energy = new String[]
    {
        "FREQ",
        "ENER",
        "WAVN",
        "VRAD",
        "WAVE",
        "VOPT",
        "ZOPT",
        "AWAV",
        "VELO",
        "BETA"
    };
    
    // Time CTYPE's.
    // From http://hea-www.cfa.harvard.edu/~arots/TimeWCS/
    public static String[] time = new String[]
    {
        "TIME",
        "TAI",
        "TT",
        "TDT",
        "ET",
        "IAT",
        "UT1",
        "UTC",
        "GMT",
        "GPS",
        "TCG",
        "TCB",
        "TDB",
        "LOCAL"
    };
    
    // Polarization CTYPE's.
    public static String[] polarization = new String[]
    {
        "STOKES"
    };
    
    /**
     * Determines the coordinate type for a given ctype.
     * 
     * @param ctype the CTYPE to test.
     * @return an int representing the coordinate type of the ctype,
     *         or 0 if the ctype is unknown.
     */
    public static int getWCSType(String ctype)
    {
        String type = getTypeCode(ctype);
        
        for (String s : position)
            if (type.equals(s))
                return POSITION_WCS;
        
        for (String s : energy)
            if (type.equals(s))
                return ENERGY_WCS;
        
        for (String s : time)
            if (type.equals(s))
                return TIME_WCS;
        
        for (String s : polarization)
            if (type.equals(s))
                return POLARIZATION_WCS;
        
        return UNKNOWN_WCS;
    }
    
    /**
     * Test if ctype is a position CTYPE.
     * 
     * @param ctype the CTYPE to test.
     * @return true if ctype is a position CTYPE, false otherwise.
     */
    public static boolean isPositionCtype(String ctype)
    {
        // TODO WCS also defines yzLN and yzLT as possible CTYPE's without
        // defining possible values for yz. 
        // Do we need to check if chars 3 and 4 are LN or LT?
        String type = getTypeCode(ctype);
        for (String s : position)
            if (type.equals(s))
                return true;
        return false;
    }
    
    /**
     * Test if ctype is a energy CTYPE.
     * 
     * @param ctype the CTYPE to test.
     * @return true if ctype is a energy CTYPE, false otherwise.
     */
    public static boolean isEnergyCtype(String ctype)
    {
        String type = getTypeCode(ctype);
        for (String s : energy)
            if (type.equals(s))
                return true;
        return false;
    }
    
    /**
     * Test if ctype is a time CTYPE.
     * 
     * @param ctype the CTYPE to test.
     * @return true if ctype is a time CTYPE, false otherwise.
     */
    public static boolean isTimeCtype(String ctype)
    {
        String type = getTypeCode(ctype);
        for (String s : time)
            if (type.equals(s))
                return true;
        return false;
    }
    
    /**
     * Test if ctype is a polarization CTYPE.
     * 
     * @param ctype the CTYPE to test.
     * @return true if ctype is a polarization CTYPE, false otherwise.
     */
    public static boolean isPolarizationCtype(String ctype)
    {
        String type = getTypeCode(ctype);
        for (String s : polarization)
            if (type.equals(s))
                return true;
        return false;
    }
    
    // Return the type code of the CTYPE, which is the part 
    // of the CTYPE before the first '-' character.
    private static String getTypeCode(String ctype)
    {
        int index = ctype.indexOf('-');
        if (index == -1)
            return ctype;
        return ctype.substring(0, index);
    }
    
}

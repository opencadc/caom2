/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2017.                            (c) 2017.
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
************************************************************************
*/

package ca.nrc.cadc.caom2.compute;


import ca.nrc.cadc.caom2.*;
//import ca.nrc.cadc.caom2.types.IllegalPolygonException;
//import ca.nrc.cadc.caom2.types.MultiPolygon;
//import ca.nrc.cadc.caom2.types.Point;
//import ca.nrc.cadc.caom2.types.Polygon;
//import ca.nrc.cadc.caom2.wcs.*;
//import ca.nrc.cadc.wcs.Transform;
import ca.nrc.cadc.wcs.exceptions.NoSuchKeywordException;
import ca.nrc.cadc.wcs.exceptions.WCSLibRuntimeException;
import jsky.coords.wcscon;
import org.apache.log4j.Logger;

import java.awt.geom.Point2D;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Utility class to assign values to fields marked with the computed stereotype
 * in the data model.
 * 
 * @author pdowler
 */
public class ComputeUtil 
{
    private static final Logger log = Logger.getLogger(ComputeUtil.class);
    public static final double MAX_SANE_AREA = 250.0; // square degrees, CGPS has 235

    private ComputeUtil() { }
    
    /**
     * Clear computed plane metadata.
     * @deprecated 
     */
    public static void clearTransientState(Plane p)
    {
        p.position = null;
        p.energy = null;
        p.time = null;
        p.polarization = null;
    }
    
    /**
     * Compute plane metadata from WCS.
     * @deprecated 
     */
    public static void computeTransientState(Observation o, Plane p)
    {
        computePosition(p);
        computeEnergy(p);
        computeTime(p);
        computePolarization(p);
    }


    private static void computePosition(Plane p)
    {
        try
        {
            p.position = PositionUtil.compute(p.getArtifacts());
        }
        catch(NoSuchKeywordException ex)
        {
            throw new IllegalArgumentException("failed to compute Plane.position", ex);
        }
        catch(WCSLibRuntimeException ex)
        {
            throw new IllegalArgumentException("failed to compute Plane.position", ex);
        }
    }

    private static void computeEnergy(Plane p)
    {
        try
        {
            p.energy = EnergyUtil.compute(p.getArtifacts());
        }
        catch(NoSuchKeywordException ex)
        {
            throw new IllegalArgumentException("failed to compute Plane.energy", ex);
        }
        catch(WCSLibRuntimeException ex)
        {
            throw new IllegalArgumentException("failed to compute Plane.energy", ex);
        }
    }

    private static void computeTime(Plane p)
    {
        p.time = TimeUtil.compute(p.getArtifacts());
    }

    private static void computePolarization(Plane p)
    {
        p.polarization = PolarizationUtil.compute(p.getArtifacts());
    }

}

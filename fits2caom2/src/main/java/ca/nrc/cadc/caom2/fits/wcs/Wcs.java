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
*  $Revision: 4 $
*
************************************************************************
*/
package ca.nrc.cadc.caom2.fits.wcs;

import ca.nrc.cadc.caom2.EnergyTransition;
import ca.nrc.cadc.caom2.fits.Ctypes;
import ca.nrc.cadc.caom2.fits.FitsMapping;
import ca.nrc.cadc.caom2.wcs.Axis;
import ca.nrc.cadc.caom2.wcs.Coord2D;
import ca.nrc.cadc.caom2.wcs.CoordAxis1D;
import ca.nrc.cadc.caom2.wcs.CoordAxis2D;
import ca.nrc.cadc.caom2.wcs.CoordBounds1D;
import ca.nrc.cadc.caom2.wcs.CoordBounds2D;
import ca.nrc.cadc.caom2.wcs.CoordCircle2D;
import ca.nrc.cadc.caom2.wcs.CoordError;
import ca.nrc.cadc.caom2.wcs.CoordFunction1D;
import ca.nrc.cadc.caom2.wcs.CoordFunction2D;
import ca.nrc.cadc.caom2.wcs.CoordPolygon2D;
import ca.nrc.cadc.caom2.wcs.CoordRange1D;
import ca.nrc.cadc.caom2.wcs.CoordRange2D;
import ca.nrc.cadc.caom2.wcs.Dimension2D;
import ca.nrc.cadc.caom2.wcs.RefCoord;
import ca.nrc.cadc.caom2.wcs.Slice;
import ca.nrc.cadc.caom2.wcs.ValueCoord2D;
import java.util.List;
import org.apache.log4j.Logger;

/**
 *
 * @author jburke
 */
public class Wcs
{
    private static final Logger log = Logger.getLogger(Wcs.class);
    
    public static Integer[] getPositionAxis(Integer naxis, FitsMapping mapping)
    {
        String ctype0 = null;
        String ctype1 = null;
        Integer[] axis = new Integer[] { null, null };
        if (naxis != null && !FitsMapping.IGNORE.equals(mapping.getConfig().get("Chunk.position")))
        {
            for (int i = 1; i <= naxis.intValue(); i++)
            {
                String ctype = mapping.getKeywordValue("CTYPE" + i);
                if (ctype == null)
                    continue;
                if (Ctypes.isPositionCtype(ctype))
                {
                    if (axis[0] == null)
                    {
                        axis[0] = Integer.valueOf(i);
                        ctype0 = ctype;
                    }
                    else
                    {
                        axis[1] = Integer.valueOf(i);
                        ctype1 = ctype;
                    }
                }
            }
        }
        if (axis[0] != null && axis[1] != null)
        {
            log.info(mapping.uri + "[" + mapping.extension + "] CTYPE" + axis[0] + "=" + ctype0 + " - positionAxis" + axis[0]);
            log.info(mapping.uri + "[" + mapping.extension + "] CTYPE" + axis[1] + "=" + ctype1 + " - positionAxis" + axis[1]);
        }
        return axis;
    }
    
    public static Integer getEnergyAxis(Integer naxis, FitsMapping mapping)
    {
        String ctype = null;
        Integer axis = null;
        if (naxis != null && !FitsMapping.IGNORE.equals(mapping.getConfig().get("Chunk.energy")))
        {
            for (int i = 1; i <= naxis.intValue(); i++)
            {
                ctype = mapping.getKeywordValue("CTYPE" + i);
                if (ctype == null)
                    continue;
                if (Ctypes.isEnergyCtype(ctype))
                {
                    axis = Integer.valueOf(i);
                    break;
                }
            }
        }
        if (axis != null)
            log.info(mapping.uri + "[" + mapping.extension + "] CYTPE" + axis + "=" + ctype + " - energyAxis" + axis);
        return axis;
    }
    
    public static Integer getTimeAxis(Integer naxis, FitsMapping mapping)
    {
        String ctype = null;
        Integer axis = null;
        if (naxis != null && !FitsMapping.IGNORE.equals(mapping.getConfig().get("Chunk.time")))
        {
            for (int i = 1; i <= naxis.intValue(); i++)
            {
                ctype = mapping.getKeywordValue("CTYPE" + i);
                if (ctype == null)
                    continue;
                if (Ctypes.isTimeCtype(ctype))
                {
                    axis = Integer.valueOf(i);
                    break;
                }
            }
        }
        if (axis != null)
            log.info(mapping.uri + "[" + mapping.extension + "] CYTPE" + axis + "=" + ctype + " - timeAxis" + axis);
        return axis;
    }
    
    public static Integer getPolarizationAxis(Integer naxis, FitsMapping mapping)
    {
        String ctype = null;
        Integer axis = null;
        if (naxis != null && !FitsMapping.IGNORE.equals(mapping.getConfig().get("Chunk.polarization")))
        {
            for (int i = 1; i <= naxis.intValue(); i++)
            {
                ctype = mapping.getKeywordValue("CTYPE" + i);
                if (ctype == null)
                    continue;
                if (Ctypes.isPolarizationCtype(ctype))
                {
                    axis = Integer.valueOf(i);
                    break;
                }
            }
        }
        if (axis != null)
            log.info(mapping.uri + "[" + mapping.extension + "] CYTPE" + axis + "=" + ctype + " - timeAxis" + axis);
        return axis;
    }
    
    public static Integer getObservableAxis(Integer naxis, FitsMapping mapping)
    {
        String ctype = null;
        Integer axis = null;
        if (naxis != null && !FitsMapping.IGNORE.equals(mapping.getConfig().get("Chunk.observable")))
        {
            for (int i = 1; i <= naxis.intValue(); i++)
            {
                ctype = mapping.getKeywordValue("CTYPE" + i);
                if (ctype == null)
                    continue;
                if (!Ctypes.isPositionCtype(ctype) &&
                    !Ctypes.isEnergyCtype(ctype) &&
                    !Ctypes.isTimeCtype(ctype) &&
                    !Ctypes.isPolarizationCtype(ctype))
                {
                    axis = Integer.valueOf(i);
                    break;
                }
            }
        }
        if (axis != null)
            log.info(mapping.uri + "[" + mapping.extension + "] CYTPE" + axis + "=" + ctype + " - observableAxis" + axis);
        return axis;
    }
        
    public static EnergyTransition getEnergyTransition(String utype, FitsMapping mapping)
    {
        String species = mapping.getMapping(utype + ".species");
        String transition = mapping.getMapping(utype + ".transition");
        if (species == null && transition == null)
            return null;
        return new EnergyTransition(species, transition);
    }
    
    public static Axis getAxis(String utype, FitsMapping mapping)
    {
        String ctype = mapping.getMapping(utype + ".ctype");
        String cunit = mapping.getMapping(utype + ".cunit");
        if (cunit != null) {
            cunit = cunit.trim();
            if (cunit.isEmpty()) {
                cunit = null;
            }
        }
        if (ctype == null && cunit == null)
            return null;
        return new Axis(ctype, cunit);
    }
    
    public static Coord2D getCoord2D(String utype, FitsMapping mapping)
    {
        RefCoord coord1 = getRefCoord(utype + ".coord1", mapping);
        RefCoord coord2 = getRefCoord(utype + ".coord2", mapping);
        if (coord1 == null && coord2 == null)
            return null;
        return new Coord2D(coord1, coord2); 
    }
    
    public static CoordAxis1D getCoordAxis1D(String utype, FitsMapping mapping)
    {
        Axis axis = getAxis(utype + ".axis", mapping);
        if (axis == null)
            return null;
        
        CoordAxis1D coordAxis1D = new CoordAxis1D(axis);
        coordAxis1D.bounds = getCoordBounds1D(utype + ".bounds", mapping);
        coordAxis1D.error = getCoordError(utype + ".error", mapping);
        coordAxis1D.function = getCoordFunction1D(utype + ".function", mapping);
        coordAxis1D.range = getCoordRange1D(utype + ".range", mapping);
        return coordAxis1D;
    }
    
    public static CoordAxis2D getCoordAxis2D(String utype, FitsMapping mapping)
    {
        Axis axis1 = getAxis(utype + ".axis1", mapping);
        Axis axis2 = getAxis(utype + ".axis2", mapping);
        if (axis1 == null && axis2 == null)
            return null;
        
        CoordAxis2D coordAxis2D = new CoordAxis2D(axis1, axis2);
        coordAxis2D.bounds = getCoordBounds2D(utype + ".bounds", mapping);
        coordAxis2D.error1 = getCoordError(utype + ".error1", mapping);
        coordAxis2D.error2 = getCoordError(utype + ".error2", mapping);
        coordAxis2D.function = getCoordFunction2D(utype + ".function", mapping);
        coordAxis2D.range = getCoordRange2D(utype + ".range", mapping);
        return coordAxis2D;
    }
    
    public static CoordBounds1D getCoordBounds1D(String utype, FitsMapping mapping)
    {
        List<CoordRange1D> samples = getSamples(utype + ".samples", mapping);
        if (samples == null)
            return null;
        CoordBounds1D bounds = new CoordBounds1D();
        for (CoordRange1D range : samples)
        {
            bounds.getSamples().add(range);
        }
        return bounds;
    }
    
    public static CoordBounds2D getCoordBounds2D(String utype, FitsMapping mapping)
    {
        // Look for a CoordCircle2D
        CoordCircle2D circle = getCoordCircle2D(utype, mapping);
        if (circle != null)
            return circle;
        
        // Look for a CoordPolygon2D
        CoordPolygon2D polygon = getCoordPolygon2D(utype, mapping);
        if (polygon != null)
            return polygon;
        
        return null;
    }
    
    public static CoordCircle2D getCoordCircle2D(String utype, FitsMapping mapping)
    {
        ValueCoord2D center = getValueCoord2D(utype + ".center", mapping);
        Double radius = getDoubleValue(utype + ".radius", mapping);
        if (center == null && radius == null)
            return null;
        return new CoordCircle2D(center, radius);
    }
    
    public static CoordError getCoordError(String utype, FitsMapping mapping)
    {
        Double syser = getDoubleValue(utype + ".syser", mapping);
        Double rnder = getDoubleValue(utype + ".rnder", mapping);
        if (syser == null && rnder == null)
            return null;
        return new CoordError(syser, rnder);
    }
    
    public static CoordFunction1D getCoordFunction1D(String utype, FitsMapping mapping)
    {
        Long naxis = getLongValue(utype + ".naxis", mapping);
        Double delta = getDoubleValue(utype + ".delta", mapping);
        if (delta == null)
        {
            int i = 0;
            if (utype.startsWith("Chunk.energy"))
                i = mapping.energyAxis;
            else if (utype.startsWith("Chunk.time"))
                i = mapping.timeAxis;
            else if (utype.startsWith("Chunk.polarization"))
                i = mapping.polarizationAxis;
            if (i > 0)
            {
                StringBuilder sb = new StringBuilder();
                String kw = sb.append("CD").append(i).append("_").append(i).toString();
                String cdnn = mapping.getKeywordValue(kw);
                if (cdnn != null)
                {
                    try
                    {
                        delta = Double.valueOf(cdnn);
                    }
                    catch(NumberFormatException ex)
                    {
                        log.debug("NumberFormatException: " + kw + " = " + cdnn);
                    }
                }
            }
        }
        RefCoord refCoord = getRefCoord(utype + ".refCoord", mapping);
        if (naxis == null && delta == null && refCoord == null)
            return null;
        return new CoordFunction1D(naxis, delta, refCoord);
    }
    
    public static CoordFunction2D getCoordFunction2D(String utype, FitsMapping mapping)
    {
        Dimension2D dimension = getDimension2D(utype + ".dimension", mapping);
        Coord2D refCoord = getCoord2D(utype + ".refCoord", mapping);
        if (dimension == null || refCoord == null)
            return null;

        // Look for the CD matrix first.
        Double cd11 = getDoubleValue(utype + ".cd11", mapping);
        Double cd12 = getDoubleValue(utype + ".cd12", mapping);
        Double cd21 = getDoubleValue(utype + ".cd21", mapping);
        Double cd22 = getDoubleValue(utype + ".cd22", mapping);
        if (cd11 != null || cd12 != null || cd21 != null || cd22 != null)
        {
            if (cd11 == null) {
                cd11 = 0.0;
            }
            if (cd12 == null) {
                cd12 = 0.0;
            }
            if (cd21 == null) {
                cd21 = 0.0;
            }
            if (cd22 == null) {
                cd22 = 0.0;
            }
            return new CoordFunction2D(dimension, refCoord, cd11, cd12, cd21, cd22);
        }

        // Get the CDELT's
        int posAxis1 = mapping.positionAxis1;
        int posAxis2 = mapping.positionAxis2;
        String cdelt1_kw = mapping.getKeywordValue("CDELT" + posAxis1);
        String cdelt2_kw = mapping.getKeywordValue("CDELT" + posAxis2);
        Double cdelt1 = parseDouble(cdelt1_kw, "CDELT" + posAxis1);
        Double cdelt2 = parseDouble(cdelt2_kw, "CDELT" + posAxis1);

        // Look for PIXSCALE if CDELT{i,j} not found
        if (cdelt1 == null && cdelt2 == null) {
            String pixscale_kw = mapping.getKeywordValue("PIXSCALE");
            Double pixscale = parseDouble(pixscale_kw, "PIXSCALE");
            if (pixscale != null) {
                // pixscale is in arcseconds, divide by 3600 to get degrees.
                cdelt1 = pixscale/36000;
                cdelt2 = cdelt1;
            }
        }

        // A CD matrix cannot be generated using a PC matrix
        // if CDELTS{i,j} are not found. Return null if CDELT's are null.
        if (cdelt1 == null || cdelt2 == null) {
            return null;
        }

        // The WCS FITS standard specifies that the CD and PC matrix
        // indices are not prefixed with a 0, i.e. CD1_1 instead of
        // CD01_01. To process data that does use 0's for the PC indices
        // we check the headers for both.
        Double pc11 = null;
        Double pc12 = null;
        Double pc21 = null;
        Double pc22 = null;
        String kw11 = String.format("PC%d_%d", posAxis1, posAxis1);
        String s11 = mapping.getKeywordValue(kw11);
        if (s11 == null) {
            kw11 = String.format("PC0%d_0%d", posAxis1, posAxis1);
            s11 = mapping.getKeywordValue(kw11);
        }
        pc11 = parseDouble(s11, kw11);

        String kw12 = String.format("PC%d_%d", posAxis1, posAxis2);
        String s12 = mapping.getKeywordValue(kw12);
        if (s12 == null) {
            kw12 = String.format("PC0%d_0%d", posAxis1, posAxis2);
            s12 = mapping.getKeywordValue(kw12);
        }
        pc12 = parseDouble(s12, kw12);

        String kw21 = String.format("PC%d_%d", posAxis2, posAxis1);
        String s21 = mapping.getKeywordValue(kw21);
        if (s21 == null) {
            kw21 = String.format("PC0%d_0%d", posAxis2, posAxis1);
            s21 = mapping.getKeywordValue(kw21);
        }
        pc21 = parseDouble(s21, kw21);

        String kw22 = String.format("PC%d_%d", posAxis2, posAxis2);
        String s22 = mapping.getKeywordValue(kw22);
        if (s22 == null) {
            kw22 = String.format("PC0%d_0%d", posAxis2, posAxis2);
            s22 = mapping.getKeywordValue(kw22);
        }
        pc22 = parseDouble(s22, kw22);

        if (pc11 != null || pc12 != null || pc21 != null || pc22 != null) {
            if (pc11 == null) {
                pc11 = 1.0;
            }
            if (pc12 == null) {
                pc12 = 0.0;
            }
            if (pc21 == null) {
                pc21 = 0.0;
            }
            if (pc22 == null) {
                pc22 = 1.0;
            }
        }

        if (pc11 == null && pc12 == null && pc21 == null && pc22 == null) {
            // Create a default PC matrix and apply CROTA if found.
            pc11 = 1.0;
            pc12 = 0.0;
            pc21 = 0.0;
            pc22 = 1.0;

            String crota_kw = mapping.getKeywordValue("CROTA" + posAxis1);
            Double crota = parseDouble(crota_kw, "CROTA" + posAxis1);
            if (crota == null) {
                crota_kw = mapping.getKeywordValue("CROTA" + posAxis2);
                crota = parseDouble(crota_kw, "CROTA" + posAxis2);
                if (crota != null) {
                    crota = 90 - crota;
                }
            }
            if (crota == null) {
                crota_kw = mapping.getKeywordValue("CROTA");
                crota = parseDouble(crota_kw, "CROTA");
            }

            String crotsign_kw = mapping.getKeywordValue("CROTSIGN");
            double crotsign = (crotsign_kw == null ? 1.0 : parseDouble(crotsign_kw,"CROTSIGN"));

            if (crota != null) {
                pc11 = Math.cos(crota) * crotsign;
                pc12 = -1.0 * Math.sin(crota) * crotsign;
                pc21 = Math.sin(crota) * crotsign;
                pc22 = Math.cos(crota) * crotsign;
            }
        }

        // If CDELT's are given apply using the PC matrix.
        if (cdelt1 != null && cdelt2 != null &&
            pc11 != null && pc12 != null && pc21 != null && pc22 != null) {
            cd11 = pc11 * cdelt1;
            cd12 = pc12 * cdelt1;
            cd21 = pc21 * cdelt2;
            cd22 = pc22 * cdelt2;

            return new CoordFunction2D(dimension, refCoord, cd11, cd12, cd21, cd22);
        }

        // No spatial wcs found.
        return null;
    }
    
    public static CoordPolygon2D getCoordPolygon2D(String utype, FitsMapping mapping)
    {
        List<ValueCoord2D> vertices = getVertices(utype + ".vertices", mapping);
        if (vertices == null)
            return null;
        CoordPolygon2D polygon = new CoordPolygon2D();
        for (ValueCoord2D vertex : vertices)
        {
            polygon.getVertices().add(vertex);
        }
        return polygon;
    }
    
    public static CoordRange1D getCoordRange1D(String utype, FitsMapping mapping)
    {
        RefCoord start = getRefCoord(utype + ".start", mapping);
        RefCoord end = getRefCoord(utype + ".end", mapping);
        if (start == null && end == null)
            return null;
        return new CoordRange1D(start, end);
    }
    
    public static CoordRange2D getCoordRange2D(String utype, FitsMapping mapping)
    {
        Coord2D start = getCoord2D(utype + ".start", mapping);
        Coord2D end = getCoord2D(utype + ".end", mapping);
        if (start == null && end == null)
            return null;
        return new CoordRange2D(start, end);
    }
    
    public static Dimension2D getDimension2D(String utype, FitsMapping mapping)
    {
        Long naxis1 = getLongValue(utype + ".naxis1", mapping);
        Long naxis2 = getLongValue(utype + ".naxis2", mapping);
        if (naxis1 == null && naxis2 == null)
            return null;
        if (naxis1 == null && naxis2 != null)
            throw new IllegalArgumentException("Value found for " + utype + 
                                               ".naxis2, but not for " + utype + ".naxis1 in "
                                               + mapping.uri + "[" + mapping.extension + "]");
        if (naxis1 != null && naxis2 == null)
            throw new IllegalArgumentException("Value found for " + utype + 
                                               ".naxis1, but not for " + utype + ".naxis2 in "
                                               + mapping.uri + "[" + mapping.extension + "]");
        return new Dimension2D(naxis1, naxis2);
    }
    
    public static ValueCoord2D getValueCoord2D(String utype, FitsMapping mapping)
    {
        Double c1 = getDoubleValue(utype + ".coord1", mapping);
        Double c2 = getDoubleValue(utype + ".coord2", mapping);
        if (c1 == null && c2 == null)
            return null;
        if (c1 == null && c2 != null)
            throw new IllegalArgumentException("Value found for " + utype + 
                                               ".coord2, but not for " + utype + ".coord1 in "
                                               + mapping.uri + "[" + mapping.extension + "]");
        if (c1 != null && c2 == null)
            throw new IllegalArgumentException("Value found for " + utype + 
                                               ".coord1, but not for " + utype + ".coord2 in "
                                               + mapping.uri + "[" + mapping.extension + "]");
        return new ValueCoord2D(c1, c2);
    }
    
    public static RefCoord getRefCoord(String utype, FitsMapping mapping)
    {
        Double pix = getDoubleValue(utype + ".pix", mapping);
        Double val = getDoubleValue(utype + ".val", mapping);
        if (pix == null && val == null)
            return null;
        if (pix == null && val != null)
            throw new IllegalArgumentException("Value found for " + utype + 
                                               ".val, but not for " + utype + ".pix in "
                                               + mapping.uri + "[" + mapping.extension + "]");
        if (pix != null && val == null)
            throw new IllegalArgumentException("Value found for " + utype + 
                                               ".pix, but not for " + utype + ".val in "
                                               + mapping.uri + "[" + mapping.extension + "]");
        return new RefCoord(pix, val);
    }
    
    public static Slice getSlice(String utype, FitsMapping mapping)
    {
        Axis axis = getAxis(utype + ".axis", mapping);
        Long bin = getLongValue(utype + ".bin", mapping);
        if (axis == null && bin == null)
            return null;
        if (bin == null)
            throw new IllegalArgumentException("Value not found for " + utype + ".bin in "
                    + mapping.uri + "[" + mapping.extension + "]");
        return new Slice(axis, bin);
    }
    
    protected static String getStringValue(String utype, FitsMapping mapping)
    { 
        return mapping.getMapping(utype);
    }
    
    protected static Double getDoubleValue(String utype, FitsMapping mapping)
    {
        String value = mapping.getMapping(utype);
        if (value == null)
            return null;
        try
        {
            return Double.valueOf(value);
        }
        catch (NumberFormatException e)
        {
            String error = "Unable to create Double from " + value + " for " + utype
                    + " in " + mapping.uri + "[" + mapping.extension + "]";
            throw new IllegalArgumentException(error);
        }
    }
    
    protected static Long getLongValue(String utype, FitsMapping mapping)
    {
        String value = mapping.getMapping(utype);
        if (value == null)
            return null;
        try
        {
            return Long.valueOf(value);
        }
        catch (NumberFormatException e)
        {
            String error = "Unable to create Long from " + value + " for " + utype
                    + " in " + mapping.uri + "[" + mapping.extension + "]";
            throw new IllegalArgumentException(error);
        }
    }
    
    protected static List<ValueCoord2D> getVertices(String utype, FitsMapping mapping)
    {
        String value = mapping.getMapping(utype);
        if (value == null)
            return null;
        throw new UnsupportedOperationException("not yet implemented");
    }
    
    protected static List<CoordRange1D> getSamples(String utype, FitsMapping mapping)
    {
        String value = mapping.getMapping(utype);
        if (value == null)
            return null;
        throw new UnsupportedOperationException("not yet implemented");
    }

    private static Double parseDouble(String value, String keyword) {
        if (value == null) {
            return null;
        }
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            String error = String
                .format("Unable to parse double for %s = %s", keyword, value);
            throw new IllegalArgumentException(error);
        }
    }
}

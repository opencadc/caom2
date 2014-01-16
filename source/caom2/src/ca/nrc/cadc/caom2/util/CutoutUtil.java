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

package ca.nrc.cadc.caom2.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.Chunk;
import ca.nrc.cadc.caom2.Part;
import ca.nrc.cadc.caom2.PolarizationState;
import ca.nrc.cadc.caom2.types.Circle;
import ca.nrc.cadc.caom2.types.EnergyUtil;
import ca.nrc.cadc.caom2.types.Interval;
import ca.nrc.cadc.caom2.types.PositionUtil;
import ca.nrc.cadc.caom2.types.Shape;
import ca.nrc.cadc.wcs.exceptions.NoSuchKeywordException;

public final class CutoutUtil
{
	private static final Logger log = Logger.getLogger(CutoutUtil.class);

    private static final String POS1_CUT = "px";
    private static final String POS2_CUT = "py";
    private static final String NRG_CUT = "ee";
    private static final String TIM_CUT = "tt";
    private static final String POL_CUT = "pp";
    private static final String OBS_CUT = "oo";
    private static final int CUT_LEN = 2;
    
    private CutoutUtil() { }

    // impl is for spatial cutout with a circle only
    public static List<String> computeCutout(Artifact a, Shape shape, Interval energyInter, Interval timeInter, List<PolarizationState> polarStates )
        throws NoSuchKeywordException
    {
        if (a == null)
            throw new IllegalArgumentException("null Artifact");
        Circle circle = null;
        if (shape != null)
        {
            if (shape instanceof Circle)
                circle = (Circle) shape;
            else
                throw new IllegalArgumentException("Only Circle is currently supported.");
        }
        
        if (log.isDebugEnabled()) // costly string conversions here
        {
            StringBuilder sb = new StringBuilder();
            sb.append("computeCutout: ").append(a.getURI());
            if (shape != null)
                sb.append(" vs ").append(shape);
            if (energyInter != null)
                sb.append(" vs ").append(energyInter);
            log.debug(sb.toString());
        }

         // for each chunk, we make a [part][chunk] cutout aka [extno][<pix range>,...]
        List<String> ret = new ArrayList<String>();
        for (Part p : a.getParts()) // currently, only FITS files have parts and chunks
        {
            for (Chunk c : p.getChunks())
            {
                boolean doCutObservable = false;
                boolean doCut = true;

                StringBuilder sb = new StringBuilder();
                sb.append("[").append(p.getName()).append("]");

                // create template cutout for each axis in the data array in the right order
                sb.append("[");
                for (int i=1; i <= c.naxis.intValue(); i++)
                {
                    if (i > 1)
                        sb.append(",");
                    if (c.positionAxis1 != null && i == c.positionAxis1.intValue())
                        sb.append(POS1_CUT);
                    else if (c.positionAxis2 != null && i == c.positionAxis2.intValue())
                        sb.append(POS2_CUT);
                    else if (c.energyAxis != null && i == c.energyAxis.intValue())
                        sb.append(NRG_CUT);
                    else if (c.timeAxis != null && i == c.timeAxis.intValue())
                        sb.append(TIM_CUT);
                    else if (c.polarizationAxis != null && i == c.polarizationAxis.intValue())
                        sb.append(POL_CUT);
                    else if (c.observableAxis != null && i == c.observableAxis.intValue())
                        sb.append(OBS_CUT);
                }
                sb.append("]");
                log.debug("cutout template: " + sb.toString());

                // check if spatial axes are part of the actual data array
                if ( doCut && circle != null )
                {
                    if ( canPositionCutout(c) )
                    {
                        long[] cut = PositionUtil.getBounds(c.position, circle);
                        if (cut != null)
                        {
                            // cut.length==0 means circle contains all pixels
                            // cut.length==4 means circle picks a subset of pixels

                            if (cut.length == 4)
                            {
                                String cutX = cut[0] + ":" + cut[1];
                                String cutY = cut[2] + ":" + cut[3];
                                log.debug("cutout: " + cut[0] + "," + cut[1] + "," + cut[2] + "," + cut[3] + " -> " + cutX + "," + cutY);
                                int i1 = sb.indexOf(POS1_CUT);
                                sb.replace(i1, i1+CUT_LEN, cutX);
                                int i2 = sb.indexOf(POS2_CUT);
                                sb.replace(i2, i2+CUT_LEN, cutY);
                                doCutObservable = true;
                            }
                            else
                            {
                                int i1 = sb.indexOf(POS1_CUT);
                                sb.replace(i1, i1+CUT_LEN, "*");
                                int i2 = sb.indexOf(POS2_CUT);
                                sb.replace(i2, i2+CUT_LEN, "*");
                            }
                            String cs = sb.toString();
                            log.debug("position cutout: " + a.getURI() + "," + p.getName() + ",Chunk: " + cs);
                        }
                        else
                        {
                            log.debug("cutout: " + a.getURI() + "," + p.getName() + ",Chunk: no position overlap");
                            doCut = false;
                        }
                    }
                    else
                        log.debug("cutout: " + a.getURI() + "," + p.getName() + ",Chunk: no SpatialWCS axes or function");
                }
                
                // energy cutout
                if ( doCut && energyInter != null )
                {
                    if ( canEnergyCutout(c) )
                    {
                        long[] cut = EnergyUtil.getBounds(c.energy, energyInter);
                        if (cut != null)
                        {
                            // cut.length==0 means circle contains all pixels
                            // cut.length==2 means interval picks a subset of pixels
                            int i = sb.indexOf(NRG_CUT);
                            if (cut.length == 2)
                            {
                                sb.replace(i, i+CUT_LEN, cut[0] + ":" + cut[1]);
                                doCutObservable = true;
                            }
                            else
                            {
                                sb.replace(i, i+CUT_LEN, "*");
                            }
                            String cs = sb.toString();
                            log.debug("energy cutout: " + a.getURI() + "," + p.getName() + ",Chunk: " + cs);
                        }
                        else
                        {
                            log.debug("cutout: " + a.getURI() + "," + p.getName() + ",Chunk: no energy overlap");
                            doCut = false;
                        }
                    }
                    else
                        log.debug("cutout: " + a.getURI() + "," + p.getName() + ",Chunk: no SpectralWCS axes or function");
                }

                // time cutout: not supported
                
                // polarization cutout: not supported
                
                // observable cutout
                if ( doCut && doCutObservable )
                {
                    log.debug("observable: " + c.observable);
                    if ( canObservableCutout(c) )
                    {
                        long o1 = c.observable.getDependent().getBin();
                        long o2 = c.observable.getDependent().getBin();
                        int i = sb.indexOf(OBS_CUT);
                        if (c.observable.independent != null)
                        {
                            if (o1 < c.observable.independent.getBin())
                                o2 = c.observable.independent.getBin();
                            else
                                o1 = c.observable.independent.getBin();
                        }
                        sb.replace(i, i+CUT_LEN, o1+":"+o2);
                        String cs = sb.toString();
                        log.debug("observable cutout: " + a.getURI() + "," + p.getName() + ",Chunk: " + cs);
                    }
                    else
                        log.debug("cutout: " + a.getURI() + "," + p.getName() + ",Chunk: no Observable axis");
                }


                // for any axis in the data but not in the cutout: keep all pixels
                int i;
                i = sb.indexOf(POS1_CUT);
                if (i > 0)
                    sb.replace(i, i+CUT_LEN, "*");

                i = sb.indexOf(POS2_CUT);
                if (i > 0)
                    sb.replace(i, i+CUT_LEN, "*");

                i = sb.indexOf(NRG_CUT);
                if (i > 0)
                    sb.replace(i, i+CUT_LEN, "*");

                i = sb.indexOf(TIM_CUT);
                if (i > 0)
                    sb.replace(i, i+CUT_LEN, "*");

                i = sb.indexOf(POL_CUT);
                if (i > 0)
                    sb.replace(i, i+CUT_LEN, "*");
                
                i = sb.indexOf(OBS_CUT);
                if (i > 0)
                    sb.replace(i, i+CUT_LEN, "*");

                if (doCut)
                    ret.add(sb.toString());
            }
        }
        return ret;
    }

    public static boolean canCutout(Chunk c)
    {
        
        boolean posCutout = canPositionCutout(c);
        boolean energyCutout = canEnergyCutout(c);
        boolean timeCutout = canTimeCutout(c);
        boolean polCutout = canPolarizationCutout(c);

        return posCutout || energyCutout || timeCutout || polCutout;
    }

    // check if spatial cutout is possible (currently function only)
    protected static boolean canPositionCutout(Chunk c)
    {
        boolean posCutout = (c.naxis != null && c.naxis.intValue() >= 2
                    && c.position != null && c.position.getAxis().function != null
                    && c.positionAxis1 != null && c.positionAxis1.intValue() <= c.naxis.intValue()
                    &&  c.positionAxis2 != null && c.positionAxis2.intValue() <= c.naxis.intValue() );
        return posCutout;
    }

    // check if energy cutout is possible (currently function only)
    protected static boolean canEnergyCutout(Chunk c)
    {
        boolean energyCutout = (c.naxis != null && c.naxis.intValue() >= 1
                    && c.energy != null
                    && c.energyAxis != null && c.energyAxis.intValue() <= c.naxis.intValue()
                    && ( 
                            c.energy.getAxis().bounds != null 
                            || 
                            c.energy.getAxis().function != null
                        )
                );
        return energyCutout;
    }

    // check if time cutout is possible (currently function only)
    protected static boolean canTimeCutout(Chunk c)
    {
        boolean timeCutout = false;
        //(c.naxis != null && c.naxis.intValue() >= 1
        //            && c.time != null && c.time.getAxis().function != null
        //            && c.timeAxis != null && c.timeAxis.intValue() <= c.naxis.intValue());
        return timeCutout;
    }

    // check if polarization cutout is possible (currently function only)
    protected static boolean canPolarizationCutout(Chunk c)
    {
        boolean polarizationCutout = false;
        //(c.naxis != null && c.naxis.intValue() >= 1
        //            && c.polarization != null && c.polarization.getAxis().function != null
        //            && c.polarizationAxis != null && c.polarizationAxis.intValue() <= c.naxis.intValue());
        return polarizationCutout;
    }
    
    // check if polarization cutout is possible (currently function only)
    protected static boolean canObservableCutout(Chunk c)
    {
        boolean observableCutout = (c.naxis != null && c.naxis.intValue() >= 1
                    && c.observable != null
                    && c.observableAxis != null && c.observableAxis.intValue() <= c.naxis.intValue());
        return observableCutout;
    }
}

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
import ca.nrc.cadc.caom2.types.Polygon;
import ca.nrc.cadc.caom2.types.PositionUtil;
import ca.nrc.cadc.caom2.types.Shape;
import ca.nrc.cadc.caom2.wcs.ObservableAxis;
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
            boolean doCutObservable = false;
            boolean doCut = true;
            long[] posCut = null;
            long[] nrgCut = null;
            long[] timCut = null;
            long[] polCut = null;
            long[] obsCut = null;
            //for (Chunk c : p.getChunks())
            if (p.chunk != null)
            {   
                Chunk c = p.chunk;
                // check if spatial axes are part of the actual data array
                if (shape != null )
                {
                    if ( canPositionCutout(c) )
                    {
                        // cut.length==0 means circle contains all pixels
                        // cut.length==4 means circle picks a subset of pixels
                        long[] cut = PositionUtil.getBounds(c.position, shape);
                        if (posCut == null)
                            posCut = cut;
                        else if (posCut.length == 4 && cut != null) // subset
                        {
                            if (cut.length == 0)
                                posCut = cut;
                            else // both are length 4
                            {
                                posCut[0] = Math.min(posCut[0], cut[0]);
                                posCut[1] = Math.max(posCut[1], cut[1]);
                                posCut[2] = Math.min(posCut[2], cut[2]);
                                posCut[3] = Math.max(posCut[3], cut[3]);
                            }
                        }
                    }
                }
                // energy cutout
                if (energyInter != null )
                {
                    if ( canEnergyCutout(c) )
                    {
                        long[] cut = EnergyUtil.getBounds(c.energy, energyInter);
                        if (nrgCut == null)
                            nrgCut = cut;
                        else if (nrgCut.length == 2 && cut != null) // subset
                        {
                            if (cut.length == 0)
                                nrgCut = cut;
                            else // both are length 4
                            {
                                nrgCut[0] = Math.min(nrgCut[0], cut[0]);
                                nrgCut[1] = Math.max(nrgCut[1], cut[1]);
                            }
                        }
                    }
                }
                // no time cutout
                // no polarization cutout
                // no input observable cutout, but merge
                if ( canObservableCutout(c) )
                {
                    long[] cut = getObservableCutout(c.observable);
                    log.debug("checking chunk " + c.getID() + " obs cut: " + toString(cut));
                    if (obsCut == null)
                    {
                        log.debug("observable cut: " + toString(obsCut) + " -> " + toString(cut));
                        obsCut = cut;
                    }
                    else if (obsCut.length == 2 && cut != null) 
                    {
                        if (cut.length == 0)
                        {
                            log.debug("observable cut: " + toString(obsCut) + " -> " + toString(cut));
                            obsCut = cut;
                        }
                        else // both are length 2
                        {
                            log.debug("observable cut merge before: " + toString(obsCut));
                            obsCut[0] = Math.min(obsCut[0], cut[0]);
                            obsCut[1] = Math.max(obsCut[1], cut[1]);
                            log.debug("observable cut merge after: " + toString(obsCut));
                        }
                    }
                }
            }
            
            // now inject the pixel ranges into the cutout spec
            StringBuilder sb = initCutout(p.getName(), p);
            if (posCut != null)
            {
                // cut.length==0 means circle contains all pixels
                // cut.length==4 means circle picks a subset of pixels
                if (posCut.length == 4)
                {
                    String cutX = posCut[0] + ":" + posCut[1];
                    String cutY = posCut[2] + ":" + posCut[3];
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
            else if (shape != null)
            {
                log.debug("cutout: " + a.getURI() + "," + p.getName() + ",Chunk: no position overlap");
                doCut = false;
            }
                    
            if (nrgCut != null)
            {
                // cut.length==0 means circle contains all pixels
                // cut.length==2 means interval picks a subset of pixels
                int i = sb.indexOf(NRG_CUT);
                if (nrgCut.length == 2)
                {
                    sb.replace(i, i+CUT_LEN, nrgCut[0] + ":" + nrgCut[1]);
                    doCutObservable = true;// cut.length==0 means circle contains all pixels
                }
                else
                {
                    sb.replace(i, i+CUT_LEN, "*");
                }
                String cs = sb.toString();
                log.debug("energy cutout: " + a.getURI() + "," + p.getName() + ",Chunk: " + cs);
            }
            else if (energyInter != null)
            {
                log.debug("cutout: " + a.getURI() + "," + p.getName() + ",Chunk: no energy overlap");
                doCut = false;
            }

            // time cutout: not supported
                
            // polarization cutout: not supported
                
            if (obsCut != null)
            {
                int i = sb.indexOf(OBS_CUT);
                sb.replace(i, i+CUT_LEN, obsCut[0]+":"+obsCut[1]);
                String cs = sb.toString();
                log.debug("observable cutout: " + a.getURI() + "," + p.getName() + ",Chunk: " + cs);
            }
            else
                log.debug("cutout: " + a.getURI() + "," + p.getName() + ",Chunk: no Observable axis");

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
        return ret;
    }
    
    private static String toString(long[] cut)
    {
        if (cut == null)
            return null;
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (long v : cut)
        {
            sb.append(v);
            sb.append(",");
        }
        sb.setCharAt(sb.length() - 1, ']');
        return sb.toString();
    }
    private static long[] getObservableCutout(ObservableAxis o)
    {
        
        long o1 = o.getDependent().getBin();
        long o2 = o.getDependent().getBin();

        if (o.independent != null)
        {
            if (o1 < o.independent.getBin())
                o2 = o.independent.getBin();
            else
                o1 = o.independent.getBin();
        }
        return new long[] { o1, o2 };
    }
    
    private static StringBuilder initCutout(String partName, Part p)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("[").append(partName).append("]");
        // create template cutout for each axis in the data array in the right order
        sb.append("[");
        boolean pos1 = false;
        boolean pos2 =  false;
        boolean nrg = false;
        boolean tim = false;
        boolean pol = false;
        boolean obs = false;
        int naxis = 0;
        //for (Chunk c : p.getChunks())
        if (p.chunk != null)
        {
            Chunk c = p.chunk;
            naxis = Math.max(naxis, c.naxis);
            for (int i=1; i <= c.naxis.intValue(); i++)
            {
                pos1 = pos1 || (c.positionAxis1 != null && i == c.positionAxis1.intValue());
                pos2 = pos2 || (c.positionAxis2 != null && i == c.positionAxis2.intValue());
                nrg = nrg || (c.energyAxis != null && i == c.energyAxis.intValue());
                tim = tim || (c.timeAxis != null && i == c.timeAxis.intValue());
                pol = pol || (c.polarizationAxis != null && i == c.polarizationAxis.intValue());
                obs = obs || (c.observableAxis != null && i == c.observableAxis.intValue());
            }
        }
        if (pos1)
            sb.append(POS1_CUT).append(",");
        if (pos2)
            sb.append(POS2_CUT).append(",");
        if (nrg)
            sb.append(NRG_CUT).append(",");
        if (tim)
            sb.append(TIM_CUT).append(",");
        if (pol)
            sb.append(POL_CUT).append(",");
        if (obs)
            sb.append(OBS_CUT).append(",");
        sb.setCharAt(sb.length() - 1, ']'); // last comma to ]
        log.debug("cutout template: " + sb.toString());
        return sb;
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

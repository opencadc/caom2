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

package ca.nrc.cadc.caom2.types;

import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.Chunk;
import ca.nrc.cadc.caom2.Part;
import ca.nrc.cadc.caom2.Polarization;
import ca.nrc.cadc.caom2.PolarizationState;
import ca.nrc.cadc.caom2.ProductType;
import ca.nrc.cadc.caom2.wcs.CoordBounds1D;
import ca.nrc.cadc.caom2.wcs.CoordFunction1D;
import ca.nrc.cadc.caom2.wcs.CoordRange1D;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Set;
import org.apache.log4j.Logger;

/**
 *
 * @author pdowler
 */
public final class PolarizationUtil
{
    private static final Logger log = Logger.getLogger(PolarizationUtil.class);
    
    private PolarizationUtil() { }

    public static Polarization compute(Set<Artifact> artifacts)
    {
        Set<PolarizationState> pol = EnumSet.noneOf(PolarizationState.class);
        ProductType productType = Util.choseProductType(artifacts);
        log.debug("compute: " + productType);
        int numPixels = 0;
        for (Artifact a : artifacts)
        {
            for (Part p : a.getParts())
            {
                for (Chunk c : p.getChunks())
                {
                    if (Util.useChunk(a.getProductType(), p.productType, c.productType, productType))
                    {
                        if (c.polarization != null)
                        {
                            numPixels += Util.getNumPixels(c.polarization.getAxis());
                            CoordRange1D range = c.polarization.getAxis().range;
                            CoordBounds1D bounds = c.polarization.getAxis().bounds;
                            CoordFunction1D function = c.polarization.getAxis().function;
                            if (range != null)
                            {
                                int lb = (int) range.getStart().val;
                                int ub = (int) range.getEnd().val;
                                for (int i=lb; i <= ub; i++)
                                {
                                    pol.add(PolarizationState.toValue(i));
                                }
                            }
                            else if (bounds != null)
                            {
                                for (CoordRange1D cr : bounds.getSamples())
                                {
                                    int lb = (int) cr.getStart().val;
                                    int ub = (int) cr.getEnd().val;
                                    for (int i=lb; i <= ub; i++)
                                    {
                                        pol.add(PolarizationState.toValue(i));
                                    }
                                }
                            }
                            else if (function != null)
                            {
                                for (int i=1; i <= function.getNaxis(); i++)
                                {
                                    double pix = (double) i;
                                    int val = (int) Util.pix2val(function, pix);
                                    pol.add(PolarizationState.toValue(val));
                                }
                            }
                        }
                    }
                }
            }
        }

        Polarization p = new Polarization();
        if ( !pol.isEmpty() )
        {
            p.states = new ArrayList<PolarizationState>();
            p.states.addAll(pol);
            p.dimension = new Integer(numPixels);
        }
        return p;
    }
}

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

package ca.nrc.cadc.caom2.repo;

import ca.nrc.cadc.caom2.util.EnergyConverter;
import ca.nrc.cadc.vosi.avail.CheckException;
import ca.nrc.cadc.vosi.avail.CheckResource;
import ca.nrc.cadc.wcs.Transform;
import ca.nrc.cadc.wcs.WCSKeywords;
import ca.nrc.cadc.wcs.WCSKeywordsImpl;
import org.apache.log4j.Logger;

/**
 *
 * @author pdowler
 */
public class CheckWcsLib implements CheckResource
{
    private static Logger log = Logger.getLogger(CheckWcsLib.class);

    private static final double FREQ = 600.0;
    private static final String CUNIT = "kHz";
    
    public CheckWcsLib() { }
    
    public void check() 
        throws CheckException
    {
        try
        {
            WCSKeywordsImpl wcs = new WCSKeywordsImpl();
            wcs.put("SPECSYS", "TOPOCENT");
            wcs.put("NAXIS", 1);
            wcs.put("CTYPE1", "FREQ");
            wcs.put("CUNIT1", CUNIT);
            wcs.put("NAXIS1", 1000);
            wcs.put("CRPIX1", 0.5);
            wcs.put("CRVAL1", 200.0);
            wcs.put("CDELT1", 1.0); // 200-1200
            Transform trans = new Transform(wcs);
            trans.sky2pix( new double[] { FREQ });
            trans.pix2sky( new double[] { 200.0 });
            
            WCSKeywords kw = trans.translate("WAVE-???");
            Transform trans2 = new Transform(kw);
            
            EnergyConverter ec = new EnergyConverter();
            double wav = ec.toMeters(FREQ, CUNIT);
            trans2.sky2pix( new double[] { wav });
            trans2.pix2sky( new double[] { 200.0 });
        }
        catch(Throwable t)
        {
            throw new CheckException("wcslib not available", t);
        }
    }
    
    
}

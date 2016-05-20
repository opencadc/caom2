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

package ca.nrc.cadc.caom2.util;

import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.caom2.types.Polygon;
import ca.nrc.cadc.caom2.types.PolygonUtil;
import java.util.Set;

/**
 *
 * @author pdowler
 */
public final class CaomValidator
{
    private CaomValidator() { }
    
    public static void assertNotNull(Class caller, String name, Object test)
        throws IllegalArgumentException
    {
        if (test == null)
            throw new IllegalArgumentException(caller.getSimpleName() + ": null " + name);
    }

    public static void assertValidKeyword(Class caller, String name, String val)
    {
        assertNotNull(caller, name, val);
        boolean space = (val.indexOf(' ') >= 0);
        boolean tick = (val.indexOf('\'') >= 0);
        if (!tick && !space)
            return;
        throw new IllegalArgumentException(caller.getSimpleName() + ": invalid " + name
                + ": may not contain single tick (') or space ( )");
    }
    /**
     * A valid path component has no space ( ), slash (/), escape (\), or percent (%) characters.
     * 
     * @param caller
     * @param name
     * @param test
     */
    public static void assertValidPathComponent(Class caller, String name, String test)
    {
        assertNotNull(caller, name, test);
        boolean space = (test.indexOf(' ') >= 0);
        boolean slash = (test.indexOf('/') >= 0);
        boolean escape = (test.indexOf('\\') >= 0);
        boolean percent = (test.indexOf('%') >= 0);

        if (!space && !slash && !escape && !percent)
            return;
        throw new IllegalArgumentException(caller.getSimpleName() + ": invalid " + name
                + ": may not contain space ( ), slash (/), escape (\\), or percent (%)");
    }

    public static void assertPositive(Class caller, String name, double test)
    {
        if (test <= 0.0)
            throw new IllegalArgumentException(caller.getSimpleName() + ": " + name + " must be > 0.0");
    }

    private static void validateKeywords(String name, Set<String> vals)
    {
        for (String s : vals)
        {
            assertValidKeyword(CaomValidator.class, name, s);
        }
    }
    public static void validateKeywords(Observation obs)
    {
        if (obs.proposal != null)
            validateKeywords("proposal.keywords", obs.proposal.getKeywords());
        if (obs.target != null)
            validateKeywords("target.keywords", obs.target.getKeywords());
        if (obs.telescope != null)
            validateKeywords("telescope.keywords", obs.telescope.getKeywords());
        if (obs.instrument != null)
            validateKeywords("instrument.keywords", obs.instrument.getKeywords());
        
        for (Plane p : obs.getPlanes())
        {
            if (p.provenance != null)
                validateKeywords("provenance.keywords", p.provenance.getKeywords());            
        }
    }
    public static void validate(Observation obs)
    {
        validateKeywords(obs);
        
        for (Plane p : obs.getPlanes())
        {
            try
            {
                p.clearTransientState();
                p.computeTransientState();
                if (p.position != null && p.position.bounds != null)
                {
                    Polygon poly = PolygonUtil.toPolygon(p.position.bounds);
                    PolygonUtil.getOuterHull(poly);
                }
            }
            catch(Error er)
            {
                throw new RuntimeException("failed to compute metadata for plane " + p.getProductID(), er);
            }
            catch(Exception ex)
            {
                throw new IllegalArgumentException("failed to compute metadata for plane " + p.getProductID(), ex);
            }
        }
    }
}

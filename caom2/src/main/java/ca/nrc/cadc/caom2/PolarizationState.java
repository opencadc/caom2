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

package ca.nrc.cadc.caom2;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Standard polarization codes for FITS WCS STOKES axis. We have added some additional codes
 * for values outside the original FITS WCS paper from the discussion at
 * </p>
 * <p>
 * http://listmgr.cv.nrao.edu/pipermail/fitswcs/2008-March/000408.html
 * </p>
 * 
 * @author pdowler
 */
public enum PolarizationState implements CaomEnum
{
    I(1),
    Q(2),
    U(3),
    V(4),
    POLI(5),   // linear polarized intensity sqrt(Q^2 + U^2), code used in AIPS
    FPOLI(6),  // fractional linear polarization POLI/I, code used in AIPS
    POLA(7),   // linear polarization angle 1/2 arctan(U,Q), code used in AIPS
    EPOLI(8),  // elliptical polarization intensity sqrt(Q^2 + U^2 + V^2)
    CPOLI(9),  // circular polarization intensity |V|
    NPOLI(10), // unpolarized intensity I - EPOLI
    RR(-1),
    LL(-2),
    RL(-3),
    LR(-4),
    XX(-5),
    YY(-6),
    XY(-7),
    YX(-8);

    private int value;

    private PolarizationState(int value) { this.value = value; }

    public int getValue() { return value; }
    
    public String stringValue()
    {
        switch(value)
        {
            case 1: return "I";
            case 2: return "Q";
            case 3: return "U";
            case 4: return "V";
            case 5: return "POLI";
            case 6: return "FPOLI"; 
            case 7: return "POLA";  
            case 8: return "EPOLI"; 
            case 9: return "CPOLI"; 
            case 10: return "NPOLI"; 
            case -1: return "RR";
            case -2: return "LL";
            case -3: return "RL";
            case -4: return "LR";
            case -5: return "XX";
            case -6: return "YY";
            case -7: return "XY";
            case -8: return "YX";
        }
        throw new IllegalStateException("BUG: unexpected polarization code: " + value);
    }

    @Override
    public String toString()
    {
        return this.getClass().getSimpleName() + "[" + stringValue() + "]";
    }

    public static PolarizationState toValue(int val)
    {
        switch(val)
        {
            case 1: return I;
            case 2: return Q;
            case 3: return U;
            case 4: return V;
            case 5: return POLI; 
            case 6: return FPOLI;
            case 7: return POLA; 
            case 8: return EPOLI; 
            case 9: return CPOLI;
            case 10: return NPOLI;
            case -1: return RR;
            case -2: return LL;
            case -3: return RL;
            case -4: return LR;
            case -5: return XX;
            case -6: return YY;
            case -7: return XY;
            case -8: return YX;
        }
        throw new IllegalArgumentException("invalid polarization code: " + val);
    }
    
     public static PolarizationState toValue(String s)
    {
        for (PolarizationState ps : values())
        {
            if ( ps.stringValue().equals(s))
                return ps;
        }
        throw new IllegalArgumentException("invalid value: " + s);
    }

    public int checksum()
    {
        return value;
    }
    
    public static class PolStateComparator implements Comparator<PolarizationState>, Serializable
    {
        private static final long serialVersionUID = 201401131450L;
        
        public int compare(PolarizationState lhs, PolarizationState rhs)
        {
            // Java 1.7:
            //return Integer.compare(lhs.value, rhs.value);
            if (lhs.value < rhs.value)
                return -1;
            if (lhs.value > rhs.value)
                return 1;
            return 0;
        }
        
    }
}

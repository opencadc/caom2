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

package ca.nrc.cadc.caom2.access;

import ca.nrc.cadc.caom2.AbstractCaomEntity;
import ca.nrc.cadc.caom2.util.CaomValidator;
import ca.nrc.cadc.util.StringUtil;
import java.net.URI;

/**
 *
 * @author pdowler
 */
public class ReadAccess extends AbstractCaomEntity implements Comparable<ReadAccess>
{
    private static final long serialVersionUID = 201202081620L;
    
    private Long assetID;

    private URI groupID;
    
    private ReadAccess() { }

    public ReadAccess(Long assetID, URI groupID)
    {
        super(true);
        CaomValidator.assertNotNull(this.getClass(), "assetID", assetID);
        CaomValidator.assertNotNull(this.getClass(), "groupID", groupID);
        this.assetID = assetID;
        this.groupID = groupID;
        String name = getGroupName();
        if (name == null)
            throw new IllegalArgumentException("invalid groupID (no group name found in query string or fragment): " + groupID);
    }

    public Long getAssetID()
    {
        return assetID;
    }

    public URI getGroupID()
    {
        return groupID;
    }
    
    public final String getGroupName()
    {
        // canonical form: ivo://<authority>/<path>?<name>
        
        String ret = groupID.getQuery();
        if (StringUtil.hasText(ret))
            return ret;
        
        // backwards compat
        ret = groupID.getFragment();
        if (StringUtil.hasText(ret))
            return ret;
        
        // temporary backwards compat for caom2ac usage hack
        return groupID.toASCIIString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null)
            return false;
        if (this == o)
            return true;
        if ( !this.getClass().equals(o.getClass()) ) // only exact class match
            return false;

        ReadAccess ra = (ReadAccess) o;
        return this.groupID.equals(ra.groupID)
                && this.assetID.equals(ra.assetID);
    }

    public int compareTo(ReadAccess o)
    {
        // groupID,assetID,classname==permission type
        int ret = this.groupID.compareTo(o.groupID);
        if (ret == 0)
            ret = this.assetID.compareTo(o.assetID);
        if (ret == 0)
            ret = this.getClass().getName().compareTo(o.getClass().getName());
        return ret;
    }

    @Override
    public String toString()
    {
        return this.getClass().getSimpleName()
                + "[" + assetID + "," + groupID + "]";
    }
}

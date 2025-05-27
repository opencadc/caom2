/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2019.                            (c) 2019.
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

package org.opencadc.argus.tap;

import ca.nrc.cadc.dali.util.DefaultFormat;
import ca.nrc.cadc.dali.util.Format;
import ca.nrc.cadc.tap.TapSelectItem;
import ca.nrc.cadc.tap.writer.format.PostgreSQLFormatFactory;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;
import org.opencadc.argus.tap.format.DataLinkURLFormat;
import org.opencadc.argus.tap.format.FlexIntervalFormat;
import org.opencadc.argus.tap.format.FlexPointFormat;
import org.opencadc.argus.tap.format.PositionBoundsShapeFormat;
import org.opencadc.argus.tap.format.URISetFormat;

/**
 *
 *
 */
public class CaomFormatFactory extends PostgreSQLFormatFactory {
    private static Logger log = Logger.getLogger(CaomFormatFactory.class);

    private static final List<String> URI_SET_UTYPES = new ArrayList<>();
    
    static {
        URI_SET_UTYPES.add("caom2:Observation.metaReadGroups");
        URI_SET_UTYPES.add("caom2:Observation.metaReadGroups");
        URI_SET_UTYPES.add("caom2:Observation.members");
        URI_SET_UTYPES.add("caom2:Plane.dataReadGroups");
        URI_SET_UTYPES.add("caom2:Plane.metaReadGroups");
        URI_SET_UTYPES.add("caom2:Plane.provenance.inputs");
        URI_SET_UTYPES.add("caom2:Artifact.contentReadGroups");
    }
    
    public CaomFormatFactory() {
        super();
    }

    @Override
    public Format<Object> getFormat(TapSelectItem tsi) {
        Format<Object> ret = super.getFormat(tsi);
        if ("ivoa.ObsCore".equalsIgnoreCase(tsi.tableName)
                && "access_url".equalsIgnoreCase(tsi.getColumnName())) {
            ret = new DataLinkURLFormat("ivoaPublisherID");
        }
        log.debug("fomatter: " + tsi + " " + ret.getClass().getName());
        return ret;
    }

    @Override
    protected Format<Object> getStringFormat(TapSelectItem columnDesc) {
        if (columnDesc.utype != null && URI_SET_UTYPES.contains(columnDesc.utype)) {
            return new URISetFormat(" ");
        }
        return super.getStringFormat(columnDesc);
    }

    
    @Override
    protected Format<Object> getPointFormat(TapSelectItem columnDesc) {
        return new FlexPointFormat();
    }
    
    @Override
    protected Format<Object> getShapeFormat(TapSelectItem columnDesc) {
        // actual output format controlled by the tap_schema: utype and xtype
        if (columnDesc.utype != null) {
            if (columnDesc.utype.equals("caom2:Plane.position.bounds")
                    || columnDesc.utype.equals("caom2:Plane.position.minBounds")
                    || columnDesc.utype.equals("obscore:Char.SpatialAxis.Coverage.Support.Area")) {
                return new PositionBoundsShapeFormat();
            } else if (columnDesc.utype.equals("caom2:Plane.position.samples")) {
                return new DefaultFormat(); // currently text in db
            }
        }
        // default to pgsphere formatters
        return super.getShapeFormat(columnDesc);
    }

    @Override
    public Format<Object> getRegionFormat(TapSelectItem columnDesc) {
        // actual output format controlled by the tap_schema: utype and xtype
        if (columnDesc.utype != null
                && (columnDesc.utype.equals("caom2:Plane.position.bounds")
                    || columnDesc.utype.equals("obscore:Char.SpatialAxis.Coverage.Support.Area"))) {
            return new PositionBoundsShapeFormat();
        }
        return super.getRegionFormat(columnDesc);
    }

    @Override
    public Format<Object> getIntervalFormat(TapSelectItem columnDesc) {
        return new FlexIntervalFormat(columnDesc.getDatatype().isVarSize());
    }

    @Override
    protected Format<Object> getMultiIntervalFormat(TapSelectItem columnDesc) {
        return new FlexIntervalFormat(columnDesc.getDatatype().isVarSize());
    }

    
}

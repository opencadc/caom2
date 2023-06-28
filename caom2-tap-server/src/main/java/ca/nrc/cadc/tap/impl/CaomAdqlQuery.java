/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2023.                            (c) 2023.
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

package ca.nrc.cadc.tap.impl;

import ca.nrc.cadc.tap.AdqlQuery;
import ca.nrc.cadc.tap.caom2.CaomReadAccessConverter;
import ca.nrc.cadc.tap.caom2.CaomRegionConverter;
import ca.nrc.cadc.tap.caom2.CaomSelectListConverter;
import ca.nrc.cadc.tap.caom2.IsDownloadableConverter;
import ca.nrc.cadc.tap.parser.BaseExpressionDeParser;
import ca.nrc.cadc.tap.parser.PgsphereDeParser;
import ca.nrc.cadc.tap.parser.converter.ColumnNameConverter;
import ca.nrc.cadc.tap.parser.converter.TableNameConverter;
import ca.nrc.cadc.tap.parser.converter.TableNameReferenceConverter;
import ca.nrc.cadc.tap.parser.converter.TopConverter;
import ca.nrc.cadc.tap.parser.converter.postgresql.PgFunctionNameConverter;
import ca.nrc.cadc.tap.parser.extractor.FunctionExpressionExtractor;
import ca.nrc.cadc.tap.parser.navigator.ExpressionNavigator;
import ca.nrc.cadc.tap.parser.navigator.FromItemNavigator;
import ca.nrc.cadc.tap.parser.navigator.ReferenceNavigator;
import ca.nrc.cadc.tap.parser.navigator.SelectNavigator;
import net.sf.jsqlparser.util.deparser.SelectDeParser;
import org.apache.log4j.Logger;

/**
 * AdqlQuery implementation for PostgreSQL + pg_sphere + CAOM-2.
 * 
 * @author pdowler
 */
public class CaomAdqlQuery extends AdqlQuery
{
    private static Logger log = Logger.getLogger(CaomAdqlQuery.class);
    
    private final boolean enableMetaReadAccessConverter;
    
    /**
     * Standard ADQL processor for a CAOM-2.x TAP service. By default, the 
     * CaomReadAccessConverter is enabled. Sub-classes may disable it via
     * an alternate constructor.
     */
    public CaomAdqlQuery() { 
        this(true);
    }
    
    protected CaomAdqlQuery(boolean enableMetaReadAccessConverter) {
        this.enableMetaReadAccessConverter = enableMetaReadAccessConverter;
    }
    
    @Override
    protected void init()
    {
        super.init();

        // convert TOP -> LIMIT
        super.navigatorList.add(new TopConverter(
                new ExpressionNavigator(), new ReferenceNavigator(), new FromItemNavigator()));

        // convert ADQL geometry function calls to alternate form
        super.navigatorList.add(new CaomRegionConverter(tapSchema));
        
        // convert functions to PG-specific names
        navigatorList.add(new FunctionExpressionExtractor(
            new PgFunctionNameConverter(), new ReferenceNavigator(), new FromItemNavigator()));

        // after CaomRegionConverter: triggering off the same column names and converting some uses
        TableNameConverter tnc = new TableNameConverter(true);
        tnc.put("ivoa.ObsCore", "caom2.ObsCore");
        // TAP-1.1 version of tap_schema
        tnc.put("tap_schema.schemas", "tap_schema.schemas11");
        tnc.put("tap_schema.tables", "tap_schema.tables11");
        tnc.put("tap_schema.columns", "tap_schema.columns11");
        tnc.put("tap_schema.keys", "tap_schema.keys11");
        tnc.put("tap_schema.key_columns", "tap_schema.key_columns11");
        TableNameReferenceConverter tnrc = new TableNameReferenceConverter(tnc.map);
        super.navigatorList.add(new SelectNavigator(new ExpressionNavigator(), tnrc, tnc));

        // temporary backwards compatibility hack for CAOM-2.4 column name change
        ColumnNameConverter cnc = new ColumnNameConverter(true, tapSchema);
        ColumnNameConverter.QualifiedColumn emBand = new ColumnNameConverter.QualifiedColumn("caom2.Plane", "energy_emBand");
        ColumnNameConverter.QualifiedColumn energyBands = new ColumnNameConverter.QualifiedColumn("caom2.Plane", "energy_energyBands");
        cnc.put(emBand, energyBands);
        emBand = new ColumnNameConverter.QualifiedColumn("caom2.EnumField", "energy_emBand");
        energyBands = new ColumnNameConverter.QualifiedColumn("caom2.EnumField", "energy_energyBands");
        cnc.put(emBand, energyBands);
        super.navigatorList.add(new SelectNavigator(new ExpressionNavigator(), cnc, new FromItemNavigator()));
        
        if (enableMetaReadAccessConverter) {
            // enforce access control policy in queries - must be after TableNameConverter
            super.navigatorList.add(new CaomReadAccessConverter());
        }

        // convert use of the isDownloadable function
        super.navigatorList.add(new IsDownloadableConverter());
        
        // change caom2.Artifact.accessURL to caom2.Artifact.uri
        super.navigatorList.add(new CaomSelectListConverter(tapSchema));
        
        //for (Object o : navigatorList)
        //    log.debug("navigator: " + o.getClass().getName());
    }
    
    @Override
    protected BaseExpressionDeParser getExpressionDeparser(SelectDeParser dep, StringBuffer sb)
    {
        return new PgsphereDeParser(dep, sb);
    }
    
    @Override
    public String getSQL()
    {
        String sql = super.getSQL();
        log.debug("SQL:\n" + sql); 
        return sql;
    }
}

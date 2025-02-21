/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2009.                            (c) 2009.
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

package org.opencadc.argus.tap.caom2;

import org.opencadc.argus.tap.query.CaomRegionConverter;
import ca.nrc.cadc.tap.TapQuery;
import ca.nrc.cadc.tap.schema.TapSchema;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.uws.Parameter;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.argus.tap.CaomAdqlQuery;

/**
 * test predicate function converter in CAOM context
 * 
 * @author Sailor Zhang
 *
 */
public class AreaFunctionTest
{
    private static Logger log = Logger.getLogger(AreaFunctionTest.class);

    public String _query;
    public String _expected = "";
    public String _sql;

    private static TapSchema caomTapSchema = TestUtil.loadTapSchema();
    
    static {
        Log4jInit.setLevel("ca.nrc.cadc.tap.caom", org.apache.log4j.Level.INFO);
    }

    private class TestQuery extends CaomAdqlQuery
    {
        protected void init()
        {
            //super.init();
            super.navigatorList.add(new CaomRegionConverter(caomTapSchema));
        }
    }

    private void run()
    {
        try
        {
            Parameter para;
            para = new Parameter("QUERY", _query);
            List<Parameter> paramList = new ArrayList<Parameter>();
            paramList.add(para);

            TapQuery tapQuery = new TestQuery();
            TestUtil.job.getParameterList().addAll(paramList);
            tapQuery.setJob(TestUtil.job);
            _sql = tapQuery.getSQL();
            log.info("    input: " + _query);
            log.info("   result: " + _sql);
        }
        finally
        {
            TestUtil.job.getParameterList().clear();
        }
    }

    private String prepareToCompare(String str)
    {
        String ret = str.trim();
        ret = ret.replaceAll("\\s+", " ");
        return ret.toLowerCase();
    }

    private void assertContain()
    {
        Assert.assertTrue(_sql.toLowerCase().indexOf(_expected.toLowerCase()) > 0);
    }

    @Test
    public void testWithTableName()
    {
        _expected = "SELECT TOP 1 Plane.position_bounds_area FROM caom2.Plane WHERE position_bounds IS NOT NULL";
        _expected = prepareToCompare(_expected);
        _query = "select top 1 area(Plane.position_bounds) from caom2.Plane where position_bounds is not null";
        run();
        log.info(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));
    }

    @Test
    public void testWithAlias()
    {
        _expected = "SELECT TOP 1 p.position_bounds_area FROM caom2.Plane AS p WHERE p.position_bounds IS NOT NULL";
        _expected = prepareToCompare(_expected);
        _query = "select top 1 area(p.position_bounds) from caom2.Plane p where p.position_bounds is not null";
        run();
        log.info(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));
    }

    @Test
    public void testColumnNameOnly()
    {
        _expected = "SELECT TOP 1 position_bounds_area FROM caom2.Plane AS p WHERE p.position_bounds IS NOT NULL";
        _expected = prepareToCompare(_expected);
        _query = "select top 1 area(position_bounds) from caom2.Plane p where p.position_bounds is not null";
        run();
        log.info(" expected: " + _expected);
        Assert.assertEquals(_expected, prepareToCompare(_sql));
    }

}

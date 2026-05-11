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
*  $Revision: 4 $
*
************************************************************************
*/

package ca.nrc.cadc.sia;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;

/**
 * Generate TAP query parameters for an SIA request.
 * 
 * @author jburke
 */
public class AdqlQueryGenerator
{
    private static Logger log = Logger.getLogger(AdqlQueryGenerator.class);
    
    private static final String TABLE = "caom2.SIAv1";
    private static final String COLUMNS = "collection,publisherDID,instrument_name,"
            + "position_center_ra,position_center_dec,position_naxes,position_naxis,position_scale,position_bounds,"
            + "energy_bounds_center,energy_bounds_cval1,energy_bounds_cval2,energy_units,energy_bandpassName,"
            + "time_bounds_center,time_bounds_cval1,time_bounds_cval2,time_exposure,"
            + "imageFormat,accessURL,metaRelease,dataRelease";
    private static final String INTERSECTS_OPEN_CIRCLE = "INTERSECTS(position_bounds, CIRCLE('ICRS',";
    private static final String INTERSECTS_CLOSE = ")) = 1";
    private static final String INTERSECTS_OPEN_POINT = "CONTAINS(POINT('ICRS',";
    private static final String INTERSECTS_CLOSE_POINT = "), position_bounds) = 1";

    public static final String SIA_CONTENT_TYPE = "text/xml;content=x-votable";

    private SiaRequest siaRequest;

    public AdqlQueryGenerator(SiaRequest siaRequest)
    {
        this.siaRequest = siaRequest;
    }

    public Map<String, Object> getParameterMap()
    {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("REQUEST", "doQuery");
        map.put("LANG", "ADQL");
        map.put("FORMAT", SIA_CONTENT_TYPE);
        if (siaRequest.isMetadataFormat)
        {
            map.put("QUERY", getMetadataQuery());
        }
        else
        {
            map.put("QUERY", getQuery());
        }

        Integer maxRec = getMaxRec();
        if (maxRec != null)
        {
            map.put("MAXREC", maxRec);
        }
        
        return map;
    }

    protected String encode(String s)
    {
        // URLEncode the query string.
        try
        {
            return URLEncoder.encode(s, "UTF-8");

        }
        catch (UnsupportedEncodingException impossible)
        {
            throw new RuntimeException("BUG", impossible);
        }
    }

    protected String getQuery()
    {
        StringBuilder query = new StringBuilder();
        query.append("SELECT ");
        query.append(COLUMNS);
        query.append(" FROM ");
        query.append(TABLE);
        query.append(" WHERE ");


        // If not a metadata query and POS is defined.
        // If size = 0, create a point.
        if (siaRequest.size[0] == 0 && siaRequest.size[1] == 0)
        {
            query.append(INTERSECTS_OPEN_POINT);
            query.append(siaRequest.pos[0]);
            query.append(",");
            query.append(siaRequest.pos[1]);
            query.append(INTERSECTS_CLOSE_POINT);
        }
        else
        {
             // BOX support in TAP fails at poles, so use circle
            query.append(INTERSECTS_OPEN_CIRCLE);
            query.append(siaRequest.pos[0]);
            query.append(",");
            query.append(siaRequest.pos[1]);
            query.append(",");
            query.append( 0.5*Math.max(siaRequest.size[0], siaRequest.size[1]));
            query.append(INTERSECTS_CLOSE);
        }

        // If the query is restricted to a single collection.
        if (siaRequest.collection != null && !SiaRequest.ALL_COLLECTIONS.equals(siaRequest.collection))
        {
            query.append(" AND collection = '");
            query.append(siaRequest.collection);
            query.append("'");
        }

        // If format is image/fits.
        if (siaRequest.isFitsFormat)
        {
            query.append(" AND imageFormat in ('image/fits', 'application/fits')");
        }
        
        return query.toString();
    }

    protected String getMetadataQuery()
    {
        StringBuilder query = new StringBuilder();
        query.append("SELECT TOP 0 ");
        query.append(COLUMNS);
        query.append(" FROM ");
        query.append(TABLE);
        return query.toString();
    }
    
    protected Integer getMaxRec()
    {
        return siaRequest.maxRec;
    }
    
}

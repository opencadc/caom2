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

import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.Parameter;
import ca.nrc.cadc.uws.ParameterUtil;
import java.util.Arrays;
import java.util.List;
import org.apache.log4j.Logger;

/**
 * Extract SIA request parameters as submitted via HTTP.
 * 
 * @author jburke
 */
public class SiaRequest
{
    private static Logger log = Logger.getLogger(SiaRequest.class);

    public static final String DEFAULT_FORMAT = "ALL";
    public static final String ALL_COLLECTIONS = "ALL";

    public static final String MODE_ARCHIVE = "archive";
    public static final String MODE_CUTOUT = "cutout";

    private static List<String> collections;
    private static List<String> modes;
    static
    {
       String[] c = { 
           "CFHT", "CFHTMEGAPIPE", "CFHTWIRWOLF", "CFHTTERAPIX", 
           "CGPS", "HST", "IRIS", "VGPS", "JCMT", "OMM",
           ALL_COLLECTIONS 
       };
       String[] m = { MODE_ARCHIVE, MODE_CUTOUT };
       SiaRequest.collections = Arrays.asList(c);
       SiaRequest.modes = Arrays.asList(m);
    }

    public double[] pos;
    public double[] size;
    public String format;
    public String collection;
    public String mode;
    public Integer maxRec;
    
    public boolean isAllFormat;
    public boolean isFitsFormat;
    public boolean isCustomFormat;
    public boolean isMetadataFormat;

    public SiaRequest(Job job, double maxSearchSize)
        throws SiaUsageException
    {
         List<Parameter> parameters = job.getParameterList();

        if (parameters == null)
        {
            throw new SiaUsageException("No query parameters found");
        }

        // must support: POS
        String parameter = getParameter(parameters, "POS");
        validatePos(parameter);

        // must support: SIZE
        parameter = getParameter(parameters, "SIZE");
        validateSize(parameter, maxSearchSize);

        // optional: FORMAT
        parameter = getParameter(parameters, "FORMAT");
        validateFormat(parameter);

        // optional: MAXREC
        parameter = getParameter(parameters, "MAXREC");
        validateMaxRec(parameter);

        // optional: collection
        validateCollection(job, "collection", collections);

        validateMode(job, "mode", modes);
        
        // If FORMAT != METADATA, pos and size must be specified.
        if (!isMetadataFormat && (pos == null || size == null))
        {
            throw new SiaUsageException("POS and SIZE must be specified.");
        }

        log.debug(toString());
    }

    private void validatePos(String parameter)
        throws SiaUsageException
    {
        if (parameter == null || parameter.trim().isEmpty())
        {
            pos = null;
            return;
        }

        pos = new double[2];
        try
        {
            String[] tokens = parameter.split(",");
            pos[0] = Double.parseDouble(tokens[0]);
            pos[1] = Double.parseDouble(tokens[1]);
        }
        catch(IndexOutOfBoundsException nex)
        {
            throw new SiaUsageException("failed to parse POS: " + parameter + " reason: requires two values separated by a comma");
        }
        catch(Throwable t)
        {
            throw new SiaUsageException("failed to parse POS: " + parameter, t);
        }

    }

    private void validateSize(String parameter, double maxSearchSize)
        throws SiaUsageException
    {
        // default size=0
        size = new double[] {0, 0};

        if (parameter == null || parameter.trim().isEmpty())
        {
            return;
        }

        try
        {
            String[] tokens = parameter.split(",");
            size[0] = Double.parseDouble(tokens[0]);
            if (tokens.length == 1)
            {
                size[1] = size[0];
            }
            else
            {
                size[1] = Double.parseDouble(tokens[1]);
            }
        }
        catch(IndexOutOfBoundsException nse)
        {
            throw new SiaUsageException("failed to parse SIZE: " + parameter + " reason: requies 1 or 2 values separated by comma");
        }
        catch(Throwable t)
        {
            throw new SiaUsageException("failed to parse SIZE: " + parameter, t);
        }
        if (size[0] > maxSearchSize)
        {
            size[0] = maxSearchSize;
        }
        if (size[1] > maxSearchSize)
        {
            size[1] = maxSearchSize;
        }
    }

    private void validateFormat(String parameter)
    {
        if (parameter == null)
        {
            format = DEFAULT_FORMAT;
            isAllFormat = true;
            return;
        }

        format = parameter;
        if (parameter.equals("ALL"))
        {
            isAllFormat = true;
        }
        else if (parameter.equals("METADATA"))
        {
            isMetadataFormat = true;
        }
        else if (parameter.equals("image/fits") || parameter.equals("application/fits"))
        {
            isFitsFormat = true;
        }
        else
        {
            // HACK:  we only have fits but this simulates it
            log.debug("FORMAT="+parameter + " being simulated with MAXREC=0");
            isCustomFormat = true;
            maxRec = 0;
        }
    }

    private void validateMaxRec(String parameter)
        throws SiaUsageException
    {
        log.debug("validateMaxRec: " + parameter);
        if (parameter == null)
        {
            maxRec = null;
            return;
        }

        if (isMetadataFormat)
        {
            log.debug("isMetadataFormat="+isMetadataFormat + ", adding MAXREC=0");
            maxRec = 0;
            return;
        }
        
        try
        {
            maxRec = Integer.valueOf(parameter);
        }
        catch (NumberFormatException nfe)
        {
            throw new SiaUsageException("Unable to parse MAXREC value because " + nfe.getMessage());
        }

        if (maxRec < 0)
            maxRec = null;
    }

    private void validateCollection(Job job, String param, List<String> collections)
    {
        log.debug("validateCollection: request path=" + job.getRequestPath());
        // currently registered pattern: /sia/[<collection>/]query
        String col = null;
        
        if (job.getRequestPath() != null)
        {
            String[] parts = job.getRequestPath().split("/");
            if (parts.length == 4)
            {
                col = parts[2];
            }
        }
        log.debug("validateCollection: col from path=" + col);
        
        // new implementation: optional collection parameter
        if (col == null)
        {
            col = ParameterUtil.findParameterValue(param, job.getParameterList());
        }
        if (col == null)
        {
            return;
        }

        // restrict to known values
        if (collections.contains(col))
        {
            this.collection = col;
        }
        else
        {
            // unknown collection: force no results
            this.maxRec = 0;
        }
        log.debug("validateCollection: collection=" + collection);
    }

    private void validateMode(Job job, String param, List<String> modes)
    {
        String m = ParameterUtil.findParameterValue(param, job.getParameterList());
        if (m == null)
        {
            return;
        }
        if (modes.contains(m))
        {
            this.mode = m;
        }
    }

    private String getParameter(List<Parameter> parameters, String name)
    {
        for (Parameter parameter : parameters)
        {
            if (parameter.getName().equalsIgnoreCase(name))
            {
                return parameter.getValue();
            }
        }
        return null;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("SiaRequest[");
        sb.append("(");
        if (pos == null)
        {
            sb.append("null");
        }
        else
        {
            for (int i = 0; i < pos.length; i++)
            {
                sb.append(pos[i]);
                if (i < pos.length - 1)
                {
                    sb.append(",");
                }
            }
        }
        sb.append("), (");
        if (size == null)
        {
            sb.append("null");
        }
        else
        {
            for (int i = 0; i < size.length; i++)
            {
                sb.append(size[i]);
                if (i < size.length - 1)
                {
                    sb.append(",");
                }
            }
        }
        sb.append("), ");
        sb.append(format);
        sb.append("]");
        return sb.toString();
    }
    
}

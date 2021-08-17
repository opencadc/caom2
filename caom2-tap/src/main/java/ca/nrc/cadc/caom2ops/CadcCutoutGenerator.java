/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2021.                            (c) 2021.
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
************************************************************************
*/

package ca.nrc.cadc.caom2ops;

import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.artifact.resolvers.CadcResolver;
import ca.nrc.cadc.caom2.util.CaomValidator;
import ca.nrc.cadc.net.NetUtil;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.List;
import org.apache.log4j.Logger;

/**
 * CutoutGenerator for the CADC Storage Inventory system.
 * 
 * @author adriand
 */
public class CadcCutoutGenerator extends CadcResolver implements CutoutGenerator {
    private static final Logger log = Logger.getLogger(CadcCutoutGenerator.class);

    static final String CUTOUT_PARAM = "SUB";
    
    public CadcCutoutGenerator() {
        super();
    }

    protected CadcCutoutGenerator(final String scheme) {
        super(scheme);
    }

    @Override
    public boolean canCutout(Artifact a) {
        // file types supported by SODA
        return "application/fits".equals(a.contentType) || "image/fits".equals(a.contentType);
    }

    @Override
    public URL toURL(URI uri, List<String> cutouts, String label) {
        URL base = super.toURL(uri);
        if (cutouts == null || cutouts.isEmpty()) {
            return base;
        }

        StringBuilder sb = new StringBuilder();
        sb.append(base.toExternalForm());
        appendCutoutQueryString(sb, cutouts, null, CUTOUT_PARAM);

        try {
            return new URL(sb.toString());
        } catch (MalformedURLException ex) {
            throw new RuntimeException("BUG: failed to generate cutout URL", ex);
        }
    }

    static String removeCompressionExtension(String uriFilename) {
        String filename = uriFilename;
        int i = uriFilename.lastIndexOf('.');
        if (i != -1 && i < uriFilename.length() - 1) {
            String ext = uriFilename.substring(i + 1, uriFilename.length());
            if (ext.equalsIgnoreCase("cf") || ext.equalsIgnoreCase("z")
                || ext.equalsIgnoreCase("gz") || ext.equalsIgnoreCase("fz")) {
                filename = uriFilename.substring(0, i);
            }
        }
        return filename;
    }

    /*
     * Equivalent behaviour to the algorithm used in fslice2-fcat.sh.
     * Replaces non alphanumerics with an underscore. Removes the leading and trailing underscore.
     */
    static String replaceNonAlphanumeric(List<String> cutouts) {
        String cutoutString = "";
        for (String cutout : cutouts) {
            cutoutString = cutoutString + cutout.replaceAll("[^0-9a-zA-Z]","_").substring(1, cutout.length() - 1) + "___";
        }
        
        // remove the the last 3 underscores that separate between two sequential cutout strings
        return cutoutString.substring(0, cutoutString.length() - 3);
    }
    
    static String generateFilename(URI uri, String label, List<String> cutouts) {
        String filename = null;
        if (label != null) {
            try {
                CaomValidator.assertValidPathComponent(AdCutoutGenerator.class, "filename", label);
                String ssp = uri.getSchemeSpecificPart();
                int i = ssp.lastIndexOf('/');
                if (i != -1 && i < ssp.length() - 1) {
                    String uncompressedFilename = removeCompressionExtension(ssp.substring(i + 1));
                    String head = uncompressedFilename;
                    String tail = "";
                    int index = uncompressedFilename.lastIndexOf('.');
                    if (index > -1) {
                        head = uncompressedFilename.substring(0, index);
                        tail = uncompressedFilename.substring(index, uncompressedFilename.length());
                    }

                    filename = head + "__" + label + "__" + replaceNonAlphanumeric(cutouts) + tail;
                }
            } catch (IllegalArgumentException ex) {
                throw new UsageFault(ex.getMessage());
            }
        }
        return filename;
    }

    static void appendCutoutQueryString(StringBuilder sb, List<String> cutouts, String filename) {
        appendCutoutQueryString(sb, cutouts, filename, "cutout");
    }
     
    // package access so other CutoutGenerator implementations can use it
    static void appendCutoutQueryString(StringBuilder sb, List<String> cutouts, String filename, String cutoutParamName) {
        if (cutouts != null && !cutouts.isEmpty()) {
            boolean add = (sb.indexOf("?") > 0); // already has query params
            if (!add) {
                sb.append("?");
            }
             
            if (filename != null) {
                if (add) {
                    sb.append("&");
                }
                add = true;
                sb.append("fo=").append(filename);
            }
            
            for (String cutout : cutouts) {
                if (add) {
                    sb.append("&");
                }
                add = true;
                sb.append(cutoutParamName).append("=").append(NetUtil.encode(cutout));
            }
        }
    }
}

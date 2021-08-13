/*
 ************************************************************************
 ****  C A N A D I A N   A S T R O N O M Y   D A T A   C E N T R E  *****
 *
 * (c) 2020.                            (c) 2020.
 * National Research Council            Conseil national de recherches
 * Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
 * All rights reserved                  Tous droits reserves
 *
 * NRC disclaims any warranties         Le CNRC denie toute garantie
 * expressed, implied, or statu-        enoncee, implicite ou legale,
 * tory, of any kind with respect       de quelque nature que se soit,
 * to the software, including           concernant le logiciel, y com-
 * without limitation any war-          pris sans restriction toute
 * ranty of merchantability or          garantie de valeur marchande
 * fitness for a particular pur-        ou de pertinence pour un usage
 * pose.  NRC shall not be liable       particulier.  Le CNRC ne
 * in any event for any damages,        pourra en aucun cas etre tenu
 * whether direct or indirect,          responsable de tout dommage,
 * special or general, consequen-       direct ou indirect, particul-
 * tial or incidental, arising          ier ou general, accessoire ou
 * from the use of the software.        fortuit, resultant de l'utili-
 *                                      sation du logiciel.
 *
 ****  C A N A D I A N   A S T R O N O M Y   D A T A   C E N T R E  *****
 ************************************************************************
 */

package ca.nrc.cadc.caom2ops;

import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.artifact.resolvers.AdResolver;
import ca.nrc.cadc.caom2.util.CaomValidator;
import ca.nrc.cadc.net.NetUtil;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.List;

/**
 * CutoutGenerator implementation for the Archive Directory (ad) system.
 * This class can convert an AD URI into a URL. This is an alternate version
 * that uses the RegistryClient to find the data web service base URL.
 *
 * @author yeunga
 */
public class AdCutoutGenerator extends AdResolver implements CutoutGenerator {

    public AdCutoutGenerator() {
        
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
        String filename = generateFilename(uri, label, cutouts);
        appendCutoutQueryString(sb, cutouts, filename);

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

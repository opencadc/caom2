/*
 ************************************************************************
 ****  C A N A D I A N   A S T R O N O M Y   D A T A   C E N T R E  *****
 *
 * (c) 2017.                            (c) 2017.
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

package ca.nrc.cadc.caom2.artifact.resolvers;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.net.StorageResolver;
import ca.nrc.cadc.reg.Capabilities;
import ca.nrc.cadc.reg.Capability;
import ca.nrc.cadc.reg.Interface;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;

import javax.security.auth.Subject;

import org.apache.log4j.Logger;

/**
 * StorageResolver implementation for the MAST archive. 
 * This class can convert an MAST URI into a URL . This is an alternate version that uses the RegistryClient to find the data web service base URL.
 *
 * @author yeunga
 */
public class CadcMastResolver implements StorageResolver {
    private static final Logger log = Logger.getLogger(CadcMastResolver.class);

    public static final String SCHEME = "mast";
    private static final URI DATA_RESOURCE_ID = URI.create("ivo://cadc.nrc.ca/data");
    private String baseDataURL;

    public CadcMastResolver() {
        try {
            RegistryClient rc = new RegistryClient();
            Subject subject = AuthenticationUtil.getCurrentSubject();
            AuthMethod authMethod = AuthenticationUtil.getAuthMethodFromCredentials(subject);
            if (authMethod == null) {
                authMethod = AuthMethod.ANON;
            }
            Capabilities caps = rc.getCapabilities(DATA_RESOURCE_ID);
            Capability dataCap = caps.findCapability(Standards.DATA_10);
            URI securityMethod = Standards.getSecurityMethod(authMethod);
            Interface ifc = dataCap.findInterface(securityMethod);
            if (ifc == null) {
                throw new IllegalArgumentException("No interface for security method " + securityMethod);
            }
            this.baseDataURL = ifc.getAccessURL().getURL().toString();
        } catch (Throwable t) {
            String message = "Failed to initialize data URL";
            throw new RuntimeException(message, t);
        }
    }

    @Override
    public URL toURL(URI uri) {
        if (!SCHEME.equals(uri.getScheme())) {
            throw new IllegalArgumentException("invalid scheme in " + uri);
        }

        try {
            URL url = new URL(this.baseDataURL + "/MAST/" + uri.getRawSchemeSpecificPart());
            log.debug(uri + " --> " + url);
            return url;
        } catch (MalformedURLException ex) {
            throw new RuntimeException("BUG", ex);
        }
    }

    @Override
    public String getSchema() {
        return SCHEME;
    }
}

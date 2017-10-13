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

import ca.nrc.cadc.net.StorageResolver;

import java.net.URI;
import java.net.URL;

/**
 * StorageResolver implementation for the ESAC MAST archive. 
 * This class can convert an ESAC MAST URI into a URL. This is an alternate version that uses the RegistryClient to find the data web service base URL.
 *
 * @author yeunga
 */
public class EsacMastResolver implements StorageResolver {

    public EsacMastResolver() {
        throw new UnsupportedOperationException("");
    }

    @Override
    public String getSchema() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public URL toURL(URI arg0) throws IllegalArgumentException {
        // TODO Auto-generated method stub
        return null;
    }

}

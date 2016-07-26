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
*  $Revision: 5 $
*
************************************************************************
*/

package ca.nrc.cadc.caom2ops;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.CredUtil;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.uws.ErrorSummary;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.vos.Direction;
import ca.nrc.cadc.vos.Protocol;
import ca.nrc.cadc.vos.Transfer;
import ca.nrc.cadc.vos.VOS;
import ca.nrc.cadc.vos.VOSURI;
import ca.nrc.cadc.vos.client.ClientTransfer;
import ca.nrc.cadc.vos.client.VOSpaceClient;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.List;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;

/**
 *
 * @author pdowler
 */
public class VOSpaceSchemeHandler implements SchemeHandler
{
    private static final Logger log = Logger.getLogger(VOSpaceSchemeHandler.class);

    public static final String SCHEME = "vos";
    
    private AuthMethod authMethod;
    
    public VOSpaceSchemeHandler() 
    { 
        this.authMethod = AuthenticationUtil.getAuthMethod(AuthenticationUtil.getCurrentSubject());
    }

    public URL getURL(URI uri)
    {
        if (!SCHEME.equals(uri.getScheme()))
            throw new IllegalArgumentException("invalid scheme in " + uri);

        try
        {
            String s = createURL(uri);
            URL url = null;
            if (s != null)
                url = new URL(s);
            log.debug(uri + " --> " + url);
            return url;
        }
        catch(MalformedURLException ex)
        {
            throw new RuntimeException("BUG", ex);
        }
    }

    public void setAuthMethod(AuthMethod authMethod)
    {
        this.authMethod = authMethod;
    }

    
    private String createURL(URI uri)
    {
        String errorMessage = null;
        try
        {
            VOSURI vuri = new VOSURI(uri);
            VOSpaceClient vosClient = new VOSpaceClient(vuri.getServiceURI());

            List<Protocol> protocols = new ArrayList<Protocol>();
            if ( AuthMethod.CERT.equals(authMethod))
                protocols.add(new Protocol(VOS.PROTOCOL_HTTPS_GET));
            else
                protocols.add(new Protocol(VOS.PROTOCOL_HTTP_GET));

            Transfer trans = new Transfer(vuri.getURI(), Direction.pullFromVoSpace, protocols);
            ClientTransfer ct = vosClient.createTransfer(trans);
            trans = ct.getTransfer();
            for (Protocol p : trans.getProtocols())
            {
                if ( p.getEndpoint() != null) // first available URL
                    return p.getEndpoint();
            }
            // did not find desired protocol/endpoint
            if ( ExecutionPhase.ERROR.equals(ct.getPhase()))
            {
                ErrorSummary err = ct.getServerError();
                errorMessage = err.getSummaryMessage();
            }
        }
        catch(Throwable t)
        {
            throw new RuntimeException("failed to convert " + uri + " -> URL", t);
        }
        finally { }
        
        if (errorMessage != null)
            throw new RuntimeException("failed to convert " + uri + " -> URL: " + errorMessage);
        return null;
    }
}

/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2020.                            (c) 2020.
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
 *  : 5 $
 *
 ************************************************************************
 */

package ca.nrc.cadc.caom2.repo;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.cred.client.CredUtil;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.LocalAuthority;
import ca.nrc.cadc.util.MultiValuedProperties;
import ca.nrc.cadc.util.PropertiesReader;
import ca.nrc.cadc.util.StringUtil;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.AccessControlException;
import java.security.Principal;
import java.security.cert.CertificateException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.security.auth.Subject;
import javax.security.auth.x500.X500Principal;
import org.apache.log4j.Logger;
import org.opencadc.gms.GroupClient;
import org.opencadc.gms.GroupURI;
import org.opencadc.gms.GroupUtil;

/**
 * <p>
 * Authorize a subject against a properties file containing distinguished names for authorized users
 * and URI's for authorized groups.
 * </p>
 * <p>
 * The properties file contains of two keys, using multiple lines to specify multiple users or groups:
 * <code>user = user DN</code>
 * <code>group = group URI</code>
 * </p>
 * <code>
 * user = cn=user_1,ou=cadc,o=hia,c=ca
 * user = cn=user_2,ou=cadc,o=hia,c=ca
 * group = ivo://cadc.nrc.ca/gms?GROUP_1
 * group = ivo://cadc.nrc.ca/gms?GROUP_2
 * </code>
 */
public class PropertyAuthorizer {
    private static final Logger log = Logger.getLogger(PropertyAuthorizer.class);

    static final String USER_DNS_PROPERTY = "user";
    static final String GROUP_URIS_PROPERTY = "group";

    private final String propertiesFilename;

    public PropertyAuthorizer(final String propertiesFilename) {
        if (propertiesFilename == null) {
            throw new IllegalArgumentException(PropertyAuthorizer.class.getSimpleName() + ": null " + "propertiesFilename");
        }
        this.propertiesFilename = propertiesFilename;
    }

    /**
     * Check if the calling Subject matches a user DN, or is a member of a group,
     * in the properties file.
     *
     * @throws AccessControlException if the allowed users and groups properties file is not found or cannot be read,
     *                                if the Subject does not match a user, or is not a member of a group,
     *                                given in the properties file.
     */
    public void authorize()
        throws AccessControlException, ResourceNotFoundException {
        Subject subject = AuthenticationUtil.getCurrentSubject();
        log.debug("Subject: " + subject.toString());

        // Get the properties file.
        PropertiesReader propertiesReader = getPropertiesReader(this.propertiesFilename);
        if (propertiesReader == null) {
            log.debug(this.propertiesFilename + " not found");
            throw new AccessControlException("no grants configured");
        }

        // first check if request user matches authorized config file users
        Set<Principal> authorizedUsers = getAuthorizedUserPrincipals(propertiesReader);
        if (isAuthorizedUser(subject, authorizedUsers)) {
            log.debug("Subject is an authorized user");
            return;
        }

        // Check for groups configured in servlet init or properties file.
        Set<GroupURI> authorizedGroupURIs = getAuthorizedGroupUris(propertiesReader);

        // If no user or groups configured.
        if (authorizedUsers.isEmpty() && authorizedGroupURIs.isEmpty()) {
            log.debug("no user's or group's configured");
            throw new AccessControlException("no grants configured");
        }

        // Check if calling user is a member of a properties file group.
        // Assuming the list of authorized groups is small (likely one?),
        // and calling isMember() for each group, instead of getMemberships()
        // to get the list of all groups the user belongs to.
        try {
            if (CredUtil.checkCredentials()) {
                LocalAuthority loc = new LocalAuthority();
                URI resourceID = loc.getServiceURI(Standards.GMS_SEARCH_01.toString());
                GroupClient client = GroupUtil.getGroupClient(resourceID);
                for (GroupURI authorizedGroupURI : authorizedGroupURIs) {
                    if (client.isMember(authorizedGroupURI)) {
                        log.debug("authorized group: " + authorizedGroupURI);
                        return;
                    }
                }
            } else {
                throw new AccessControlException("permission denied (anon access or invalid credentials)");
            }
        } catch (CertificateException ex) {
            throw new AccessControlException("permission denied (invalid delegated client certificate)");
        }

        // If all authorization failed, throw AccessControlException.
        throw new AccessControlException("permission denied");
    }

    /**
     * Get a Set of X500Principal's from the properties file.
     *
     * @return Set of authorized X500Principals, can be an empty Set if none configured.
     */
    private Set<Principal> getAuthorizedUserPrincipals(PropertiesReader propertiesReader) {
        Set<Principal> principals = new HashSet<Principal>();
        try {
            MultiValuedProperties allProperties = propertiesReader.getAllProperties();
            List<String> properties =  allProperties.getProperty(USER_DNS_PROPERTY);
            if (properties != null) {
                for (String property : properties) {
                    if (!property.isEmpty()) {
                        principals.add(new X500Principal(property));
                        log.debug("found authorized user: " + property);
                    }
                }
            }
        } catch (IllegalArgumentException e) {
            log.debug("No authorized users configured");
        }
        return principals;
    }

    /**
     * Checks if the caller Principal matches an authorized Principal
     * from the properties file.

     * @return true if the calling user is an authorized user, false otherwise.
     */
    private boolean isAuthorizedUser(Subject subject, Set<Principal> authorizedUsers) {
        if (!authorizedUsers.isEmpty()) {
            Set<X500Principal> principals = subject.getPrincipals(X500Principal.class);
            for (Principal caller : principals) {
                for (Principal authorizedUser : authorizedUsers) {
                    if (AuthenticationUtil.equals(authorizedUser, caller)) {
                        return true;
                    }
                }
            }
        } else {
            log.debug("Authorized users not configured.");
        }
        return false;
    }

    /**
     * Get a Set of groupURI's from the properties file.
     *
     * @return Set of authorized groupURI's, can be an empty Set if none configured.
     */
    private Set<GroupURI> getAuthorizedGroupUris(PropertiesReader propertiesReader) {
        Set<GroupURI> groupUris = new HashSet<GroupURI>();
        try {
            MultiValuedProperties allProperties = propertiesReader.getAllProperties();
            List<String> properties = allProperties.getProperty(GROUP_URIS_PROPERTY);
            if (properties != null) {
                for (String property : properties) {
                    if (StringUtil.hasLength(property)) {
                        try {
                            groupUris.add(new GroupURI(new URI(property)));
                            log.debug("found authorized group: " + property);
                        } catch (IllegalArgumentException | URISyntaxException e) {
                            log.error("invalid GroupURI: " + property, e);
                        }
                    }
                }
            }
        } catch (IllegalArgumentException e) {
            log.debug("Authorized groupURI's not configured");
        }
        return groupUris;
    }

    /**
     * Read the properties file and returns a PropertiesReader.
     *
     * @return A PropertiesReader, or null if the properties file does not
     *          exist or can not be read.
     */
    private PropertiesReader getPropertiesReader(final String propertiesFilename) {
        PropertiesReader reader = null;
        if (propertiesFilename != null) {
            reader = new PropertiesReader(propertiesFilename);
            if (!reader.canRead()) {
                reader = null;
            }
        }
        return reader;
    }

}

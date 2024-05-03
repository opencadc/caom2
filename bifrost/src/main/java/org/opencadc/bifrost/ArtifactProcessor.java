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

package org.opencadc.bifrost;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.NotAuthenticatedException;
import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.CustomAxis;
import ca.nrc.cadc.caom2.Energy;
import ca.nrc.cadc.caom2.Polarization;
import ca.nrc.cadc.caom2.PolarizationState;
import ca.nrc.cadc.caom2.Position;
import ca.nrc.cadc.caom2.ProductType;
import ca.nrc.cadc.caom2.PublisherID;
import ca.nrc.cadc.caom2.Time;
import ca.nrc.cadc.caom2.access.AccessUtil;
import ca.nrc.cadc.caom2.access.ArtifactAccess;
import ca.nrc.cadc.caom2.artifact.resolvers.CaomArtifactResolver;
import ca.nrc.cadc.caom2.compute.CustomAxisUtil;
import ca.nrc.cadc.caom2.compute.CutoutUtil;
import ca.nrc.cadc.caom2.compute.EnergyUtil;
import ca.nrc.cadc.caom2.compute.PolarizationUtil;
import ca.nrc.cadc.caom2.compute.PositionUtil;
import ca.nrc.cadc.caom2.compute.TimeUtil;
import ca.nrc.cadc.caom2.types.Circle;
import ca.nrc.cadc.caom2.types.Polygon;
import ca.nrc.cadc.caom2ops.ArtifactQueryResult;
import ca.nrc.cadc.caom2ops.CutoutGenerator;
import ca.nrc.cadc.cred.client.CredUtil;
import ca.nrc.cadc.dali.util.DoubleArrayFormat;
import ca.nrc.cadc.net.NetUtil;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.StorageResolver;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.wcs.exceptions.NoSuchKeywordException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;
import org.opencadc.datalink.DataLink;
import org.opencadc.datalink.ServiceDescriptor;
import org.opencadc.datalink.ServiceParameter;
import org.opencadc.gms.GroupURI;
import org.opencadc.gms.IvoaGroupClient;
import org.opencadc.permissions.ReadGrant;
import org.opencadc.permissions.client.PermissionsCheck;
import org.opencadc.permissions.client.PermissionsClient;

/**
 * Convert Artifacts to DataLinks.
 *
 * @author pdowler
 */
public class ArtifactProcessor {

    private static final Logger log = Logger.getLogger(ArtifactProcessor.class);

    private static final String PKG_CONTENT_TYPE_TAR = "application/x-tar";

    private final RegistryClient registryClient;
    private final URI locatorService;
    private final List<URI> readGrantProviders;
    
    // reusable permission checker
    private PermissionsCheck permCheck;

    private CaomArtifactResolver artifactResolver;    
    private boolean downloadOnly;

    public ArtifactProcessor(URI locatorService, List<URI> readGrantProviders) {
        this.registryClient = new RegistryClient();
        //this.artifactResolver = new CaomArtifactResolver();
        this.locatorService = locatorService;
        this.readGrantProviders = readGrantProviders;
    }

    /**
     * Force DataLink generation to only include file download links. This is used
     * when passing the output off to the ManifestWriter instead of creating all the
     * links and having the writer filter them.
     *
     * @param downloadOnly
     */
    public void setDownloadOnly(boolean downloadOnly) {
        this.downloadOnly = downloadOnly;
    }

    public List<DataLink> process(URI uri, ArtifactQueryResult ar) {
        log.debug("process: " + uri + " " + ar);
        List<DataLink> ret = new ArrayList<>(ar.getArtifacts().size());
        int numFiles = ar.getArtifacts().size();
        
        // TODO: detect or make configurable
        DataLink.LinkAuthTerm linkAuthPrediction = DataLink.LinkAuthTerm.OPTIONAL;
        boolean pkgAuthorized = true;
        for (Artifact a : ar.getArtifacts()) {
            DataLink.Term sem = null;
            // handle known discrepancies between datalink and caom2
            if (ProductType.CATALOG.equals(a.getProductType())) {
                sem = DataLink.Term.DERIVATION;
            }
            if (ProductType.SCIENCE.equals(a.getProductType()) 
                    || ProductType.CALIBRATION.equals(a.getProductType())) {
                sem = DataLink.Term.THIS;
            }
            if (sem == null) {
                String hashed = "#" + a.getProductType().getTerm();
                // match DataLink vocabulary
                sem = DataLink.Term.getTerm(hashed);
            }
            if (sem == null) {
                sem = DataLink.Term.AUXILIARY;
            }

            Boolean linkAuthorizedPrediction = predictReadable(ar, a);
            if (linkAuthorizedPrediction != null) {
                pkgAuthorized = pkgAuthorized && linkAuthorizedPrediction;
            }
            // direct download links
            try {
                DataLink dl = new DataLink(uri.toASCIIString(), sem);
                dl.accessURL = getDownloadURL(a);
                dl.contentType = a.contentType;
                dl.contentLength = a.contentLength;
                dl.contentQualifier = null; // TODO: get plane.datatProductType?
                dl.linkAuth = linkAuthPrediction;
                dl.linkAuthorized = linkAuthorizedPrediction;
                dl.description = "download " + a.getURI().toASCIIString();
                ret.add(dl);
            } catch (MalformedURLException | RuntimeException ex) {
                DataLink dl = new DataLink(uri.toASCIIString(), sem);
                dl.errorMessage = "FatalFault: failed to generate download URL: " + ex.toString();
                ret.add(dl);
            }

            if (!downloadOnly && canCutout(a)) {
                try {
                    final ArtifactBounds ab = generateBounds(a);
                    
                    DataLink syncLink = new DataLink(uri.toASCIIString(), DataLink.Term.CUTOUT);
                    syncLink.serviceDef = "soda-" + UUID.randomUUID();
                    syncLink.contentType = a.contentType; // unchanged
                    syncLink.contentLength = null; // unknown
                    syncLink.contentQualifier = null; // unknown or still plane.datatProductType?
                    syncLink.linkAuth = linkAuthPrediction;
                    syncLink.linkAuthorized = linkAuthorizedPrediction;
                    syncLink.description = "SODA-sync cutout of " + a.getURI().toASCIIString();
                    ServiceDescriptor sds = generateServiceDescriptor(ar.getPublisherID(), Standards.SODA_SYNC_10, syncLink.serviceDef, a, ab);
                    log.debug("SODA-sync: " + sds);
                    if (sds != null) {
                        syncLink.descriptor = sds;
                        ret.add(syncLink);
                    }

                    DataLink asyncLink = new DataLink(uri.toASCIIString(), DataLink.Term.CUTOUT);
                    asyncLink.serviceDef = "soda-" + UUID.randomUUID();
                    asyncLink.contentType = a.contentType; // unchanged
                    asyncLink.contentLength = null; // unknown
                    asyncLink.contentQualifier = null; // unknown or still plane.datatProductType?
                    asyncLink.linkAuth = linkAuthPrediction;
                    asyncLink.linkAuthorized = linkAuthorizedPrediction;
                    asyncLink.description = "SODA-async cutout of " + a.getURI().toASCIIString();
                    ServiceDescriptor sda = generateServiceDescriptor(ar.getPublisherID(), Standards.SODA_ASYNC_10, asyncLink.serviceDef, a, ab);
                    log.debug("SODA-async: " + sda);
                    if (sda != null) {
                        asyncLink.descriptor = sda;
                        ret.add(asyncLink);
                    }
                } catch (NoSuchKeywordException ex) {
                    throw new RuntimeException("FAIL: invalid WCS", ex);
                }
            }
        }
        log.debug("num files for package: " + numFiles);
        if (numFiles > 1) {

            URL pkg = getBasePackageURL(ar.getPublisherID());
            log.debug("base pkg url: " + pkg);
            if (pkg != null) {
                DataLink link = new DataLink(uri.toASCIIString(), DataLink.Term.PACKAGE);
                try {
                    link.accessURL = getPackageURL(pkg, ar.getPublisherID());
                    link.contentType = PKG_CONTENT_TYPE_TAR;
                    link.linkAuth = linkAuthPrediction;
                    link.linkAuthorized = pkgAuthorized;
                    link.description = "single download containing all files (previews and thumbnails excluded)";
                } catch (MalformedURLException ex) {
                    link.errorMessage = "failed to create package link: " + ex;
                }
                ret.add(link);
            }
        }
        return ret;
    }

    private Set<GroupURI> membershipCache = new TreeSet<>();
    
    // get grants from configured grant providers and merge
    private ReadGrant getGrants(ArtifactQueryResult ar, Artifact a) {
        if (permCheck == null) {
            this.permCheck = new PermissionsCheck();
        }
        List<ReadGrant> grants = permCheck.getReadGrants(a.getURI(), readGrantProviders);
        log.debug("found: " + grants.size() + " grants");
        // merge
        boolean anon = false;
        Date expiryDate = new Date();
        for (ReadGrant g : grants) {
            anon = anon || g.isAnonymousAccess();
        }
        ReadGrant ret = new ReadGrant(a.getURI(), expiryDate, anon);
        for (ReadGrant g : grants) {
            ret.getGroups().addAll(g.getGroups());
            log.debug("getGrants: add " + g + " now: " + ret.getGroups().size());
        }
        return ret;
    }
    
    private Boolean predictReadable(ArtifactQueryResult ar, Artifact a) {
        // perform checks in the cheapest possible order to reduce latency
        
        // get access info from caom metadata already in hand
        ArtifactAccess aa = AccessUtil.getArtifactAccess(a, ar.metaRelease, ar.getMetaReadGroups(), 
                ar.dataRelease, ar.getDataReadGroups());
        if (aa.isPublic) {
            return true;
        }
        
        // call external grant provider(s)
        ReadGrant rg = getGrants(ar, a); // COST
        if (rg.isAnonymousAccess()) {
            return true;
        }
        
        if (aa.getReadGroups().isEmpty() && rg.getGroups().isEmpty()) {
            log.debug("no group grants: done");
            return false;
        }
        
        try {
            // collect allowed groups
            Set<GroupURI> allowed = new TreeSet<>();
            allowed.addAll(rg.getGroups());
            for (URI u : aa.getReadGroups()) {
                allowed.add(new GroupURI(u));
            }
            // check membership cache
            for (GroupURI mem : membershipCache) {
                if (allowed.contains(mem)) {
                    return true;
                }
            }
            
            // check group membership
            try {
                if (!CredUtil.checkCredentials()) { // maybe COST
                    log.debug("no credentials and not public");
                    return false;
                }
            } catch (CertificateException ex) {
                // TODO: respond false (probably won't be allowed) or null (don't know)?
                throw new NotAuthenticatedException("delegated proxy certificate invalid", ex);
            }
            
            IvoaGroupClient gms = new IvoaGroupClient();
            Set<GroupURI> mem = gms.getMemberships(allowed); // COST
            membershipCache.addAll(mem);
            return !mem.isEmpty();
        } catch (IOException | InterruptedException ex) {
            // TODO: respond false (probably won't be allowed) or null (don't know)?
            throw new RuntimeException("failed to verify group membership(s)", ex);
        } catch (ResourceNotFoundException ex) {
            log.debug("unable to verify membership: gms service not found", ex);
            return null; // unknown but not fail
        }
    }
    
    private class ArtifactBounds {

        public String circle;
        public String poly;
        public String bandMin;
        public String bandMax;
        public String timeMin;
        public String timeMax;
        public Set<PolarizationState> pol;
        public String customParam;
        public String customMin;
        public String customMax;
    }

    private boolean canCutout(Artifact a) {
        if (artifactResolver == null) {
            return false;
        }
        
        StorageResolver sr = artifactResolver.getStorageResolver(a.getURI());
        if (!(sr instanceof CutoutGenerator)) {
            log.debug("canCutout: no code to generate cutout for " + a.getURI());
            return false;
        }

        CutoutGenerator cg = (CutoutGenerator) sr;
        if (!cg.canCutout(a)) {
            log.debug("canCutout: artifact not supported by  " + cg.getClass().getName() + ": " + a.getURI());
            return false;
        }

        if (!CutoutUtil.canCutout(a)) {
            log.debug("canCutout: insufficient metadata to compute cutout " + a.getURI());
            return false;
        }

        // file type check moved into CutoutGenerator.canCutout(Artifact)
        return true;
    }

    private ArtifactBounds generateBounds(Artifact a)
            throws NoSuchKeywordException {
        ArtifactBounds ret = new ArtifactBounds();
        Set<Artifact> aset = new TreeSet<>();
        aset.add(a);

        // compute artifact-specific metadata for cutout params
        DoubleArrayFormat daf = new DoubleArrayFormat();

        Position pos = PositionUtil.compute(aset);
        if (pos != null) {
            log.debug("pos: " + pos.bounds + " " + pos.dimension);
        }
        if (pos != null && pos.bounds != null && pos.bounds != null
                && pos.dimension != null && (pos.dimension.naxis1 > 1 || pos.dimension.naxis2 > 1)) {
            Polygon outer = (Polygon) pos.bounds;
            ret.poly = daf.format(new CoordIterator(outer.getPoints().iterator()));

            Circle msc = outer.getMinimumSpanningCircle();
            ret.circle = daf.format(new double[]{msc.getCenter().cval1, msc.getCenter().cval2, msc.getRadius()});
        }

        Energy nrg = EnergyUtil.compute(aset);
        if (nrg != null) {
            log.debug("nrg: " + nrg.bounds + " " + nrg.dimension);
        }
        if (nrg != null && nrg.bounds != null && nrg.dimension != null && nrg.dimension > 1) {
            ret.bandMin = Double.toString(nrg.bounds.getLower());
            ret.bandMax = Double.toString(nrg.bounds.getUpper());
        }

        Time tim = TimeUtil.compute(aset);
        if (tim != null) {
            log.debug("tim: " + tim.bounds + " " + tim.dimension);
        }
        if (tim != null && tim.bounds != null && tim.dimension != null && tim.dimension > 1) {
            ret.timeMin = Double.toString(tim.bounds.getLower());
            ret.timeMax = Double.toString(tim.bounds.getUpper());
        }

        Polarization pol = PolarizationUtil.compute(aset);
        if (pol != null && pol.dimension != null && pol.dimension > 1) {
            ret.pol = pol.states;
        }

        CustomAxis ca = CustomAxisUtil.compute(aset);
        if (ca != null) {
            log.debug("custom: " + ca.getCtype() + " " + ca.bounds + " " + ca.dimension);
        }
        if (ca != null && ca.bounds != null && ca.dimension != null && ca.dimension > 1) {
            ret.customParam = ca.getCtype();
            ret.customMin = Double.toString(ca.bounds.getLower());
            ret.customMax = Double.toString(ca.bounds.getUpper());
        }

        return ret;
    }

    private ServiceDescriptor generateServiceDescriptor(PublisherID pubID, URI standardID, String id, Artifact a, ArtifactBounds ab) {
        if (ab.poly == null && ab.bandMin == null && ab.bandMax == null
                && ab.timeMin == null && ab.timeMax == null && ab.pol == null) {
            return null;
        }

        Subject caller = AuthenticationUtil.getCurrentSubject();
        AuthMethod authMethod = AuthenticationUtil.getAuthMethod(caller);
        if (authMethod == null) {
            authMethod = AuthMethod.ANON;
        }

        // generate artifact-specific SODA service descriptor
        URL accessURL = registryClient.getServiceURL(pubID.getResourceID(), standardID, authMethod);
        log.debug("resolve cuotut: " + pubID.getResourceID() + " + " + standardID + " +" + authMethod + " -> " + accessURL);
        if (accessURL == null) {
            // no SODA support for this publisherID
            return null;
        }
        ServiceDescriptor sd = new ServiceDescriptor(accessURL);
        sd.id = id;
        sd.standardID = standardID;
        sd.resourceIdentifier = pubID.getResourceID(); // the data collection
        sd.contentType = a.contentType;
        
        // TODO: example?
        sd.exampleURL = null;
        sd.exampleDescription = null;
        
        ServiceParameter sp;
        String val = a.getURI().toASCIIString();
        String arraysize = Integer.toString(val.length());
        sp = new ServiceParameter("ID", "char", arraysize, "meta.id;meta.dataset");
        sp.setValueRef(val, null);
        sd.getInputParams().add(sp);

        if (ab.poly != null) {
            sp = new ServiceParameter("POS", "char", "*", "obs.field");
            sd.getInputParams().add(sp);
        }

        if (ab.circle != null) {
            sp = new ServiceParameter("CIRCLE", "double", "3", "obs.field");
            sp.xtype = "circle";
            sp.unit = "deg";
            sp.setMinMax(null, ab.circle);
            sd.getInputParams().add(sp);
        }

        if (ab.poly != null) {
            sp = new ServiceParameter("POLYGON", "double", "*", "obs.field");
            sp.xtype = "polygon";
            sp.unit = "deg";
            sp.setMinMax(null, ab.poly);
            sd.getInputParams().add(sp);
        }

        if (ab.bandMin != null || ab.bandMax != null) {
            sp = new ServiceParameter("BAND", "double", "2", "em.wl;stat.interval");
            sp.xtype = "interval";
            sp.unit = "m";
            sp.setMinMax(ab.bandMin, ab.bandMax);
            sd.getInputParams().add(sp);
        }

        if (ab.timeMin != null || ab.timeMax != null) {
            sp = new ServiceParameter("TIME", "double", "2", "time;stat.interval");
            sp.xtype = "interval";
            sp.unit = "d";
            sp.setMinMax(ab.timeMin, ab.timeMax);
            sd.getInputParams().add(sp);
        }

        if (ab.pol != null) {
            sp = new ServiceParameter("POL", "char", "*", "phys.polarization.state");
            for (PolarizationState s : ab.pol) {
                sp.getOptions().add(s.getValue());
            }
            sd.getInputParams().add(sp);
        }
        if (ab.customParam != null && (ab.customMin != null || ab.customMax != null)) {
            sp = new ServiceParameter(ab.customParam, "double", "2", null);
            sp.xtype = "interval";
            sp.unit = CustomAxisUtil.getUnits(ab.customParam);
            sp.setMinMax(ab.customMin, ab.customMax);
            sd.getInputParams().add(sp);
        }

        return sd;

    }

    // generate URL to locator using SI_FILES API
    private URL getDownloadURL(Artifact a)
            throws MalformedURLException {
        //URL url = artifactResolver.getURL(a.getURI());
        try {
            Subject caller = AuthenticationUtil.getCurrentSubject();
            AuthMethod am = AuthenticationUtil.getAuthMethodFromCredentials(caller);
            URL baseURL = registryClient.getServiceURL(locatorService, Standards.SI_FILES, am);
            if (baseURL == null) {
                throw new RuntimeException("unable to generator URL for " + locatorService);
            }
            StringBuilder sb = new StringBuilder(baseURL.toExternalForm());
            // API: append ID to path
            sb.append("/").append(a.getURI().toASCIIString());
            return new URL(sb.toString());
        } catch (MalformedURLException ex) {
            throw new RuntimeException("BUG: failed to generate valid URL for " + locatorService
                    + " and artifact " + a.getURI());
        }
    }

    // find data collection #aux capability PKG_10 API endpoint if it exists
    private URL getBasePackageURL(PublisherID id) {
        Subject caller = AuthenticationUtil.getCurrentSubject();
        AuthMethod authMethod = AuthenticationUtil.getAuthMethod(caller);
        if (authMethod == null) {
            authMethod = AuthMethod.ANON;
        }

        URI resourceID = id.getResourceID();
        URL ret = registryClient.getServiceURL(resourceID, Standards.PKG_10, authMethod);
        log.debug("resolve package: " + id
                + " > " + resourceID + " " + Standards.PKG_10 + " " + authMethod
                + " >> " + ret);
        return ret;
    }

    // generate URL to data collection #aux capability using PKG_10 API
    private URL getPackageURL(URL pkg, PublisherID id) throws MalformedURLException {
        StringBuilder sb = new StringBuilder();
        sb.append(pkg.toExternalForm());
        sb.append("?ID=").append(NetUtil.encode(id.getURI().toASCIIString()));
        return new URL(sb.toString());
    }
}

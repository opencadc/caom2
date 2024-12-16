/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2024.                            (c) 2024.
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

package ca.nrc.cadc.caom2;

import ca.nrc.cadc.caom2.util.CaomUtil;
import ca.nrc.cadc.caom2.util.CaomValidator;
import ca.nrc.cadc.util.HexUtil;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * An observation describes a set of empirical data.
 * 
 * @author pdowler
 */
public abstract class Observation extends CaomEntity implements Comparable<Observation> {
    // immutable
    private final String collection;
    private final URI uri;
    private final String uriBucket;
    
    // mutable
    private Algorithm algorithm;
    private final SortedSet<Plane> planes = new TreeSet<>();
    private final SortedSet<URI> metaReadGroups = new TreeSet<>();

    public Integer sequenceNumber;
    public ObservationIntentType intent;
    public String type;
    public Proposal proposal;
    public Telescope telescope;
    public Instrument instrument;
    public Target target;
    public TargetPosition targetPosition;
    public Requirements requirements;
    public Environment environment;
    public Date metaRelease;

    protected Observation(String collection, URI uri, Algorithm algorithm) {
        CaomValidator.assertNotNull(getClass(), "collection", collection);
        CaomValidator.assertNotNull(getClass(), "uri", uri);
        CaomValidator.assertNotNull(getClass(), "algorithm", algorithm);
        CaomValidator.assertValidPathComponent(getClass(), "collection", collection);
        CaomValidator.assertValidIdentifier(getClass(), "uri", uri); // TODO: require uri contains collection?
        this.collection = collection;
        this.uri = uri;
        this.algorithm = algorithm;
        this.uriBucket = CaomUtil.computeBucket(uri);
    }
    

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + uri + "]";
    }

    public String getCollection() {
        return collection;
    }

    public URI getURI() {
        return uri;
    }

    public String getUriBucket() {
        return uriBucket;
    }

    public void setAlgorithm(Algorithm algorithm) {
        CaomValidator.assertNotNull(getClass(), "algorithm", algorithm);
        this.algorithm = algorithm;
    }

    public Algorithm getAlgorithm() {
        return algorithm;
    }

    public Set<Plane> getPlanes() {
        return planes;
    }

    public Set<URI> getMetaReadGroups() {
        return metaReadGroups;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (this == o) {
            return true;
        }
        if (o instanceof Observation) {
            Observation obs = (Observation) o;
            return (this.compareTo(obs) == 0);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return getURI().hashCode();
    }

    @Override
    public int compareTo(Observation o) {
        return getURI().compareTo(o.getURI());
    }
}

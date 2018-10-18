/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2018.                            (c) 2018.
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

import ca.nrc.cadc.caom2.util.CaomValidator;
import java.net.URI;
import java.util.Date;
import java.util.Set;
import java.util.TreeSet;
import org.apache.log4j.Logger;

/**
 * A Plane is a specific science data product resulting from an observation. As
 * the data for an observation is processed in different ways this makes new
 * data products (e.g. new Planes) with different characteristics.
 * 
 * @author pdowler
 */
public class Plane extends CaomEntity implements Comparable<Plane> {
    private static final long serialVersionUID = 201704121420L;
    private static final Logger log = Logger.getLogger(Plane.class);

    // immutable state
    private final String productID;

    // mutable contents
    private final Set<Artifact> artifacts = new TreeSet<Artifact>();
    private final Set<URI> metaReadAccessGroups = new TreeSet<URI>();
    private final Set<URI> dataReadAccessGroups = new TreeSet<URI>();

    // mutable state
    public URI creatorID;
    public Date metaRelease;
    public Date dataRelease;
    public DataProductType dataProductType;
    public CalibrationLevel calibrationLevel;
    public Provenance provenance;
    public Metrics metrics;
    public DataQuality quality;
    public Position position;
    public Energy energy;
    public Time time;
    public Polarization polarization;

    public Plane(String productID) {
        CaomValidator.assertValidPathComponent(getClass(), "productID",
                productID);
        this.productID = productID;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + productID + "]";
    }

    public String getProductID() {
        return productID;
    }

    public Set<Artifact> getArtifacts() {
        return artifacts;
    }

    public Set<URI> getMetaReadAccessGroups() {
        return metaReadAccessGroups;
    }

    public Set<URI> getDataReadAccessGroups() {
        return dataReadAccessGroups;
    }
    
    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (this == o) {
            return true;
        }
        if (o instanceof Plane) {
            Plane p = (Plane) o;
            return (this.hashCode() == p.hashCode());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return productID.hashCode();
    }

    @Override
    public int compareTo(Plane p) {
        return this.productID.compareTo(p.productID);
    }
}

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
*  $Revision: 4 $
*
************************************************************************
*/

package ca.nrc.cadc.caom2;

import ca.nrc.cadc.caom2.types.EnergyUtil;
import ca.nrc.cadc.caom2.types.PolarizationUtil;
import ca.nrc.cadc.caom2.types.PositionUtil;
import ca.nrc.cadc.caom2.types.TimeUtil;
import ca.nrc.cadc.caom2.util.CaomValidator;
import ca.nrc.cadc.wcs.exceptions.NoSuchKeywordException;
import ca.nrc.cadc.wcs.exceptions.WCSLibRuntimeException;
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
public class Plane extends AbstractCaomEntity implements Comparable<Plane>
{
    private static final long serialVersionUID = 201110261400L;
    private static final Logger log = Logger.getLogger(Plane.class);
    
    // immutable state
    private final String productID;
    
    // mutable contents
    private final Set<Artifact> artifacts = new TreeSet<Artifact>();

    // mutable state
    public Date metaRelease;
    public Date dataRelease;
    public DataProductType dataProductType;
    public CalibrationLevel calibrationLevel;
    public Provenance provenance;
    public Metrics metrics;
    public DataQuality quality;

    /**
     * Computed position metadata. Computed state is always qualified as transient
     * since it does not (always) need to be stored or serialised.
     * @see computeTransientState()
     */
    public transient Position position;
    /**
     * Computed energy metadata. Computed state is always qualified as transient
     * since it does not (always) need to be stored or serialised.
     * @see computeTransientState()
     */
    public transient Energy energy;
    /**
     * Computed time metadata. Computed state is always qualified as transient
     * since it does not (always) need to be stored or serialised.
     * @see computeTransientState()
     */
    public transient Time time;
    /**
     * Computed polarization metadata. Computed state is always qualified as transient
     * since it does not (always) need to be stored or serialised.
     * @see computeTransientState()
     */
    public transient Polarization polarization;

    public Plane(String productID)
    {
        CaomValidator.assertNotNull(getClass(), "productID", productID);
        this.productID = productID;
    }

    /**
     * Clear all computed state.
     */
    public void clearTransientState()
    {
        this.position = null;
        this.energy = null;
        this.time = null;
        this.polarization = null;
        // clear metaRelease to children
        for (Artifact a : artifacts)
        {
            a.metaRelease = null;
            for (Part p : a.getParts())
            {
                p.metaRelease = null;
                for (Chunk c : p.getChunks())
                {
                    c.metaRelease = null;
                }
            }
        }
    }

    /**
     * Force (re)computation of all computed state.
     */
    public void computeTransientState()
    {
        computePosition();
        computeEnergy();
        computeTime();
        computePolarization();
        
        propagateMetaRelease();
    }

    public PlaneURI getURI(ObservationURI parentURI)
    {
        return new PlaneURI(parentURI, productID);
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[" + productID + "]";
    }

    public String getProductID()
    {
        return productID;
    }

    public Set<Artifact> getArtifacts()
    {
        return artifacts;
    }

    /**
     * Get the computed position metadata. Note that computeTransientState must 
     * be called first (no lazy computation triggered here).
     * @return 
     * @deprecated field is now public
     */
    public Position getPosition()
    {
        return position;
    }

    /**
     * Get the computed energy metadata. Note that computeTransientState must 
     * be called first (no lazy computation triggered here).
     * @return 
     * @deprecated field is public
     */
    public Energy getEnergy()
    {
        return energy;
    }

    /**
     * Get the computed time metadata. Note that computeTransientState must be 
     * called first (no lazy computation triggered here).
     * @return 
     * @deprecated field is now public
     */
    public Time getTime()
    {
        return time;
    }

    /**
     * Get the computed polarization metadata. Note that computeTransientState 
     * must be called first (no lazy computation triggered here).
     * @return 
     * @deprecated field is now public
     */
    public Polarization getPolarization()
    {
        return polarization;
    }
    
    @Override
    public boolean equals(Object o)
    {
        if (o == null)
            return false;
        if (this == o)
            return true;
        if (o instanceof Plane)
        {
            Plane p = (Plane) o;
            return ( this.hashCode() == p.hashCode() );
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return productID.hashCode();
    }

    public int compareTo(Plane p)
    {
        return this.productID.compareTo(p.productID);
    }

    protected void computePosition()
    {
        try
        {
            this.position = PositionUtil.compute(artifacts);
        }
        catch(NoSuchKeywordException ex)
        {
            throw new IllegalArgumentException("failed to compute Plane.position", ex);
        }
        catch(WCSLibRuntimeException ex)
        {
            throw new IllegalArgumentException("failed to compute Plane.position", ex);
        }
    }

    protected void computeEnergy()
    {
        try
        {
            this.energy = EnergyUtil.compute(artifacts);
        }
        catch(NoSuchKeywordException ex)
        {
            throw new IllegalArgumentException("failed to compute Plane.energy", ex);
        }
        catch(WCSLibRuntimeException ex)
        {
            throw new IllegalArgumentException("failed to compute Plane.energy", ex);
        }
    }

    protected void computeTime()
    {
        this.time = TimeUtil.compute(artifacts);
    }

    protected void computePolarization()
    {
        this.polarization = PolarizationUtil.compute(artifacts);
    }

    protected void propagateMetaRelease()
    {
        // propagate metaRelease to children of the plane
        for (Artifact a : artifacts)
        {
            a.metaRelease = metaRelease;
            for (Part p : a.getParts())
            {
                p.metaRelease = metaRelease;
                for (Chunk c : p.getChunks())
                {
                    c.metaRelease = metaRelease;
                }
            }
        }
    }

}

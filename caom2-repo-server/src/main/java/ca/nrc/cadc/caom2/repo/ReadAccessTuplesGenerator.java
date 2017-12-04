/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2017.                            (c) 2017.
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

package ca.nrc.cadc.caom2.repo;

import ca.nrc.cadc.ac.Group;
import ca.nrc.cadc.ac.GroupAlreadyExistsException;
import ca.nrc.cadc.ac.GroupNotFoundException;
import ca.nrc.cadc.ac.GroupURI;
import ca.nrc.cadc.ac.UserNotFoundException;
import ca.nrc.cadc.ac.client.GMSClient;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.caom2.Proposal;
import ca.nrc.cadc.caom2.access.ObservationMetaReadAccess;
import ca.nrc.cadc.caom2.access.PlaneDataReadAccess;
import ca.nrc.cadc.caom2.access.PlaneMetaReadAccess;
import ca.nrc.cadc.caom2.access.ReadAccess;
import ca.nrc.cadc.caom2.persistence.DuplicateEntityException;
import ca.nrc.cadc.caom2.persistence.ReadAccessDAO;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.net.TransientException;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.AccessControlException;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * A class to generate and clean up the read access tuples of an observation. The proposal group, 
 * operator group and staff group are added/updated based on the group configuration provided at
 * construction time.
 *
 */
public class ReadAccessTuplesGenerator {
    private static Logger log = Logger.getLogger(ReadAccessTuplesGenerator.class);

    private static final Class[] READACCESS_CLASSES =
    {
        ObservationMetaReadAccess.class,
        PlaneMetaReadAccess.class,
        PlaneDataReadAccess.class
    };

    private final Map<Class,String> cleanupTupleSQL = new HashMap<Class,String>();

    // Just find work to do, but don't do it
    private boolean dryrun;

    // CADC groupID for all assets
    private GroupURI cadcGroupID = new GroupURI(URI.create("ivo://cadc.nrc.ca/gms?CADC"));

    // Collection being processed
    private String collection;

    // ISO date format
    private DateFormat dateFormat;

    private ReadAccessDAO readAccessDAO;
    private GMSClient gmsClient;
    private final List<String> updatedProposalGroups = new LinkedList<String>();

    private boolean createProposalGroup = false;
    private URI groupBaseURI;
    private GroupURI operatorGroupURI;
    private GroupURI staffGroupURI;

    private ReadAccessTuplesGenerator() {}

    /**
     * Constructor.
     *
     * @param collection
     * @param raDAO read access DAO
     * @param groupConfig group data from configuration file
     * @throws IOException
     * @throws URISyntaxException
     * @throws GroupNotFoundException
     */
    public ReadAccessTuplesGenerator(String collection, ReadAccessDAO raDAO, Map<String, Object> groupConfig)
        throws IOException, URISyntaxException, GroupNotFoundException, IllegalArgumentException {
        this(collection, false, raDAO, groupConfig);
    }
    
    /**
     * Constructor.
     *
     * @param collection
     * @param dryrun only show work if true
     * @param raDAO read access DAO
     * @param groupConfig group data from configuration file
     * @throws IOException
     * @throws URISyntaxException
     * @throws GroupNotFoundException
     */
    public ReadAccessTuplesGenerator(String collection, boolean dryrun, ReadAccessDAO raDAO, Map<String, Object> groupConfig)
        throws IOException, URISyntaxException, GroupNotFoundException, IllegalArgumentException {
        this.collection = collection;
        this.dryrun = dryrun;
        this.readAccessDAO = raDAO;
        initGroups(collection, groupConfig);
        
        // GMSClient to AC webservice        
        if (this.groupBaseURI != null) {
            this.gmsClient = new GMSClient(groupBaseURI);
        }
        
        // tuple cleanup for public data
        String omraTab = raDAO.getTable(ObservationMetaReadAccess.class);
        String obsTab = raDAO.getTable(Observation.class);
        String pmraTab = raDAO.getTable(PlaneMetaReadAccess.class);
        String pdraTab = raDAO.getTable(PlaneDataReadAccess.class);
        String planeTab = raDAO.getTable(Plane.class);

        cleanupTupleSQL.put(ObservationMetaReadAccess.class,
            "delete " + omraTab
            + " from " + omraTab + " as ra"
            + " left join " + obsTab + " as obs" + " on ra.assetID = obs.obsID"
            + " where obs.metaRelease < getdate()" // now public
            + " OR obs.obsID IS NULL");              // deleted
        cleanupTupleSQL.put(PlaneMetaReadAccess.class,
            "delete " + pmraTab 
            + " from " + pmraTab + " as ra " 
            + " left join " + planeTab + " as plane on ra.assetID = plane.planeID" 
            + " where plane.metaRelease < getdate()" // now public
            + " OR plane.planeID IS NULL");              // deleted
        cleanupTupleSQL.put(PlaneDataReadAccess.class,
            "delete " + pdraTab 
            + " from " + pdraTab + " as ra" 
            + " left join " + planeTab + " as plane on ra.assetID = plane.planeID" 
            + " where plane.dataRelease < getdate()" // now public
            + " OR plane.planeID IS NULL");              // deleted
    }

    // test ctor: enough to test the create-tuples and getProposalGroupName methods
    ReadAccessTuplesGenerator(String collection, Map<String, Object> groupConfig)
        throws IOException, URISyntaxException, GroupNotFoundException {
        initGroups(collection, groupConfig);
    }

    private void initGroups(String collection, Map<String, Object> groupConfig) 
        throws IOException, URISyntaxException, GroupNotFoundException {
        this.createProposalGroup = (boolean) groupConfig.get("proposalGroup");
        this.operatorGroupURI = (GroupURI) groupConfig.get("operatorGroup");
        this.staffGroupURI = (GroupURI) groupConfig.get("staffGroup");
        if (this.staffGroupURI != null) {
            this.groupBaseURI = staffGroupURI.getServiceID();
        }
                                       
        // ISO date format                                                               
        this.dateFormat = DateUtil.getDateFormat(DateUtil.ISO_DATE_FORMAT, DateUtil.UTC);
    }
    
    /**
     * Get the set of admin group members for this collection. Package access for testing.
     *
     * @param collection being processed.
     * @return Set of admin member groups.
     * @throws ca.nrc.cadc.ac.GroupNotFoundException
     * @throws java.io.IOException
     */

    Set<GroupURI> getAdminMembers(final String collection)
        throws GroupNotFoundException, AccessControlException, IOException,
            IllegalArgumentException {
        boolean addStaffGroup = true;
        Set<GroupURI> members = new HashSet<GroupURI>();
        CollectionReadGroups collectionReadGroups = new CollectionReadGroups(collection);
        for (GroupURI readGroupURI : collectionReadGroups.getReadGroups()) {
            members.add(readGroupURI);
            if (readGroupURI.equals(this.staffGroupURI)) {
                addStaffGroup = false;
            }
        }
        
        if (this.staffGroupURI != null && addStaffGroup) {
            members.add(this.staffGroupURI);
        }
        return members;
    }

    /**
     * Generate the group name from the collection and proposal ID.
     *
     * @param collection
     * @param proposal
     * @return String containing the group name.
     * @throws java.net.URISyntaxException
     */
    protected GroupURI getProposalGroupID(final String collection, final Proposal proposal)
        throws URISyntaxException {
        if (proposal == null) {
            return null;
        }
        if (proposal.getID().trim().isEmpty()) { // whitespace only
            return null;
        }

        StringBuilder sb = new StringBuilder();
        sb.append(groupBaseURI.toString());
        sb.append("?");
        sb.append(collection);
        sb.append("-");
        sb.append(proposal.getID().trim());
        return new GroupURI(sb.toString());
    }

    protected String formatDate(Date date) {
        if (date == null) {
            return "null";
        }
        return dateFormat.format(date);
    }

    private boolean isPublic(Observation o, Date now) {
        boolean ret = true;
        ret = ret && (o.metaRelease != null && now.compareTo(o.metaRelease) > 0);
        for (Plane p : o.getPlanes()) {
            ret = ret && (p.metaRelease != null && now.compareTo(p.metaRelease) > 0);
            ret = ret && (p.dataRelease != null && now.compareTo(p.dataRelease) > 0);
        }
        return ret;
    }

    List<ObservationMetaReadAccess> createObservationMetaReadAccess(Observation o, Date now, GroupURI proposalGroupID) {
        List<ObservationMetaReadAccess> ret = new ArrayList<ObservationMetaReadAccess>();
        UUID assetID = o.getID();
        log.debug("createObservationMetaReadAccess: " + formatDate(o.metaRelease));
        if (o.metaRelease == null || now.compareTo(o.metaRelease) <= 0) {
            if (this.operatorGroupURI != null) {
                ret.add(new ObservationMetaReadAccess(assetID, operatorGroupURI.getURI()));
            }

            if (this.createProposalGroup && proposalGroupID != null) {
                ret.add(new ObservationMetaReadAccess(assetID, proposalGroupID.getURI()));
            } 

            if (this.staffGroupURI != null) {
                ret.add(new ObservationMetaReadAccess(assetID, staffGroupURI.getURI()));
            }
        }
        return ret;
    }

    List<PlaneMetaReadAccess> createPlaneMetaReadAccess(Observation o, Date now, GroupURI proposalGroupID) {
        List<PlaneMetaReadAccess> ret = new ArrayList<PlaneMetaReadAccess>();
        for (Plane p : o.getPlanes()) {
            UUID assetID = p.getID();
            log.debug("createPlaneMetaReadAccess: " + formatDate(p.metaRelease));
            if (p.metaRelease == null || now.compareTo(p.metaRelease) <= 0) {
                if (this.operatorGroupURI != null) {
                    ret.add(new PlaneMetaReadAccess(assetID, operatorGroupURI.getURI()));
                }
                
                if (this.createProposalGroup && proposalGroupID != null) {
                    ret.add(new PlaneMetaReadAccess(assetID, proposalGroupID.getURI()));
                } 
                
                if (this.staffGroupURI != null) {
                    ret.add(new PlaneMetaReadAccess(assetID, staffGroupURI.getURI()));
                }
            }
        }
        return ret;
    }

    List<PlaneDataReadAccess> createPlaneDataReadAccess(Observation o, Date now, GroupURI proposalGroupID) {
        List<PlaneDataReadAccess> ret = new ArrayList<PlaneDataReadAccess>();
        for (Plane p : o.getPlanes()) {
            UUID assetID = p.getID();
            log.debug("createPlaneDataReadAccess: " + formatDate(p.dataRelease));
            if (p.dataRelease == null || now.compareTo(p.dataRelease) <= 0) {
                if (this.operatorGroupURI != null) {
                    ret.add(new PlaneDataReadAccess(assetID, operatorGroupURI.getURI()));
                }
                
                if (this.createProposalGroup && proposalGroupID != null) {
                    ret.add(new PlaneDataReadAccess(assetID, proposalGroupID.getURI()));
                } 
                
                if (this.staffGroupURI != null) {
                    ret.add(new PlaneDataReadAccess(assetID, staffGroupURI.getURI()));
                }
            }
        }
        return ret;
    }

    int checkProposalGroup(GroupURI groupURI) throws UserNotFoundException, TransientException {
        if (groupURI == null) {
            return 0;
        }

        int ret = 0;
        String proposalGroupName = groupURI.getName();
        Group proposalGroup = null;
        if (updatedProposalGroups.contains(proposalGroupName)) {
            log.debug("group recently updated: " + proposalGroupName);
        } else {
            try {
                proposalGroup = gmsClient.getGroup(proposalGroupName);
            } catch (IOException ioex) {
                throw new TransientException("GMSClient failed to get proposal group " + proposalGroupName, ioex);
            } catch (GroupNotFoundException ignore) {
                if (!dryrun) {
                    proposalGroup = new Group(groupURI);
                    try {
                        proposalGroup = gmsClient.createGroup(proposalGroup);
                        log.info("created group: " + proposalGroupName);
                    } catch (Exception e) {
                        throw new RuntimeException("could not create group " + proposalGroupName, e);
                    }
                }
                ret++;
            }

            if (!dryrun) {
                // Add admin groups to proposal group
                proposalGroup.getGroupAdmins().add(new Group(staffGroupURI));
                
                try {
                    gmsClient.updateGroup(proposalGroup);
                } catch (GroupNotFoundException ex) {
                    throw new RuntimeException("group not found: " + proposalGroupName + " for update (right after check/create)");
                } catch (Exception e) {
                    throw new RuntimeException("could not update group " + proposalGroupName, e);
                }
                log.info("added group admins to group: " + proposalGroupName);
            }

            // cache groups we have already updated
            updatedProposalGroups.add(proposalGroupName);
            if (updatedProposalGroups.size() > 1000) {
                // remove the oldest few hundred to keep list size moderate
                for (int i = 0; i < 200; i++) {
                    updatedProposalGroups.remove(0);
                }
            }
        }
        return ret;
    }

    public void cleanupTuples() {
        // Remove tuples for assets that are now public
        if (!dryrun) {

            JdbcTemplate jdbc = new JdbcTemplate(this.readAccessDAO.getDataSource());
            for (Class raClass : READACCESS_CLASSES) {
                if (this.readAccessDAO.getTransactionManager().isOpen()) {
                    throw new RuntimeException("BUG: found open transaction at start of deleting public tuples");
                }

                log.info("Deleting public tuples from " + raClass.getSimpleName());
                String sql = cleanupTupleSQL.get(raClass);
                log.debug("SQL : \n" + sql);
                int count = jdbc.update(sql);
                log.info("Deleted  " + count + " " + raClass.getSimpleName());

            }
        }
    }

    public void generateTuples(Observation observation) 
            throws DuplicateEntityException, GroupAlreadyExistsException, UserNotFoundException, TransientException {
        log.info("START");

        int omraTuplesInserted = 0;
        int pmraTuplesInserted = 0;
        int pdraTuplesInserted = 0;
        int groupsCreated = 0;
        boolean ok = true;

        try {
            Date now = new Date();
            boolean pub = isPublic(observation, now);
            log.debug("processing " + observation + " public: " + pub + " " + formatDate(observation.getMaxLastModified()));
            if (!pub) {
                // Create a Group for this proposalID if it doesn't exist
                GroupURI proposalGroupID = null;
                try {
                    proposalGroupID = getProposalGroupID(collection, observation.proposal);
                } catch (URISyntaxException ex) {
                    log.warn("invalid proposal_id to group name: " + observation.proposal);
                    throw new IllegalArgumentException(ex);
                }

                groupsCreated += checkProposalGroup(proposalGroupID);

                // get complete list of tuples
                List<ObservationMetaReadAccess> omra = createObservationMetaReadAccess(observation, now, proposalGroupID);
                List<PlaneMetaReadAccess> pmra = createPlaneMetaReadAccess(observation, now, proposalGroupID);
                List<PlaneDataReadAccess> pdra = createPlaneDataReadAccess(observation, now, proposalGroupID);

                // check database for existing tuples
                ListIterator<ObservationMetaReadAccess> omri = omra.listIterator();
                while (omri.hasNext()) {
                    ObservationMetaReadAccess m = omri.next();
                    ReadAccess cur = readAccessDAO.get(m.getClass(), m.getAssetID(), m.getGroupID());
                    if (cur != null) {
                        log.debug("exists (skip): " + cur);
                        omri.remove();
                    }
                }

                ListIterator<PlaneMetaReadAccess> pmri = pmra.listIterator();
                while (pmri.hasNext()) {
                    PlaneMetaReadAccess m = pmri.next();
                    ReadAccess cur = readAccessDAO.get(m.getClass(), m.getAssetID(), m.getGroupID());
                    if (cur != null) {
                        log.debug("exists (skip): " + cur);
                        pmri.remove();
                    }
                }

                ListIterator<PlaneDataReadAccess> pdri = pdra.listIterator();
                while (pdri.hasNext()) {
                    PlaneDataReadAccess m = pdri.next();
                    ReadAccess cur = readAccessDAO.get(m.getClass(), m.getAssetID(), m.getGroupID());
                    if (cur != null) {
                        log.debug("exists (skip): " + cur);
                        pdri.remove();
                    }
                }

                for (ObservationMetaReadAccess m : omra) {
                    log.debug("insert: " + m);
                    if (!dryrun) {
                        readAccessDAO.put(m);
                    }
                    omraTuplesInserted++;
                }

                for (PlaneMetaReadAccess m : pmra) {
                    log.debug("insert: " + m);
                    if (!dryrun) {
                        readAccessDAO.put(m);
                    }
                    pmraTuplesInserted++;
                }

                for (PlaneDataReadAccess m : pdra) {
                    log.debug("insert: " + m);
                    if (!dryrun) {
                        readAccessDAO.put(m);
                    }
                    pdraTuplesInserted++;
                }
            }
        } finally {
            log.info("inserted " + omraTuplesInserted + " " + pmraTuplesInserted + " " + pdraTuplesInserted + " tuples");
            log.info("created " + groupsCreated + " groups");

            log.info("DONE\n");
        }
    }

}

/*
 ************************************************************************
 ****  C A N A D I A N   A S T R O N O M Y   D A T A   C E N T R E  *****
 *
 * (c) 2014.                            (c) 2014.
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
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.LocalAuthority;
import ca.nrc.cadc.util.StringUtil;

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

public class ReadAccessTuples {
    private static Logger log = Logger.getLogger(ReadAccessTuples.class);

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

    // Default admin groups for an collection
    private Set<GroupURI> adminGroups;

    // ISO date format
    private DateFormat dateFormat;

    private ReadAccessDAO readAccessDAO;
    private GMSClient gmsClient;
    private final List<String> updatedProposalGroups = new LinkedList<String>();

    private String groupBaseURI;
    
    // group configuration info read from a properties file to determine if a group will be created or updated
    private boolean createOrUpdateProposalGroup = false;
    private URI operatorGroupURI;
    private GroupURI staffGroupURI;

    private ReadAccessTuples() {}

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
    public ReadAccessTuples(String collection, ReadAccessDAO raDAO, Map<String, Object> groupConfig)
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
    public ReadAccessTuples(String collection, boolean dryrun, ReadAccessDAO raDAO, Map<String, Object> groupConfig)
        throws IOException, URISyntaxException, GroupNotFoundException, IllegalArgumentException {
        this.collection = collection;
        this.dryrun = dryrun;
        this.readAccessDAO = raDAO;
        this.createOrUpdateProposalGroup = (boolean) groupConfig.get("proposalGroup");
        String operatorGroup = (String) groupConfig.get("operatorGroup");
        if (StringUtil.hasText(operatorGroup)) {
            this.operatorGroupURI = URI.create(operatorGroup);
        }
        String staffGroup = (String) groupConfig.get("staffGroup");
        if (StringUtil.hasText(staffGroup)) {
            this.staffGroupURI = new GroupURI(staffGroup);
        }

        // GMSClient to AC webservice        
        this.gmsClient = Util.getGMSClient();

        // Group URI base comes from LocalAuthority
        LocalAuthority localAuthority = new LocalAuthority();
        URI gmsURI = localAuthority.getServiceURI(Standards.GMS_GROUPS_01.toString());
        this.groupBaseURI = gmsURI.toString() + "?";

        // Default collection admin groups             
        this.adminGroups = getAdminMembers(collection);

        // ISO date format
        this.dateFormat = DateUtil.getDateFormat(DateUtil.ISO_DATE_FORMAT, DateUtil.UTC);
        
        init(raDAO);
    }

    // test ctor: enough to test the create-tuples and getProposalGroupName methods
    ReadAccessTuples(String collection, Map<String, Object> groupConfig)
        throws IOException, URISyntaxException, GroupNotFoundException {
        this.createOrUpdateProposalGroup = (boolean) groupConfig.get("proposalGroup");
        String operatorGroup = (String) groupConfig.get("operatorGroup");
        if (StringUtil.hasText(operatorGroup)) {
            this.operatorGroupURI = URI.create(operatorGroup);
        }
        String staffGroup = (String) groupConfig.get("staffGroup");
        if (StringUtil.hasText(staffGroup)) {
            this.staffGroupURI = new GroupURI(staffGroup);
        }

        // Group URI base comes from LocalAuthority          
        LocalAuthority localAuthority = new LocalAuthority();                         
        URI gmsURI = localAuthority.getServiceURI(Standards.GMS_GROUPS_01.toString());
        this.groupBaseURI = gmsURI.toString() + "?";
                                       
        // Default collection admin groups             
        this.adminGroups = getAdminMembers(collection);
                          
        // ISO date format                                                               
        this.dateFormat = DateUtil.getDateFormat(DateUtil.ISO_DATE_FORMAT, DateUtil.UTC);
    }

    private void init(ReadAccessDAO raDAO) {
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
        sb.append(groupBaseURI);
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

    /**
     * Returns the Set of Admin Groups for this collection.
     *
     * @return Set of admin Group members
     */
    protected Set<GroupURI> getAdminGroups() {
        return this.adminGroups;
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

    List<ObservationMetaReadAccess> createObservationMetaReadAccess(Observation o, Date now, GroupURI proposalGroupID)
        throws URISyntaxException {
        List<ObservationMetaReadAccess> ret = new ArrayList<ObservationMetaReadAccess>();
        UUID assetID = o.getID();
        log.debug("createObservationMetaReadAccess: " + formatDate(o.metaRelease));
        if (o.metaRelease == null || now.compareTo(o.metaRelease) <= 0) {
            if (this.operatorGroupURI != null) {
                ret.add(new ObservationMetaReadAccess(assetID, this.operatorGroupURI));
            }
            
            if (this.createOrUpdateProposalGroup && proposalGroupID != null) {
                ret.add(new ObservationMetaReadAccess(assetID, proposalGroupID.getURI()));
                
                for (GroupURI ag : getAdminGroups()) {
                    ret.add(new ObservationMetaReadAccess(assetID, ag.getURI()));
                }
            } else if (this.staffGroupURI != null) {
                
                for (GroupURI ag : getAdminGroups()) {
                    ret.add(new ObservationMetaReadAccess(assetID, ag.getURI()));
                }
            }
        }
        return ret;
    }

    List<PlaneMetaReadAccess> createPlaneMetaReadAccess(Observation o, Date now, GroupURI proposalGroupID)
        throws URISyntaxException {
        List<PlaneMetaReadAccess> ret = new ArrayList<PlaneMetaReadAccess>();
        for (Plane p : o.getPlanes()) {
            UUID assetID = p.getID();
            log.debug("createPlaneMetaReadAccess: " + formatDate(p.metaRelease));
            if (p.metaRelease == null || now.compareTo(p.metaRelease) <= 0) {
                if (this.operatorGroupURI != null) {
                    ret.add(new PlaneMetaReadAccess(assetID, this.operatorGroupURI));
                }
                
                if (this.createOrUpdateProposalGroup && proposalGroupID != null) {
                    ret.add(new PlaneMetaReadAccess(assetID, proposalGroupID.getURI()));
                    
                    for (GroupURI ag : getAdminGroups()) {
                        ret.add(new PlaneMetaReadAccess(assetID, ag.getURI()));
                    }
                } else if (this.staffGroupURI != null) {
                    
                    for (GroupURI ag : getAdminGroups()) {
                        ret.add(new PlaneMetaReadAccess(assetID, ag.getURI()));
                    }
                }
            }
        }
        return ret;
    }

    List<PlaneDataReadAccess> createPlaneDataReadAccess(Observation o, Date now, GroupURI proposalGroupID)
        throws URISyntaxException {
        List<PlaneDataReadAccess> ret = new ArrayList<PlaneDataReadAccess>();
        for (Plane p : o.getPlanes()) {
            UUID assetID = p.getID();
            log.debug("createPlaneDataReadAccess: " + formatDate(p.dataRelease));
            if (p.dataRelease == null || now.compareTo(p.dataRelease) <= 0) {
                if (this.operatorGroupURI != null) {
                    ret.add(new PlaneDataReadAccess(assetID, this.operatorGroupURI));
                }
                
                if (this.createOrUpdateProposalGroup && proposalGroupID != null) {
                    ret.add(new PlaneDataReadAccess(assetID, proposalGroupID.getURI()));
                    
                    for (GroupURI ag : getAdminGroups()) {
                        ret.add(new PlaneDataReadAccess(assetID, ag.getURI()));
                    }
                } else if (this.staffGroupURI != null) {
                    
                    for (GroupURI ag : getAdminGroups()) {
                        ret.add(new PlaneDataReadAccess(assetID, ag.getURI()));
                    }
                }
            }
        }
        return ret;
    }

    int checkProposalGroup(GroupURI groupURI)
        throws IOException, GroupAlreadyExistsException, UserNotFoundException {
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
                for (GroupURI ag : getAdminGroups()) {
                    proposalGroup.getGroupAdmins().add(new Group(ag));
                }
                
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

    public void generateTuples(Observation observation) {
        log.info("START");

        int omraTuplesInserted = 0;
        int pmraTuplesInserted = 0;
        int pdraTuplesInserted = 0;
        int groupsCreated = 0;
        boolean ok = true;

        try {
            System.gc(); // hint

            Date now = new Date();
            boolean pub = isPublic(observation, now);
            log.info("processing " + observation + " public: " + pub + " " + formatDate(observation.getMaxLastModified()));

            // Create a Group for this proposalID if it doesn't exist
            GroupURI proposalGroupID = null;
            try {
                proposalGroupID = getProposalGroupID(collection, observation.proposal);
            } catch (URISyntaxException ex) {
                log.warn("invalid proposal_id to group name: " + observation.proposal);
            }

            if (!pub) {
                groupsCreated += checkProposalGroup(proposalGroupID);
            }

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
        } catch (DuplicateEntityException e) {
            log.error("read access tuple already exists.", e);;
        } catch (URISyntaxException e) {
            log.error("failed to create read access tuples.", e);;
        } catch (IOException | GroupAlreadyExistsException | UserNotFoundException e) {
            log.error("failure detected while checking the proposal group.", e);;
        } finally {
            log.info("inserted " + omraTuplesInserted + " " + pmraTuplesInserted + " " + pdraTuplesInserted + " tuples");
            log.info("created " + groupsCreated + " groups");

            log.info("DONE\n");
        }
    }

}

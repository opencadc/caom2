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
************************************************************************
 */

package ca.nrc.cadc.caom2.ac;

import ca.nrc.cadc.ac.Group;
import ca.nrc.cadc.ac.GroupNotFoundException;
import ca.nrc.cadc.ac.GroupURI;
import ca.nrc.cadc.ac.client.GMSClient;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.caom2.Proposal;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.net.TransientException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;

/**
 * A class to generate read access tuples for an observation. The proposal group,
 * operator group and staff group are added/updated based on the group configuration provided at
 * construction time. This class will create new proposal groups that do not exist so must
 * be run with within a Subject.doAs(...) with suitable credentials.
 *
 */
public class ReadAccessGenerator {

    private static Logger log = Logger.getLogger(ReadAccessGenerator.class);
    
    public static final String OPERATOR_GROUP_KEY = "operatorGroup";
    public static final String STAFF_GROUP_KEY = "staffGroup";
    public static final String PROPOSAL_GROUP_KEY = "proposalGroup";

    private boolean dryrun;
    private String collection;
    private DateFormat dateFormat;

    private GMSClient gmsClient;
    private final List<String> updatedProposalGroups = new LinkedList<String>();

    private boolean createProposalGroup = false;
    private URI groupBaseURI;
    private GroupURI operatorGroupURI;
    private GroupURI staffGroupURI;

    private ReadAccessGenerator() {
    }

    /**
     * Constructor.
     *
     * @param collection the CAOM collection name
     * @param groupConfig group data from configuration file
     */
    public ReadAccessGenerator(String collection, Map<String, Object> groupConfig) {
        this(collection, groupConfig, false);
    }

    /**
     * Constructor. The groupConfig map may contain the following items:
     * <pre>
     * operatorGroup={ivo identifier for system operator group}
     * staffGroup={ivo identifier for collection or telescope staff group}
     * proposalGroup={true|false}
     * </pre>
     * The presence of each of these triggers the generation or grants to the specified groups. When
     * proposalGroup is true, groups are created (if necessary) and grants generated. Proposal group
     * names are of the form {Observation.collection}-{Observation.proposalID}. The staffGroup is set as
     * an admin of the proposalGroup so a staffGroup is mandatory when proposalGroup is true.
     *
     * @param collection the CAOM collection name
     * @param dryrun only show work if true
     * @param groupConfig group data from configuration file
     */
    public ReadAccessGenerator(String collection, Map<String, Object> groupConfig, boolean dryrun) {
        this.collection = collection;
        this.dryrun = dryrun;
        initGroups(groupConfig);

        this.dateFormat = DateUtil.getDateFormat(DateUtil.ISO_DATE_FORMAT, DateUtil.UTC);

        if (this.groupBaseURI != null) {
            this.gmsClient = new GMSClient(groupBaseURI);
        }

    }

    private void initGroups(Map<String, Object> groupConfig) {
        if (groupConfig != null) {
            Object ouri = groupConfig.get(OPERATOR_GROUP_KEY);
            if (ouri instanceof URI) {
                ouri = new GroupURI((URI) ouri);
            }
            this.operatorGroupURI = (GroupURI) ouri;
            
            Object suri = groupConfig.get(STAFF_GROUP_KEY);
            if (suri instanceof URI) {
                suri = new GroupURI((URI) suri);
            }
            this.staffGroupURI = (GroupURI) suri;
            if (this.staffGroupURI != null) {
                this.groupBaseURI = staffGroupURI.getServiceID();
            }

            Object opg = groupConfig.get(PROPOSAL_GROUP_KEY);
            if (opg instanceof Boolean) { 
                this.createProposalGroup = (Boolean) opg;
            }

            if (this.createProposalGroup && this.staffGroupURI == null) {
                
                throw new IllegalArgumentException("CONFIG: found " + PROPOSAL_GROUP_KEY + "=true with no " + STAFF_GROUP_KEY);
            }
        }
    }
    
    public void generateTuples(Observation observation)
            throws TransientException {

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

            checkProposalGroup(proposalGroupID);

            // get complete list of tuples
            createObservationMetaReadAccess(observation, now, proposalGroupID);
            for (Plane p : observation.getPlanes()) {
                createPlaneMetaReadAccess(p, now, proposalGroupID);
                createPlaneDataReadAccess(p, now, proposalGroupID);
            }
        }

    }
    
    private String formatDate(Date date) {
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

    // package access for testing
    GroupURI getProposalGroupID(final String collection, final Proposal proposal)
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
    
    void createObservationMetaReadAccess(Observation o, Date now, GroupURI proposalGroupID) {
        log.debug("createObservationMetaReadAccess: " + formatDate(o.metaRelease));
        o.getMetaReadGroups().clear();
        if (o.metaRelease == null || now.compareTo(o.metaRelease) <= 0) {
            if (this.operatorGroupURI != null) {
                o.getMetaReadGroups().add(operatorGroupURI.getURI());
            }

            if (this.createProposalGroup && proposalGroupID != null) {
                o.getMetaReadGroups().add(proposalGroupID.getURI());
            }

            if (this.staffGroupURI != null) {
                o.getMetaReadGroups().add(staffGroupURI.getURI());
            }
        }
    }

    void createPlaneMetaReadAccess(Plane p, Date now, GroupURI proposalGroupID) {
        log.debug("createPlaneMetaReadAccess: " + formatDate(p.metaRelease));
        p.getMetaReadGroups().clear();
        if (p.metaRelease == null || now.compareTo(p.metaRelease) <= 0) {
            if (this.operatorGroupURI != null) {
                p.getMetaReadGroups().add(operatorGroupURI.getURI());
            }

            if (this.createProposalGroup && proposalGroupID != null) {
                p.getMetaReadGroups().add(proposalGroupID.getURI());
            }

            if (this.staffGroupURI != null) {
                p.getMetaReadGroups().add(staffGroupURI.getURI());
            }
        }
    }

    void createPlaneDataReadAccess(Plane p, Date now, GroupURI proposalGroupID) {
        log.debug("createPlaneDataReadAccess: " + formatDate(p.dataRelease));
        p.getDataReadGroups().clear();
        if (p.dataRelease == null || now.compareTo(p.dataRelease) <= 0) {
            if (this.operatorGroupURI != null) {
                p.getDataReadGroups().add(operatorGroupURI.getURI());
            }

            if (this.createProposalGroup && proposalGroupID != null) {
                p.getDataReadGroups().add(proposalGroupID.getURI());
            }

            if (this.staffGroupURI != null) {
                p.getDataReadGroups().add(staffGroupURI.getURI());
            }
        }
    }

    private int checkProposalGroup(GroupURI groupURI) throws TransientException {
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
                log.debug("added group admins to group: " + proposalGroupName);
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
}

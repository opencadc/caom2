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

package ca.nrc.cadc.caom2;

import ca.nrc.cadc.caom2.util.CaomValidator;
import java.io.Serializable;
import java.net.URI;

/**
 *
 * @author pdowler
 */
public class VocabularyTerm {
    private final URI namespace;
    private final String term;
    private boolean base;

    /**
     * Constructor. This creates a term in the specified vocabulary namepsace
     * with default base = false.
     * 
     * @param namespace
     * @param term
     */
    public VocabularyTerm(URI namespace, String term) {
        this(namespace, term, false);
    }

    /**
     * Constructor. This creates a term in the specified vocabulary namespace.
     * If the value of base is false (default for convenience constructor) then
     * the string value (from getValue()) will just be the namespace URI plus
     * the term added as a fragment. If the value of base is true, then this is
     * a term in a base vocabulary and the value will be just the term (without
     * the namespace).
     * 
     * @param namespace
     * @param term
     * @param base
     */
    protected VocabularyTerm(URI namespace, String term, boolean base) {
        CaomValidator.assertNotNull(VocabularyTerm.class, "namespace", namespace);
        CaomValidator.assertNotNull(VocabularyTerm.class, "term", term);
        CaomValidator.assertValidPathComponent(VocabularyTerm.class, "term", term);
        if (namespace.getFragment() != null) {
            throw new IllegalArgumentException("vocabulary namespace cannot have a fragment");
        }
        this.namespace = namespace;
        this.term = term;
        this.base = base;
    }

    public URI getNamespace() {
        return namespace;
    }

    public String getTerm() {
        return term;
    }

    public String getValue() {
        if (base) {
            return term;
        }
        URI tmp = URI.create(namespace.toASCIIString() + "#" + term);
        return tmp.toASCIIString();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof VocabularyTerm) {
            VocabularyTerm rhs = (VocabularyTerm) obj;
            return (this.term.equals(rhs.term)
                    && this.namespace.equals(rhs.namespace));
        }
        return false;
    }

    @Override
    public int hashCode() {
        return getValue().hashCode();
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "[" + term + "]";
    }
}

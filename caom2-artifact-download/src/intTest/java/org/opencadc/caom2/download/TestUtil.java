/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2022.                            (c) 2022.
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

package org.opencadc.caom2.download;

import ca.nrc.cadc.caom2.artifact.ArtifactMetadata;
import ca.nrc.cadc.caom2.artifact.ArtifactStore;
import ca.nrc.cadc.caom2.artifact.StoragePolicy;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.util.FileMetadata;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.CopyOption;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.AccessControlException;
import java.util.Properties;
import java.util.Set;
import org.apache.log4j.Logger;

/**
 *
 * @author pdowler
 */
public class TestUtil {
    private static final Logger log = Logger.getLogger(TestUtil.class);

    static String TMP_DIR = "build/tmp";
    static final String USER_HOME = System.getProperty("user.home");

    static String CAOM2_SERVER = "CAOM2_ARTIFACT_DOWNLOAD_TEST";
    static String CAOM2_DATABASE = "cadctest";
    static String CAOM2_SCHEMA = "caom2";

    static {
        try {
            File opt = new File("intTest.properties");
            if (opt.exists()) {
                Properties props = new Properties();
                props.load(new FileReader(opt));
                // caom2 database properties
                String s = props.getProperty("caom2.server");
                if (s != null) {
                    CAOM2_SERVER = s.trim();
                }
                s = props.getProperty("caom2.database");
                if (s != null) {
                    CAOM2_DATABASE = s.trim();
                }
                s = props.getProperty("caom2.schema");
                if (s != null) {
                    CAOM2_SCHEMA = s.trim();
                }

            }
            log.debug("intTest caom2 database config: " + CAOM2_SERVER + " " + CAOM2_DATABASE + " " + CAOM2_SCHEMA);
        } catch (Exception oops) {
            log.debug("failed to load/read optional db config", oops);
        }
    }
    
    static ArtifactStore getArtifactStore() {
        return new TestArtifactStore();
    }
    
    private TestUtil() { 
    }
    
    static class TestArtifactStore implements ArtifactStore {
        
        File baseDir = new File("build/tmp/TestArtifactStore");
        FileSystem fs = FileSystems.getDefault();
        
        public TestArtifactStore() {
            baseDir.mkdirs();
            for (File c : baseDir.listFiles()) {
                delete(c);
            }
        }
        
        private void delete(File f) {
            if (f.isDirectory()) {
                for (File c : f.listFiles()) {
                    delete(c);
                }
                return;
            }
            f.delete();
        }
        
        private Path toPath(URI artifactURI) {
            String scheme = artifactURI.getScheme();
            String ssp = artifactURI.getSchemeSpecificPart();
            int i = ssp.lastIndexOf("/");
            String filename = ssp.substring(i + 1);
            String path = scheme + "/" + ssp.substring(0, i);
            log.info("store: " + artifactURI + " -> " + path + "/" + filename);
            File dir = new File(baseDir, path);
            log.info("mkdir: " + dir);
            dir.mkdirs();
            Path ret = fs.getPath(dir.getAbsolutePath(), filename);
            log.info("path: " + ret);
            return ret;
        }
        
        private URI toURI(Path p) {
            String abs = p.toFile().getAbsolutePath();
            String base = baseDir.getAbsolutePath();
            String rel = abs.replace(base, "");
            log.info("toURI rel: " + rel);
            rel = rel.replaceFirst("/", "");
            rel = rel.replaceFirst("/", ":");
            return URI.create(rel);
        }
        
        @Override
        public ArtifactMetadata get(URI artifactURI) 
                throws TransientException, UnsupportedOperationException, IllegalArgumentException, 
                AccessControlException, IllegalStateException {
            Path p = toPath(artifactURI);
            File f = p.toFile();
            if (f.exists() && f.isFile() && f.length() > 0) {
                URI u = toURI(p);
                ArtifactMetadata am = new ArtifactMetadata(u, null);
                am.contentLength = p.toFile().length();
                return am;
            }
            return null;
        }

        @Override
        public StoragePolicy getStoragePolicy(String collection) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void store(URI artifactURI, InputStream data, FileMetadata metadata) 
                throws TransientException, UnsupportedOperationException, IllegalArgumentException, 
                AccessControlException, IllegalStateException {
            
            try {
                Path target = toPath(artifactURI);
                Files.copy(data, target, StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException ex) {
                throw new RuntimeException("store failed", ex);
            }
        }

        @Override
        public Set<ArtifactMetadata> list(String collection) 
                throws TransientException, UnsupportedOperationException, AccessControlException {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toStorageID(String artifactURI) throws IllegalArgumentException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void processResults(long total, long successes, long totalElapsedTime, long totalBytes, int threads) {
            throw new UnsupportedOperationException();
        }
        
    }
}

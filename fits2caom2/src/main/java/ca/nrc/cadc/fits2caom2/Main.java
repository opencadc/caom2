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
package ca.nrc.cadc.fits2caom2;

import ca.nrc.cadc.auth.CertCmdArgUtil;
import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.auth.X509CertificateChain;
import ca.nrc.cadc.caom2.fits.FitsMapping;
import ca.nrc.cadc.caom2.fits.exceptions.IngestException;
import ca.nrc.cadc.net.NetrcAuthenticator;
import ca.nrc.cadc.util.ArgumentMap;
import org.apache.log4j.Logger;

import javax.security.auth.Subject;
import java.io.File;
import java.net.Authenticator;
import java.net.URI;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Executable class for fits2caom2.
 * @author jburke
 */
public class Main
{
    private static Logger log;
    
    /**
     * Main method. Validates the command line arguments and and configuration
     * file and outputs any errors found.
     * 
     * @param args command line arguments.
     */
    public static void main(String[] args)
    {
        try
        {
            ArgumentMap argsMap = new ArgumentMap(args);
            
            if (argsMap.isSet(Argument.H) || argsMap.isSet(Argument.HELP))
            {
                System.out.print(Argument.usage());
                System.exit(0);
            }

            String[] packages = new String[]
            {
                "ca.nrc.cadc.fits2caom2",
                "ca.nrc.cadc.caom2",
                "ca.nrc.cadc.caom2.xml",
                "ca.nrc.cadc.net"
            };
                
            Util.initialize(packages, argsMap);
            Main.log = Logger.getLogger(Main.class);

            if (argsMap.isSet(Argument.DUMPCONFIG))
            {
                Map<String,String> config = Util.loadConfig(argsMap.getValue(Argument.CONFIG));
                Iterator<Map.Entry<String,String>> iter = config.entrySet().iterator();
                while ( iter.hasNext() )
                {
                    Map.Entry<String,String> me = iter.next();
                    System.out.print(me.getKey());
                    System.out.print(" = ");
                    System.out.println(me.getValue());
                }
                System.exit(0);
            }
            
            Ingest ingest = createIngest(argsMap);
            Subject subject = CertCmdArgUtil.initSubject(argsMap, true);
            if (subject != null)
            {
                X509CertificateChain chain = null;
                try
                {
                    Set<X509CertificateChain> chains = subject.getPublicCredentials(X509CertificateChain.class);
                    chain = chains.iterator().next();
                    chain.getChain()[0].checkValidity();
                }
                catch(CertificateExpiredException ex)
                {
                    log.warn("X509Certificate expired at : " + chain.getExpiryDate() + ": running ingest anonymously");
                    subject = null; // drop to anon
                }
                catch (CertificateNotYetValidException ex)
                {
                    log.warn("X509Certificate is not valid yet (clock issue?): " + ex.getMessage()+  ": running ingest anonymously");
                    subject = null; // drop to anon
                }
            }
            if (subject == null)
            {
                log.debug("no SSL credentials found: running ingest anonymously");
                ingest.run();
            }
            else
            {
                log.debug("SSL credentials found: running ingest as " + subject);
                ingest.setSSLEnabled(true);
                Subject.doAs(subject, new RunnableAction(ingest));
            }
        }
        catch(IllegalArgumentException iax)
        {
            log.debug("Error during ingestion", iax);
            System.err.println("Error during ingestion: " + iax.getMessage());
            System.exit(1);
        }
        catch (IngestException ie)
        {
            log.debug("Error during ingestion", ie);
            System.err.println("Error during ingestion: " + ie.getMessage());
            System.exit(1);
        }
        catch (Throwable t)
        {
            System.err.println("BUG: unexpected failure " + t + " (stack trace follows)");
            t.printStackTrace(System.err);
            System.exit(1);
        }
        
        // Exit cleanly.
        System.exit(0);
    }

    public static Ingest createIngest(ArgumentMap argsMap)
        throws Exception
    {
        String collection = argsMap.getValue(Argument.COLLECTION);
        String observationID = argsMap.getValue(Argument.OBSERVATION_ID);
        String productID = argsMap.getValue(Argument.PRODUCT_ID);
        Util.UriLocal uriLocal = Util.argumentUriToUriLocal(argsMap.getValue(Argument.URI));
        URI[] uris = uriLocal.uri;
        File[] localFiles = uriLocal.local;

        Map<String,String> config = Util.loadConfig(argsMap.getValue(Argument.CONFIG));

        if (argsMap.isSet(Argument.NETRC_SHORT) || argsMap.isSet(Argument.NETRC_SHORT))
            Authenticator.setDefault(new NetrcAuthenticator(true));

        // create ingest with minimal required items
        Ingest ingest = new Ingest(collection, observationID, productID, uris, config);

        // Optional command line arguments.
        ingest.setKeepFiles(argsMap.isSet(Argument.KEEP));
        ingest.setDryrun(argsMap.isSet(Argument.TEST));
        ingest.setIgnorePartialWCS(argsMap.isSet(Argument.IGNORE_PARTIAL_WCS));
        
        // always set this option
        ingest.setStructFitsParse(true);

        FitsMapping fm = Util.getFitsMapping(config, argsMap.getValue(Argument.DEFAULT), argsMap.getValue(Argument.OVERRIDE));
        ingest.setMapping(fm);

        if (localFiles != null && argsMap.isSet(Argument.LOCAL))
        {
            throw new IllegalArgumentException("--uri with @filename and --local cannot be used togeather");
        }
        if (localFiles == null)
        {
            localFiles = Util.argumentFileToArray(argsMap.getValue(Argument.LOCAL));
        }
        if (localFiles != null && localFiles.length != uris.length)
            throw new IllegalArgumentException("number of --uri and --local arguments do not match");
        ingest.setLocalFiles(localFiles);

        File in = null;
        if (argsMap.isSet(Argument.IN))
        {
            String f = argsMap.getValue(Argument.IN);
            in = new File(f);
            if ( !in.isFile() )
                throw new IllegalArgumentException("not a file: " + f);
            if ( !in.canRead() )
                throw new IllegalArgumentException("not readable: " + f);
            ingest.setInFile(in);
        }

        File out = null;
        if (argsMap.isSet(Argument.OUT))
        {
            String f = argsMap.getValue(Argument.OUT);
            out = new File(f);
            if ( out.exists() && !out.isFile() )
                throw new IllegalArgumentException("not a file: " + f);
            if ( out.exists() && !out.canWrite() )
                throw new IllegalArgumentException("not writable: " + f);
            ingest.setOutFile(out);
        }
        else
            missingArg(Argument.OUT);

        File certFile = null;
        if (argsMap.isSet(CertCmdArgUtil.ARG_CERT))
        {
            String f = argsMap.getValue(CertCmdArgUtil.ARG_CERT);
            certFile = new File(f);
            if ( !certFile.isFile() )
                throw new IllegalArgumentException("not a file: " + f);
            if ( !certFile.canRead() )
                throw new IllegalArgumentException("not readable: " + f);
        }

        File keyFile = null;
        if (argsMap.isSet(CertCmdArgUtil.ARG_KEY))
        {
            String f = argsMap.getValue(CertCmdArgUtil.ARG_KEY);
            keyFile = new File(f);
            if ( !keyFile.isFile() )
                throw new IllegalArgumentException("not a file: " + f);
            if ( !keyFile.canRead() )
                throw new IllegalArgumentException("not readable: " + f);
        }

        return ingest;
    }

    private static void missingArg(String arg)
    {
        throw new IllegalArgumentException("missing required argument: " + arg);
    }

}

package ca.nrc.cadc.caom2.pkg;

import ca.nrc.cadc.net.NetUtil;
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.Log4jInit;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.compress.archivers.ArchiveEntry;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

public class TarWriterTest
{
    private static final Logger log = Logger.getLogger(TarWriterTest.class);
    
    static
    {
        Log4jInit.setLevel("ca.nrc.cadc.caom2.pkg", Level.INFO);
    }
    
    //@Test
    public void testTemplate()
    {
        try
        {
            
        }
        catch (Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("Unexpected exception: " + unexpected);
        }
        
    }
    
    @Test
    public void testCreateTar()
    {
        try
        {
            URL u1 = new URL("https://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/robots.txt");
            URL u2 = new URL("https://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/GovCanada.gif");
            URL u3 = new URL("https://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/SymbolCanada.gif");
            
            List<URL> contents = new ArrayList<URL>();
            contents.add(u1);
            contents.add(u2);
            contents.add(u3);

            File tmp = File.createTempFile("tartest", ".tar");
            FileOutputStream fos=  new FileOutputStream(tmp);
            TarWriter fw = new TarWriter(fos);
            for (URL u : contents)
            {
                fw.write("foo", u);
            }
            for (URL u : contents)
            {
                fw.write("bar", u);
            }
            fw.close();
            
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            TarWriter bw = new TarWriter(bos);
            
            for (URL u : contents)
            {
                bw.write("foo", u);
            }
            for (URL u : contents)
            {
                bw.write("bar", u);
            }
            bw.close();

            byte[] content = bos.toByteArray();
            ByteArrayInputStream in = new ByteArrayInputStream(content);

            TarArchiveInputStream tar = new TarArchiveInputStream(in);
            Content c1 = getEntry(tar);
            Content c2 = getEntry(tar);
            Content c3 = getEntry(tar);
            Content b1 = getEntry(tar);
            Content b2 = getEntry(tar);
            Content b3 = getEntry(tar);
            Content r1 = getEntry(tar); // README
            Content r2 = getEntry(tar); // README
            
            ArchiveEntry te = tar.getNextTarEntry();
            Assert.assertNull(te);

            Assert.assertEquals("name", "foo/robots.txt", c1.name);
            Assert.assertEquals("name", "foo/GovCanada.gif", c2.name);
            Assert.assertEquals("name", "foo/SymbolCanada.gif", c3.name);
            Assert.assertEquals("name", "bar/robots.txt", b1.name);
            Assert.assertEquals("name", "bar/GovCanada.gif", b2.name);
            Assert.assertEquals("name", "bar/SymbolCanada.gif", b3.name);
            // TarWriter sorts the README entries
            Assert.assertEquals("name", "bar/README", r1.name);
            Assert.assertEquals("name", "foo/README", r2.name);
        }
        catch (Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("Unexpected exception: " + unexpected);
        }
    }

    class Content
    {
        String name;
        String content;
    }
    private Content getEntry(TarArchiveInputStream tar)
            throws IOException
    {
        Content ret = new Content();
        
        TarArchiveEntry entry = tar.getNextTarEntry();
        ret.name = entry.getName();
        
        byte[] bytes = new byte[(int)entry.getSize()];
        int n = tar.read(bytes);
        Assert.assertEquals("bytes in " + ret.name, entry.getSize(), n);
        
        ret.content = new String(bytes, "UTF-8");
        return ret;
    }
}

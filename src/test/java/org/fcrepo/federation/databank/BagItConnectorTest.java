package org.fcrepo.federation.databank;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;

import javax.jcr.NamespaceRegistry;
import javax.jcr.RepositoryException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.modeshape.common.logging.Logger;
import org.modeshape.jcr.ExecutionContext;
import org.modeshape.jcr.api.nodetype.NodeTypeManager;
import org.modeshape.jcr.federation.spi.Connector;
import org.modeshape.jcr.federation.spi.DocumentWriter;

public class BagItConnectorTest {

	BagItConnector testObj;
		
	DocumentWriterFactory mockFactory;
	
	DocumentWriter mockWriter;
	
	Logger mockLogger;
	
	File tempDir;
	
	@Before
	public void setUp()
        throws NoSuchFieldException, SecurityException, IllegalArgumentException,
        IllegalAccessException, RepositoryException, IOException {
		testObj = new BagItConnector();
        testObj.setDirectoryPath("/tmp/test-objects/group1/pairtree_root");
		mockFactory = mock(DocumentWriterFactory.class);
		mockWriter = mock(DocumentWriter.class);
		mockLogger = mock(Logger.class);
		
		Field logger = Connector.class.getDeclaredField("logger");
		logger.setAccessible(true);
		logger.set(testObj, mockLogger);
		//tempDir = File.createTempFile("bagit", Long.toString(System.nanoTime()));
		//tempDir.delete();
		//tempDir.mkdirs();
		Field dirPath = BagItConnector.class.getDeclaredField("directoryPath");
		dirPath.setAccessible(true);
		//dirPath.set(testObj, tempDir.getAbsolutePath());
		NamespaceRegistry mockReg = mock(NamespaceRegistry.class);
		NodeTypeManager mockNodeTypes = mock(NodeTypeManager.class);
		testObj.initialize(mockReg, mockNodeTypes);
		testObj.m_writerFactory = mockFactory;
		Field context = Connector.class.getDeclaredField("context");
		context.setAccessible(true);
		context.set(testObj, ExecutionContext.DEFAULT_CONTEXT);
	}
	
	@After
	public void tearDown() {
		if (testObj != null) testObj.shutdown();
		testObj = null;
		//tempDir.delete();
	}

	@Test
	public void testGetBagInfo() throws IOException {
		//new File(tempDir, "foo").mkdirs();
        //BagInfo bi = testObj.getBagInfo("/foo");
        BagInfo bi = testObj.getBagInfo("/");
		assertTrue(bi == null);
		//touch(new File(tempDir, "foo/bag-info.txt"));
        bi = testObj.getBagInfo("/package1");
		assertNotNull(bi);
	}

	/* @Test
	public void getDocumentById() throws IOException {
		new File(tempDir, "foo/data").mkdirs();
		touch(new File(tempDir, "foo/data/bar"));
		when(mockFactory.getDocumentWriter(any(String.class))).thenReturn(mockWriter);
		testObj.getDocumentById("/foo");
		verify(mockFactory).getDocumentWriter("/foo");
		verify(mockWriter).setParent(eq("/"));
		testObj.getDocumentById("/foo/bar");
		verify(mockFactory).getDocumentWriter("/foo/bar");
		verify(mockWriter).setParent(eq("/foo"));
	} */

	@Test
	public void testGetDocumentById() throws IOException {
		when(mockFactory.getDocumentWriter(any(String.class))).thenReturn(mockWriter);
        testObj.getDocumentById("/");
        verify(mockFactory).getDocumentWriter("/");
        testObj.getDocumentById("/package1");
		verify(mockFactory).getDocumentWriter("/package1");
		//verify(mockWriter).setParent(eq("/"));
		testObj.getDocumentById("/package1/testDS");
		verify(mockFactory).getDocumentWriter("/package1/testDS");
		//verify(mockWriter).setParent(eq("/package1"));
	}

	@Test
	public void testFileFor() throws IOException {
		//new File(tempDir, "foo/data").mkdirs();
		//touch(new File(tempDir, "foo/data/bar"));
		//File result = testObj.fileFor("/foo/bar");
        File result = testObj.fileFor("/package1/testDS");
		assertTrue(result.exists());
		//assertEquals(result.getParent(), tempDir.getAbsolutePath() + "/foo/data");
	}

	@Test
	public void testIdFor() throws IOException {
		//new File(tempDir, "foo/data").mkdirs();
		//File input = (new File(tempDir, "foo/data/bar"));
        File input = new File("/tmp/test-objects/group1/pairtree_root/pa/ck/ag/e1/obj/__2/data/testDS");
		String result = testObj.idFor(input);
		assertEquals(result, "/package1/testDS");
	}

	static void touch(File file) throws IOException {
		FileOutputStream out = new FileOutputStream(file);
		out.write(new byte[0]);
		out.flush();
		out.close();
	}
}

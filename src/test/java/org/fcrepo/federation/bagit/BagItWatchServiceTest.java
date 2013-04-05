package org.fcrepo.federation.bagit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

public class BagItWatchServiceTest {

	@Test
	public void testIsManifest() throws IOException {
		File input = mock(File.class);
		when(input.isFile()).thenReturn(true);
		when(input.canRead()).thenReturn(true);
		
		BagItWatchService test = new BagItWatchService();
		String fname = "not-a-manifest.txt";
		when(input.getName()).thenReturn(fname);
		assertFalse("\"" + fname + "\" should not be a valid manifest file", test.isManifest(input));
		fname = "manifest-md5.txt";
		when(input.getName()).thenReturn(fname);
		assertTrue("\"" + fname +  "\" should be a valid manifest file", test.isManifest(input));
		fname = "manifest-foobar.txt";
		when(input.getName()).thenReturn(fname);
		assertFalse("Unexpected checksum algorithm \"foobar\" returned valid manifest", test.isManifest(input));
	}
	
	@Test
	public void testIsTagManifest() throws IOException {
		File input = mock(File.class);
		when(input.isFile()).thenReturn(true);
		when(input.canRead()).thenReturn(true);
		
		BagItWatchService test = new BagItWatchService();
		String fname = "not-a-manifest.txt";
		when(input.getName()).thenReturn(fname);
		assertFalse("\"" + fname + "\" should not be a valid manifest file", test.isTagManifest(input));
		fname = "tagmanifest-md5.txt";
		when(input.getName()).thenReturn(fname);
		assertTrue("\"" + fname +  "\" should be a valid manifest file", test.isTagManifest(input));
		fname = "tagmanifest-foobar.txt";
		when(input.getName()).thenReturn(fname);
		assertFalse("Unexpected checksum algorithm \"foobar\" returned valid manifest", test.isTagManifest(input));
	}
}

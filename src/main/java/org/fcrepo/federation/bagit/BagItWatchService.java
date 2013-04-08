package org.fcrepo.federation.bagit;

import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;

public class BagItWatchService implements WatchService {
	
	private static final Logger logger = LoggerFactory.getLogger(BagItWatchService.class);
	
	static final Pattern MANIFEST = Pattern.compile("^manifest-([^\\.]+).txt$");
	static final Pattern TAG_MANIFEST = Pattern.compile("^tagmanifest-([^\\.]+).txt$");
	
	static GetFilesFromManifest getFilesFromManifest = new GetFilesFromManifest();
	
	WatchService delegate;
	
	Collection<Path> tagFiles = new ArrayList<Path>();
	
	Collection<Path> manifests = new ArrayList<Path>();
	
	BagItWatchService() throws IOException {
		delegate = FileSystems.getDefault().newWatchService();
	}
	
	/**
	 * Constructor to facilitate testing
	 * @param delegate
	 */
	BagItWatchService(WatchService delegate) {
		this.delegate = delegate;
	}
	
    public BagItWatchService(File bagItDir) throws IOException {
    	this();
    	for (File file: bagItDir.listFiles()) {
    		if (isManifest(file)) {
    			monitorManifest(file);
    		} else if (isTagManifest(file)) {
    			for (File listedFile: getFilesFromManifest.apply(file)) monitorTagFile(listedFile);
    		}
    	}
    }
    
	@Override
	public void close() throws IOException {
		delegate.close();
	}

	@Override
	public WatchKey poll() {
		return delegate.poll();
	}

	@Override
	public WatchKey poll(long timeout, TimeUnit unit)
			throws InterruptedException {
		return delegate.poll(timeout, unit);
	}

	@Override
	public WatchKey take() throws InterruptedException {
		return delegate.take();
	}
	
	public void monitorTagFile(File input) throws IOException {
		Path path = Paths.get(input.toURI());
		if (!tagFiles.contains(path)) tagFiles.add(path);
		path.register(delegate, ENTRY_MODIFY);
	}
	
	public void monitorManifest(File input) throws IOException {
		Path path = Paths.get(input.toURI());
		if (!manifests.contains(path)) manifests.add(path);
		path.register(delegate, ENTRY_MODIFY);
	}
	
	boolean isManifest(String fileName) {
		Matcher m = MANIFEST.matcher(fileName);
		if (m.find()) {
			String csa = m.group(1);
			try {
				MessageDigest.getInstance(csa);
				return true;
			} catch (NoSuchAlgorithmException e) {
				logger.warn("Ignoring potential manifest file {} because {} is not a supported checksum algorithm.", fileName, csa);
			}
		}
		return false;
	}
	
	boolean isManifest(File file) {
		if (file.isFile() && file.canRead() && !file.isHidden()) {
			return(isManifest(file.getName()));
		} else return false;
	}
	
	boolean isManifest(Path path) {
		return isManifest(path.toFile());
	}
	
	boolean isTagManifest(String fileName) {
		Matcher m = TAG_MANIFEST.matcher(fileName);
		if (m.find()) {
			String csa = m.group(1);
			try {
				MessageDigest.getInstance(csa);
				return true;
			} catch (NoSuchAlgorithmException e) {
				logger.warn("Ignoring potential tag-manifest file {} because {} is not a supported checksum algorithm.", fileName, csa);
			}
		}
		return false;
	}
	
	boolean isTagManifest(File file) {
		if (file.isFile() && file.canRead() && !file.isHidden()) {
			return isTagManifest(file.getName());
		} else return false;
	}
	
	boolean isTagManifest(Path path) {
		return isManifest(path.toFile());
	}
	
	static class GetFilesFromManifest implements Function<File, Collection<File>> {

		@Override
		public Collection<File> apply(File input) {
			try {
				ArrayList<File> result = new ArrayList<File>();
				LineNumberReader lnr = new LineNumberReader(new FileReader(input));
				String line = null;
				while((line = lnr.readLine()) != null) {
					String fileName = line.split(" ")[0];
					File file = new File(input.getParentFile(), fileName);
					result.add(file);
				}
				return result;
			} catch (IOException e) {
				e.printStackTrace();
				return null;
			}
		}
		
	}

}

package org.fcrepo.federation.bagit;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ManifestMonitor implements Runnable {
	
	private BagItConnector connector;
	
	private BagItWatchService watchService;
	
	private volatile boolean shutdown;
		
	public ManifestMonitor(BagItConnector connector) throws IOException {
		this.connector = connector;
		this.watchService = new BagItWatchService(connector.getBagItDirectory());
		this.shutdown = false;
	}

	@Override
	public void run() {
		while(!this.shutdown) {
			try {
			WatchKey key = watchService.poll(1, TimeUnit.SECONDS);
			List<WatchEvent<?>> events = key.pollEvents();
			boolean manifest = false;
			boolean tagManifest = false;
			Path path = null;
			for (WatchEvent<?> event: events) {
				Path context = (Path)event.context();
				if (watchService.isManifest(context)) {
					manifest = true;
					path = context;
				} else if (watchService.isTagManifest(context)) {
					tagManifest = true;
					path = context;
				}
			}
			if (manifest) {
				connector.changeManifest(path.toFile());
			} else if (tagManifest) {
				connector.changeTagFile(path.toFile());
			}
			} catch (InterruptedException e) {
				this.shutdown = true;
			}
		}
	}
	
	public void shutdown() {
		this.shutdown = true;
	}

}

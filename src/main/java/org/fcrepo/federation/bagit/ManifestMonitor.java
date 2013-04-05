package org.fcrepo.federation.bagit;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.util.List;

public class ManifestMonitor implements Runnable {
	
	private BagItConnector connector;
	
	private BagItWatchService watchService;
	
	private boolean run;
	
	public ManifestMonitor(BagItConnector connector) throws IOException {
		this.connector = connector;
		this.watchService = new BagItWatchService(connector.getBagItDirectory());
	}

	@Override
	public void run() {
		while(!Thread.interrupted()) {
			WatchKey key = watchService.poll();
			List<WatchEvent<?>> events = key.pollEvents();
			boolean manifest = false;
			boolean tagManifest = false;
			Path path;
			for (WatchEvent<?> event: events) {
				Path context = (Path)event.context();
				if (watchService.isManifest(context)) {
					manifest = true;
					path= context;
				} else if (watchService.isTagManifest(context)) {
					tagManifest = true;
					path= context;
				}
			}
			if (manifest) {
				//connector.manifestChanged(path.toFile());
			} else if (tagManifest) {
				//connector.tagFileChanged(path.toFile());
			}
		}
	}

}

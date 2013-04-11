
package org.fcrepo.federation.bagit;

import static org.fcrepo.utils.FedoraJcrTypes.FEDORA_OBJECT;
import static org.modeshape.jcr.api.JcrConstants.JCR_CONTENT;
import static org.modeshape.jcr.api.JcrConstants.JCR_DATA;
import static org.modeshape.jcr.api.JcrConstants.NT_FOLDER;
import static org.modeshape.jcr.api.JcrConstants.NT_RESOURCE;
import gov.loc.repository.bagit.impl.FileBagFile;
import gov.loc.repository.bagit.v0_97.impl.BagConstantsImpl;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.jcr.NamespaceRegistry;
import javax.jcr.RepositoryException;

import org.fcrepo.utils.FedoraJcrTypes;
import org.infinispan.schematic.document.Document;
import org.modeshape.connector.filesystem.FileSystemConnector;
import org.modeshape.jcr.JcrI18n;
import org.modeshape.jcr.api.JcrConstants;
import org.modeshape.jcr.api.nodetype.NodeTypeManager;
import org.modeshape.jcr.cache.DocumentStoreException;
import org.modeshape.jcr.federation.spi.DocumentChanges;
import org.modeshape.jcr.federation.spi.DocumentWriter;
import org.modeshape.jcr.value.BinaryValue;
import org.modeshape.jcr.value.PropertyFactory;
import org.modeshape.jcr.value.ValueFactories;

public class BagItConnector extends FileSystemConnector {
	
	private static final String BAGIT_ARCHIVE_TYPE = "bagit:archive";
	
    private static final String FILE_SEPARATOR = File.separator;

    private static final String JCR_PATH_DELIMITER = "/"; // NOT THE File.pathSeparator;

    private static final String JCR_LAST_MODIFIED = "jcr:lastModified";

    private static final String JCR_CREATED = "jcr:created";

    private static final String JCR_CREATED_BY = "jcr:createdBy";

    private static final String JCR_LAST_MODIFIED_BY = "jcr:lastModified";

    private static final String MIX_MIME_TYPE = "mix:mimeType";

    private static final String JCR_MIME_TYPE = "jcr:mimeType";

    private static final String JCR_ENCODING = "jcr:encoding";

    private static final String JCR_CONTENT_SUFFIX = FILE_SEPARATOR + JCR_CONTENT;

    private static final int JCR_CONTENT_SUFFIX_LENGTH = JCR_CONTENT_SUFFIX.length();

    /**
     * A boolean flag that specifies whether this connector should add the 'mix:mimeType' mixin to the 'nt:resource' nodes to
     * include the 'jcr:mimeType' property. If set to <code>true</code>, the MIME type is computed immediately when the
     * 'nt:resource' node is accessed, which might be expensive for larger files. This is <code>false</code> by default.
     */
    private boolean addMimeTypeMixin = false;

    /**
     * The string path for a {@link File} object that represents the top-level directory accessed by this connector. This is set
     * via reflection and is required for this connector.
     */
    private String directoryPath;

    private File directory;

    /**
     * A string that is created in the {@link #initialize(NamespaceRegistry, NodeTypeManager)} method that represents the absolute
     * path to the {@link #directory}. This path is removed from an absolute path of a file to obtain the ID of the node.
     */
    private String directoryAbsolutePath;

    private int directoryAbsolutePathLength;
    
    private ExecutorService threadPool;

    @Override
    public void initialize(NamespaceRegistry registry,
            NodeTypeManager nodeTypeManager) throws RepositoryException,
            IOException {
        getLogger().trace("Initializing...");
        // Initialize the directory path field that has been set via reflection when this method is called...
        checkFieldNotNull(directoryPath, "directoryPath");
        directory = new File(directoryPath);
        if (!directory.exists() || !directory.isDirectory()) {
            String msg =
                    JcrI18n.fileConnectorTopLevelDirectoryMissingOrCannotBeRead
                            .text(getSourceName(), "directoryPath");
            throw new RepositoryException(msg);
        }
        if (!directory.canRead() && !directory.setReadable(true)) {
            String msg =
                    JcrI18n.fileConnectorTopLevelDirectoryMissingOrCannotBeRead
                            .text(getSourceName(), "directoryPath");
            throw new RepositoryException(msg);
        }
        directoryAbsolutePath = directory.getAbsolutePath();
        getLogger().debug(
                "Using filesystem directory: " + directoryAbsolutePath);
        if (!directoryAbsolutePath.endsWith(FILE_SEPARATOR))
            directoryAbsolutePath = directoryAbsolutePath + FILE_SEPARATOR;
        directoryAbsolutePathLength =
                directoryAbsolutePath.length() - FILE_SEPARATOR.length(); // does NOT include the separator

        setExtraPropertiesStore(new BagItExtraPropertiesStore(this));
        getLogger().trace("Initialized.");
        BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<Runnable>(1);
        threadPool = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, workQueue);
        getLogger().trace("Threadpool initialized.");
        threadPool.execute(new ManifestMonitor(this));
        getLogger().trace("Monitor thread queued.");
    }
    
    @Override
    public void shutdown() {
    	threadPool.shutdown();
        getLogger().trace("Threadpool shutdown.");
    }
    
    @Override
    public Document getDocumentById(String id) {
        getLogger().trace("Entering getDocumentById()...");
        getLogger().debug("Received request for document: " + id);
        final File file = fileFor(id);
        if (isExcluded(file) || !file.exists()) return null;
        final boolean isRoot = isRoot(id);
        final boolean isResource = isContentNode(id);
        final DocumentWriter writer = newDocument(id);
        File parentFile = file.getParentFile();
        if (isRoot) {
            getLogger().debug(
                    "Determined document: " + id + " to be the projection root.");
            writer.setPrimaryType(NT_FOLDER);
            writer.addMixinType(FEDORA_OBJECT);
            writer.addProperty(JCR_CREATED, factories().getDateFactory()
                    .create(file.lastModified()));
            writer.addProperty(JCR_LAST_MODIFIED, factories().getDateFactory()
                    .create(file.lastModified()));
            writer.addProperty(JCR_CREATED_BY, null); // ignored
            for (File child : file.listFiles()) {
                // Only include as a datastream if we can access and read the file. Permissions might prevent us from
                // reading the file, and the file might not exist if it is a broken symlink (see MODE-1768 for details).
                if (child.exists() && child.canRead() &&
                        (child.isFile() || child.isDirectory())) {
                    // We use identifiers that contain the file/directory name ...
                    String childName = child.getName();
                    String childId =
                            isRoot ? FILE_SEPARATOR + childName : id + FILE_SEPARATOR +
                                    childName;
                    writer.addChild(childId, childName);
                }
            }
        } else if (isResource) {
            getLogger().debug(
                    "Determined document: " + id + " to be a binary resource.");
            BinaryValue binaryValue = binaryFor(file);
            writer.setPrimaryType(NT_RESOURCE);
            writer.addProperty(JCR_DATA, binaryValue);
            if (addMimeTypeMixin) {
                writer.addMixinType(MIX_MIME_TYPE);
                String mimeType = null;
                String encoding = null; // We don't really know this
                try {
                    mimeType = binaryValue.getMimeType();
                } catch (Throwable e) {
                    getLogger().error(e, JcrI18n.couldNotGetMimeType,
                            getSourceName(), id, e.getMessage());
                }
                writer.addProperty(JCR_ENCODING, encoding);
                writer.addProperty(JCR_MIME_TYPE, mimeType);
            }
            writer.addProperty(JCR_LAST_MODIFIED, factories().getDateFactory()
                    .create(file.lastModified()));
            writer.addProperty(JCR_LAST_MODIFIED_BY, null); // ignored

            //make these binary not queryable. If we really want to query them, we need to switch to external binaries
            writer.setNotQueryable();
            parentFile = file;
        } else if (file.isFile()) {
            getLogger().debug(
                    "Determined document: " + id + " to be a datastream.");
            writer.setPrimaryType(JcrConstants.NT_FILE);
            writer.addMixinType(FedoraJcrTypes.FEDORA_DATASTREAM);
            writer.addProperty(JCR_CREATED, factories().getDateFactory()
                    .create(file.lastModified()));
            writer.addProperty(JCR_LAST_MODIFIED, factories().getDateFactory()
                    .create(file.lastModified()));
            try {
                writer.addProperty(JCR_CREATED_BY, Files
                        .getOwner(file.toPath()).getName());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            String childId =
                    isRoot ? JCR_CONTENT_SUFFIX : id + JCR_CONTENT_SUFFIX;
            writer.addChild(childId, JCR_CONTENT);
        } else {
            getLogger().debug(
                    "Determined document: " + id + " to be a Fedora object.");
            final File dataDir =
                    new File(file.getAbsolutePath() + FILE_SEPARATOR + "data");
            getLogger().debug("searching data dir " + 
                    dataDir.getAbsolutePath());
            writer.setPrimaryType(NT_FOLDER);
            writer.addMixinType(FEDORA_OBJECT);
            writer.addMixinType(BAGIT_ARCHIVE_TYPE);
            writer.addProperty(JCR_CREATED, factories().getDateFactory()
                    .create(file.lastModified()));
            writer.addProperty(JCR_LAST_MODIFIED, factories().getDateFactory()
                    .create(file.lastModified()));
            writer.addProperty(JCR_CREATED_BY, null); // ignored
            // get datastreams as children
            for (File child : dataDir.listFiles()) {
                // Only include as a datastream if we can access and read the file. Permissions might prevent us from
                // reading the file, and the file might not exist if it is a broken symlink (see MODE-1768 for details).
                if (child.exists() && child.canRead() &&
                        (child.isFile() || child.isDirectory())) {
                    // We use identifiers that contain the file/directory name ...
                    String childName = child.getName();
                    String childId =
                            isRoot ? FILE_SEPARATOR + childName : id + FILE_SEPARATOR +
                                    childName;
                    writer.addChild(childId, childName);
                }
            }
        }

        if (!isRoot) {
            // Set the reference to the parent ...
        	String parentId = idFor(parentFile);
        	writer.setParents(parentId);
        }

        // Add the extra properties (if there are any), overwriting any properties with the same names
        // (e.g., jcr:primaryType, jcr:mixinTypes, jcr:mimeType, etc.) ...
        writer.addProperties(new BagItExtraPropertiesStore(this).getProperties(id));
        getLogger().trace("Leaving getDocumentById().");
        return writer.document();
    }

    @Override
    public void storeDocument(Document document) {
        // TODO Auto-generated method stub
        getLogger().debug("storeDocument(" + document.toString() + ")");
    }

    @Override
    public void updateDocument(DocumentChanges documentChanges) {
        // TODO Auto-generated method stub
    	getLogger().debug("updateDocument(" + documentChanges.toString() + ")");
    }
    
    File getBagItDirectory() {
    	return this.directory;
    }
    
    void changeManifest(File file) {
    	// do some manifest stuff
    }
    
    void changeTagFile(File file) {
    	// do some tagFile stuff
    }

    @Override
    protected File fileFor( String id ) {
        assert id.startsWith(JCR_PATH_DELIMITER);
        if (id.endsWith(JCR_PATH_DELIMITER)) {
            id = id.substring(0, id.length() - JCR_PATH_DELIMITER.length());
        }
        if (isContentNode(id)) {
            id = id.substring(0, id.length() - JCR_CONTENT_SUFFIX_LENGTH);
        }
    	if ("".equals(id)) return this.directory; // root node
    	
        if (isContentNode(id)) {
            id = id.substring(0, id.length() - JCR_CONTENT_SUFFIX_LENGTH);
        }
        // /{bagId}/{dsId}(/{jcr:content})?
        Pattern p = Pattern.compile("^(\\/[^\\/]+)(\\/[^\\/]+)");
        Matcher m = p.matcher(id);
        if (m.find()) {
        	id = id.replace(m.group(1), m.group(1) + FILE_SEPARATOR + "data");
        }

    	File result = new File(this.directory, id.replaceAll(JCR_PATH_DELIMITER, FILE_SEPARATOR));
    	getLogger().debug(result.getAbsolutePath());
        //return super.fileFor(id);
    	return result;
    }
    
    protected File bagInfoFileFor(String id) {
        File dir = fileFor(id);
        File result = new File(dir, "bag-info.txt");
        return (result.exists()) ? result : null;
    }
    
    @Override
    protected boolean isExcluded(File file) {
    	return !file.exists();
    }
    
    @Override
    /**
     * DIRECTLY COPIED UNTIL WE SORT OUT HOW TO EFFECTIVELY SUBCLASS
     * Utility method for determining the node identifier for the supplied file. Subclasses may override this method to change the
     * format of the identifiers, but in that case should also override the {@link #fileFor(String)},
     * {@link #isContentNode(String)}, and {@link #isRoot(String)} methods.
     *
     * @param file the file; may not be null
     * @return the node identifier; never null
     * @see #isRoot(String)
     * @see #isContentNode(String)
     * @see #fileFor(String)
     */
    protected String idFor( File file ) {
        String path = file.getAbsolutePath();
        if (!path.startsWith(directoryAbsolutePath)) {
            if (directory.getAbsolutePath().equals(path)) {
                // This is the root
                return JCR_PATH_DELIMITER;
            }
            String msg = JcrI18n.fileConnectorNodeIdentifierIsNotWithinScopeOfConnector.text(getSourceName(), directoryPath, path);
            throw new DocumentStoreException(path, msg);
        }
        String id = path.substring(directoryAbsolutePathLength);
        id = id.replace("/data/", "/"); // data dir should be removed from the id of a DS node
        if (id.endsWith("/data")) id = id.substring(0, id.length() - 5); // might also be the parent file of a DS node
        id = id.replaceAll(Pattern.quote(FILE_SEPARATOR), JCR_PATH_DELIMITER);
        if ("".equals(id)) id = JCR_PATH_DELIMITER;
        assert id.startsWith(JCR_PATH_DELIMITER);
        return id;
    }
    
    protected ValueFactories getValueFactories() {
    	return getContext().getValueFactories();
    }
    
    protected PropertyFactory getPropertyFactory() {
    	return getContext().getPropertyFactory();
    }
    
    protected BagInfo getBagInfo(String id) {
    	File bagInfoFile = bagInfoFileFor(id);
    	if (bagInfoFile == null) return null;
    	// really need to get the version from bagit.txt, but start with hard-coding
    	ValueFactories vf = getValueFactories();
    	BagInfo result = 
    			new BagInfo(id, new FileBagFile(bagInfoFile.getAbsolutePath(), bagInfoFile),
    					getPropertyFactory(), vf.getNameFactory(), new BagConstantsImpl());
    	return result;
    }
}

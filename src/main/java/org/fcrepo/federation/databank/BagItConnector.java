
package org.fcrepo.federation.databank;

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
import java.io.FileReader;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.jcr.NamespaceRegistry;
import javax.jcr.RepositoryException;

import org.apache.poi.util.TempFile;
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

import static org.fcrepo.utils.FedoraJcrTypes.FEDORA_OBJECT;
import static org.modeshape.jcr.api.JcrConstants.*;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class BagItConnector extends FileSystemConnector {
	
	private static final String BAGIT_ARCHIVE_TYPE = "bagit:archive";
	
    private static final char JCR_PATH_DELIMITER_CHAR = '/'; // NOT THE File.pathSeparator;
	
    private static final String FILE_SEPARATOR = File.separator;

    private static final String JCR_PATH_DELIMITER = "/"; // NOT THE File.pathSeparator;

    private static final String JCR_LAST_MODIFIED = "jcr:lastModified";

    private static final String JCR_CREATED = "jcr:created";

    private static final String JCR_CREATED_BY = "jcr:createdBy";

    private static final String JCR_LAST_MODIFIED_BY = "jcr:lastModified";

    private static final String MIX_MIME_TYPE = "mix:mimeType";

    private static final String JCR_MIME_TYPE = "jcr:mimeType";

    private static final String JCR_ENCODING = "jcr:encoding";

    //TODO: This should be passed in by ID and not hardcoded
    private static final String SILO_NAME = "group1";

    private static final String JCR_CONTENT_SUFFIX = JCR_PATH_DELIMITER + JCR_CONTENT;

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

    // it appears to be the case that bootstrapping the federated nodes results in a pre-init call to the connector
    // so this is a dummy file for that situation
    private File m_directory = TempFile.createTempFile("stub", "stub");

    /**
     * A string that is created in the {@link #initialize(NamespaceRegistry, NodeTypeManager)} method that represents the absolute
     * path to the {@link #m_directory}. This path is removed from an absolute path of a file to obtain the ID of the node.
     */
    private String directoryAbsolutePath;

    private int directoryAbsolutePathLength;
    
    private ExecutorService threadPool;
    
    DocumentWriterFactory m_writerFactory;
    
    public void setDirectoryPath(String directoryPath) {
    	this.directoryPath = directoryPath;
    	m_directory = new File(directoryPath);
    }
    
    public void setDirectory(File directory) {
    	m_directory = directory;
    	this.directoryPath = directory.getAbsolutePath();
    }

    @Override
    public void initialize(NamespaceRegistry registry,
            NodeTypeManager nodeTypeManager) throws RepositoryException,
            IOException {
        getLogger().trace("Initializing at " + this.directoryPath + " ...");
        // Initialize the directory path field that has been set via reflection when this method is called...
        m_writerFactory = new DocumentWriterFactory(translator());
        checkFieldNotNull(directoryPath, "directoryPath");
        m_directory = new File(directoryPath);
        if (!m_directory.exists() || !m_directory.isDirectory()) {
            String msg =
                    JcrI18n.fileConnectorTopLevelDirectoryMissingOrCannotBeRead
                            .text(getSourceName(), "directoryPath");
            throw new RepositoryException(msg);
        }
        if (!m_directory.canRead() && !m_directory.setReadable(true)) {
            String msg =
                    JcrI18n.fileConnectorTopLevelDirectoryMissingOrCannotBeRead
                            .text(getSourceName(), "directoryPath");
            throw new RepositoryException(msg);
        }
        directoryAbsolutePath = m_directory.getAbsolutePath();
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
        //TODO: projection root hard codes the silo name (silo_name/pairtree_root).
        // in order to avoid dealing with hierarchies in Modeshape.
        // silo name should be passed in the id??
        // Assumed silo to be group1 in test
        getLogger().trace("Entering getDocumentById()...");
        getLogger().debug("Received request for document: " + id);
        final File file = fileFor(id);
        getLogger().debug("Received request for document: " + id + ", resolved to " + file);
        if (file == null || isExcluded(file) || !file.exists()) return null;
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
            writer.addMixinType(FedoraJcrTypes.FEDORA_OWNED);
            writer.addMixinType(FedoraJcrTypes.FEDORA_DATASTREAM);
            writer.addProperty(JCR_CREATED, factories().getDateFactory()
                    .create(file.lastModified()));
            writer.addProperty(JCR_LAST_MODIFIED, factories().getDateFactory()
                    .create(file.lastModified()));
            try {
            	String owner = Files
                        .getOwner(file.toPath()).getName();
                writer.addProperty(JCR_CREATED_BY, owner);
                writer.addProperty(FedoraJcrTypes.FEDORA_OWNERID, owner);
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
            getLogger().debug("searching data dir " + dataDir.getAbsolutePath());
            writer.setPrimaryType(NT_FOLDER);
            writer.addMixinType(FEDORA_OBJECT);
            writer.addMixinType(FedoraJcrTypes.FEDORA_OWNED);
            writer.addMixinType(BAGIT_ARCHIVE_TYPE);
            writer.addProperty(JCR_CREATED, factories().getDateFactory()
                    .create(file.lastModified()));
            writer.addProperty(JCR_LAST_MODIFIED, factories().getDateFactory()
                    .create(file.lastModified()));
            try {
        	String owner = Files
                    .getOwner(file.toPath()).getName();
            writer.addProperty(JCR_CREATED_BY, owner); // required
            writer.addProperty(FedoraJcrTypes.FEDORA_OWNERID, owner);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            writer.addProperty(FedoraJcrTypes.DC_IDENTIFIER, id);
            // get datastreams as children
            //try {
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
            /*} catch (Throwable e) {
                getLogger().error(e, JcrI18n.childNotFoundUnderNode,
                        getSourceName(), id, e.getMessage());
            }*/
        }

        if (!isRoot) {
            // Set the reference to the parent ...
        	String parentId = idFor(parentFile);
        	writer.setParent(parentId);
        }

        // Add the extra properties (if there are any), overwriting any properties with the same names
        // (e.g., jcr:primaryType, jcr:mixinTypes, jcr:mimeType, etc.) ...
        writer.addProperties(new BagItExtraPropertiesStore(this).getProperties(id));
        getLogger().trace("Leaving getDocumentById().");
        return writer.document();
    }
    
    @Override
    public DocumentWriter newDocument(String id) {
    	return m_writerFactory.getDocumentWriter(id);
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
    	return this.m_directory;
    }
    
    void changeManifest(File file) {
    	// do some manifest stuff
    }
    
    void changeTagFile(File file) {
    	// do some tagFile stuff
    }

    /*
    Method to determine the storage location on disk
    for a given id.
    id may point to the root or to an object or to an object datastream
     */
    @Override
    protected File fileFor( String id ) {
        //Removing delimiters
        assert id.startsWith(JCR_PATH_DELIMITER);
        if (id.endsWith(JCR_PATH_DELIMITER)) {
            id = id.substring(0, id.length() - JCR_PATH_DELIMITER.length());
        }
        if (isContentNode(id)) {
            id = id.substring(0, id.length() - JCR_CONTENT_SUFFIX_LENGTH);
        }
    	if ("".equals(id)){
    		getLogger().debug("#fileFor returning root directory for \"" + id + "\"");
    		return this.m_directory; // root node
    	}

        // /{bagId}/{dsId}(/{jcr:content})?
        // Retrieving object id to be made pairtree
        Pattern p1 = Pattern.compile("^(\\/[^\\/]+)(\\/[^\\/]+)");
        Matcher m1 = p1.matcher(id);
        Pattern p2 = Pattern.compile("^(\\/[^\\/]+)");
        Matcher m2 = p2.matcher(id);
        if (m2.find()) {
            // object id to pairtree
            getLogger().debug("Extracting object " + m2.group(1));
            String pairtree = getPairtreePath(m2.group(1), null);
            id = pairtree;
        }
        if (m1.find()) {
            // Adding datastream id to it
            getLogger().debug("Extracting datastream " + m1.group(2));
            id = id + FILE_SEPARATOR + "data" + m1.group(2);
        }
    	File result = new File(this.m_directory, id.replaceAll(JCR_PATH_DELIMITER, FILE_SEPARATOR));
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
    	//TODO this should check the data manifest
        //     is this embargo info - manifest.rdf in databank??
    	return file == null || !file.exists();
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
            if (m_directory.getAbsolutePath().equals(path)) {
                // This is the root
                return JCR_PATH_DELIMITER;
            }
            String msg = JcrI18n.fileConnectorNodeIdentifierIsNotWithinScopeOfConnector.text(getSourceName(), directoryPath, path);
            throw new DocumentStoreException(path, msg);
        }
        String id = path.substring(directoryAbsolutePathLength);

        //Split string at /obj - before /obj points to object id, after to version number and datastream
        String[] parts;
        if (id.contains(FILE_SEPARATOR + "obj")) {
            // Split it.
            parts = id.split(Pattern.quote(FILE_SEPARATOR) + "obj");
        } else {
            //There is no obj dir in path. Throw exception
            String msg = JcrI18n.fileConnectorNodeIdentifierIsNotWithinScopeOfConnector.text(getSourceName(), directoryPath, id);
            throw new DocumentStoreException(path, msg);
        }
        String objId = parts[0].replaceAll(Pattern.quote(FILE_SEPARATOR), "");
        getLogger().debug("object Id from pairtree path " + objId);
        String dsId;
        if (parts.length == 2 && parts[1].startsWith(FILE_SEPARATOR + "__")) {
            Pattern p = Pattern.compile("^(\\/__[^\\/]+)");
            Matcher m = p.matcher(parts[1]);
            if (m.find()) {
                dsId =  parts[1].replace(m.group(1), "");
                getLogger().debug("Datastream part of pairtree path " + dsId);
                id = FILE_SEPARATOR + objId + dsId;

            } else {
                id = FILE_SEPARATOR + objId;
            }
        } else {
            id = FILE_SEPARATOR + objId;
        }
        id = id.replace("/data/", "/"); // data dir should be removed from the id of a DS node
        if (id.endsWith("/data")) id = id.substring(0, id.length() - 5); // might also be the parent file of a DS node
        id = id.replaceAll(Pattern.quote(FILE_SEPARATOR), JCR_PATH_DELIMITER);
        if ("".equals(id)) id = JCR_PATH_DELIMITER;
        assert id.startsWith(JCR_PATH_DELIMITER);
        getLogger().debug("id from path is " + id);
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

    /*
    Method to obtain the pairtree path on disk for a given object id or datastream id
     */
    protected String getPairtreePath(String objId, String version) {
        //Remove file separators from object id
        if (objId.endsWith(FILE_SEPARATOR)) {
            objId = objId.substring(0, objId.length() - FILE_SEPARATOR.length());
        }
        if (objId.startsWith(FILE_SEPARATOR)) {
            objId = objId.substring(FILE_SEPARATOR.length(), objId.length());
        }
        // Convert it to pairtree strucure
        String[] pairtree_id = objId.split("(?<=\\G.{2})");
        StringBuilder sb = new StringBuilder();
        for (String s : pairtree_id) {
            sb.append(s);
            sb.append(FILE_SEPARATOR);
        }
        // Append obj to pairtree structure
        objId = sb.toString();
        objId =  FILE_SEPARATOR + objId + "obj";
        getLogger().debug("Pairtree base path for object " + objId);

        File json_manifest = new File(this.m_directory, objId + FILE_SEPARATOR + "__manifest.json");
        if (!json_manifest.exists() || !json_manifest.isFile()) {
            String msg =
                    JcrI18n.fileDoesNotExist.text(getSourceName(), "directoryPath");
            throw new DocumentStoreException(json_manifest.getAbsolutePath(), msg);
        }
        if (version == null) {
            // Version not passed. Get the latest version of the package
            version = getCurrentVersion(json_manifest.getAbsolutePath());
        }
        if (version != null) {
            objId = objId + FILE_SEPARATOR + "__" + version;
        }
        File version_directory = new File(this.m_directory, objId);
        if (!version_directory.exists() || !version_directory.isDirectory()) {
            String msg =
                    JcrI18n.fileDoesNotExist.text(getSourceName(), "directoryPath");
            throw new DocumentStoreException(version_directory.getAbsolutePath(), msg);
        }
        getLogger().debug("Pairtree path for object " + objId);
        return objId;
    }

    /*
    Method to get the current version of the data package from
    __manifest.json located at the base of the object dir
     */
    protected String getCurrentVersion(String manifest_path) {
        //System.out.println("JSON Path " + manifest_path + FILE_SEPARATOR + "__manifest.json");
        JSONParser parser = new JSONParser();
        JSONObject manifest = null;
        String version = null;
        try {
            manifest = (JSONObject) parser.parse(new FileReader(manifest_path));
            version = (String) manifest.get("currentversion");
        } catch (IOException e) {
            //e.printStackTrace();
            getLogger().debug("__manifest.json not available in " + manifest_path);
            return version;
        } catch (ParseException e) {
            //e.printStackTrace();
            getLogger().debug("Error parsing __manifest.json in " + manifest_path);
            return version;
        }
        getLogger().debug("current Version of object " + version);
        return version;
    }

}

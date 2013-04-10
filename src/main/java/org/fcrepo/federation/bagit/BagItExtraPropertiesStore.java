
package org.fcrepo.federation.bagit;

import static org.slf4j.LoggerFactory.getLogger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Map;

import org.modeshape.jcr.cache.DocumentStoreException;
import org.modeshape.jcr.federation.spi.ExtraPropertiesStore;
import org.modeshape.jcr.value.Name;
import org.modeshape.jcr.value.Property;
import org.modeshape.jcr.value.PropertyFactory;
import org.modeshape.jcr.value.ValueFactories;
import org.modeshape.jcr.value.ValueFactory;
import org.slf4j.Logger;

import com.google.common.collect.ImmutableMap;

/**
 * ExtraPropertiesStore implementation which stores properties in bag-info.txt.
 * @see https://tools.ietf.org/html/draft-kunze-bagit-08#section-2.2.2
**/
public class BagItExtraPropertiesStore implements ExtraPropertiesStore {

    private static final Logger logger =
            getLogger(BagItExtraPropertiesStore.class);

    private BagItConnector connector;

    private ValueFactories factories;

    private ValueFactory<String> stringFactory;

    private PropertyFactory propertyFactory;

    protected BagItExtraPropertiesStore(BagItConnector connector) {
        this.connector = connector;
        this.factories = this.connector.getContext().getValueFactories();
        this.stringFactory = factories.getStringFactory();
        this.propertyFactory = this.connector.getContext().getPropertyFactory();
    }

    @Override
    public void storeProperties(String id, Map<Name, Property> properties) {
        File bagInfo = bagInfoFile(id);
        bagInfo.delete();
        try (PrintWriter out = new PrintWriter(new FileWriter(bagInfo))) {
            for (Map.Entry<Name, Property> entry : properties.entrySet()) {
                Name name = entry.getKey();
                Property prop = entry.getValue();
                String line =
                        stringFactory.create(name) + ": " +
                                stringFactory.create(prop);
                out.println(wrapLine(line));
            }
        } catch (Exception ex) {
            throw new DocumentStoreException(
                    "Error in storing properties for " + id + " at " +
                            connector.fileFor(id), ex);
        }
    }

    @Override
    public void updateProperties(String id, Map<Name, Property> properties) {
        Map<Name, Property> existing = getProperties(id);
        for (Map.Entry<Name, Property> entry : properties.entrySet()) {
            Name name = entry.getKey();
            Property prop = entry.getValue();
            if (prop == null) {
                existing.remove(name);
            } else {
                existing.put(name, prop);
            }
        }
        storeProperties(id, existing);
    }

    @Override
    public Map<Name, Property> getProperties(String id) {
        ImmutableMap.Builder<Name, Property> properties =
                ImmutableMap.builder();
        File bagInfo = bagInfoFile(id);
        if (bagInfo.exists()) {
        try {
            try (BufferedReader buf =
                    new BufferedReader(new FileReader(bagInfo))) {

                logger.debug("Operating on bagInfoFile(" + id + "):" +
                        bagInfo.getAbsolutePath());
                if (!bagInfo.exists()) {
                    return NO_PROPERTIES;
                } else if (!bagInfo.canRead()) {
                    throw new DocumentStoreException("id", "Can't read " +
                            bagInfo.getAbsolutePath());
                };
                String key = null;
                String val = null;
                for (String line = null; (line = buf.readLine()) != null;) {
                    if (key != null &&
                            (line.startsWith(" ") || line.startsWith("\t"))) {
                        // continuation of previous line
                        if (val == null) {
                            val = line.trim();
                        } else {
                            val += " " + line.trim();
                        }
                    } else {
                        // process completed value
                        if (key != null && val != null) {
                            Name name = factories.getNameFactory().create("info:fedora/bagit/", key.replace('-', '.'));
                            properties.put(name, makeProperty(name, val));
                        }
                        key = null;
                        val = null;

                        // start new value
                        if (line.indexOf(":") != -1) {
                            key = line.substring(0, line.indexOf(":")).trim();
                            val = line.substring(line.indexOf(":") + 1).trim();
                        }
                    }
                }
                if (key != null && val != null) {
                    Name name = factories.getNameFactory().create("info:fedora/bagit/", key.replace('-', '.'));
                    properties.put(name, makeProperty(name, val));
                }
            }
        } catch (Exception ex) {
            throw new DocumentStoreException(id, ex);
        }
        }
        return properties.build();
    }

    private Property makeProperty(Name name, String value) {
        return propertyFactory.create(name, value);
    }

    @Override
    public boolean removeProperties(String id) {
        File bagInfo = bagInfoFile(id);
        if (!bagInfo.exists()) {
            return false;
        } else {
            return bagInfo.delete();
        }
    }

    private File bagInfoFile(String id) {
        File dir = connector.fileFor(id);
        return new File(dir, "bag-info.txt");
    }

    private static String wrapLine(String value) {
        if (value == null || value.length() < 79) {
            return value;
        }
        StringBuffer wrapped = new StringBuffer();
        String[] words = value.split(" ");
        StringBuffer line = new StringBuffer(words[0]);
        for (int i = 1; i < words.length; i++) {
            if (words[i].length() + line.length() < 79) {
                line.append(" " + words[i]);
            } else {
                wrapped.append(line.toString() + "\n");
                line.setLength(0);
                line.append("     " + words[i]);
            }
        }
        if (line.length() > 0) {
            wrapped.append(line.toString());
        }
        return wrapped.toString();
    }

}

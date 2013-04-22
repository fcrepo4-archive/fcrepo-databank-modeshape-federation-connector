
package org.fcrepo.federation.databank;

import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.util.Map;

import org.modeshape.jcr.cache.DocumentStoreException;
import org.modeshape.jcr.federation.spi.ExtraPropertiesStore;
import org.modeshape.jcr.value.Name;
import org.modeshape.jcr.value.Property;
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
    
    protected static final Map<Name, Property> EMPTY = emptyMap();
    
    protected BagItExtraPropertiesStore(BagItConnector connector) {
        this.connector = connector;
    }

    @Override
    public void storeProperties(String id, Map<Name, Property> properties) {
        storeProperties(connector.getBagInfo(id), properties);
    }
    
    private void storeProperties(BagInfo bagInfo, Map<Name, Property> properties) {
    	if (bagInfo == null) return;
    	
    	try{
    		bagInfo.setProperties(properties);
    		bagInfo.save();
        } catch (Exception ex) {
            throw new DocumentStoreException(
                    "Error in storing properties for " + bagInfo.bagID + " at " +
                            bagInfo.getFilepath(), ex);
        }
    }

    @Override
    public void updateProperties(String id, Map<Name, Property> properties) {
        BagInfo bagInfo = connector.getBagInfo(id);
        if (bagInfo == null) return;
        Map<Name, Property> existing = bagInfo.getProperties();
        for (Map.Entry<Name, Property> entry : properties.entrySet()) {
            Name name = entry.getKey();
            Property prop = entry.getValue();
            if (prop == null) {
                existing.remove(name);
            } else {
                existing.put(name, prop);
            }
        }
        storeProperties(bagInfo, existing);
    }

    @Override
    public Map<Name, Property> getProperties(String id) {

        BagInfo bagInfo = connector.getBagInfo(id);
        if (bagInfo == null) {
            logger.debug("No bag-info.txt for " + id);
        	return EMPTY;
        }
        logger.debug("Operating on bagInfoFile(" + id + "):" +
                bagInfo.getFilepath());
        try {
            return bagInfo.getProperties();
        } catch (Exception ex) {
            throw new DocumentStoreException(id, ex);
        }
    }

    @Override
    public boolean removeProperties(String id) {
        BagInfo bagInfo = connector.getBagInfo(id);
        if (!bagInfo.exists()) {
            return false;
        } else {
            try {
				boolean result = bagInfo.delete();
				bagInfo.save();
				return result;
			} catch (IOException ex) {
	            throw new DocumentStoreException(
	                    "Error in removing properties for " + bagInfo.bagID + " at " +
	                            bagInfo.getFilepath(), ex);
			}
        }
    }
    
    private static final Map<Name, Property> emptyMap() {
        ImmutableMap.Builder<Name, Property> properties =
                ImmutableMap.builder();
        return properties.build();
    }

}

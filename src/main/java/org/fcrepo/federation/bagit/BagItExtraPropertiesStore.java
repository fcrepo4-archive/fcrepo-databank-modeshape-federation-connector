
package org.fcrepo.federation.bagit;

import java.util.Map;

import org.modeshape.jcr.federation.spi.ExtraPropertiesStore;
import org.modeshape.jcr.value.Name;
import org.modeshape.jcr.value.Property;

public class BagItExtraPropertiesStore implements ExtraPropertiesStore {

    @Override
    public void storeProperties(String id, Map<Name, Property> properties) {
        // TODO Auto-generated method stub
    }

    @Override
    public void updateProperties(String id, Map<Name, Property> properties) {
        // TODO Auto-generated method stub

    }

    @Override
    public Map<Name, Property> getProperties(String id) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean removeProperties(String id) {
        // TODO Auto-generated method stub
        return false;
    }

}

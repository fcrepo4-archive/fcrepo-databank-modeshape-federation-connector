
package org.fcrepo.federation.bagit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.modeshape.jcr.value.Name;
import org.modeshape.jcr.value.Property;
import org.modeshape.jcr.value.PropertyFactory;
import org.modeshape.jcr.value.ValueFactories;


public class BagItExtraPropertiesStoreTest {

    BagItExtraPropertiesStore store;
    
    BagItConnector connector;

    @Before
    public void setUp() {
        connector = mock(BagItConnector.class);
        ValueFactories values = mock(ValueFactories.class);
        PropertyFactory properties = mock(PropertyFactory.class);
        when(connector.getValueFactories()).thenReturn(values);
        when(connector.getPropertyFactory()).thenReturn(properties);
        store = new BagItExtraPropertiesStore(connector);
    }

    @Test
    public void testRead() {
        // TODO read a properties file from disk
        Map<Name, Property> props = store.getProperties("/foo");
        verify(connector).getBagInfo("/foo");        
    }

    /*

    @Test
    public void testWrite() {
        // TODO write a properties file to disk then read it back
    }

    @Test
    public void testUpdate() {
        // TODO update a properties file and make sure new props saved, delete dprops removed
    }

    @Test
    public void testRemove() {
        // TODO remove a properties file
    }
*/
}

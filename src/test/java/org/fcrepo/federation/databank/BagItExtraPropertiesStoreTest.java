
package org.fcrepo.federation.databank;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Map;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.matchers.Equals;
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
        BagInfo mockBI = getMockBagInfo();
        when(connector.getBagInfo("/foo")).thenReturn(mockBI);
        Map<Name, Property> props = store.getProperties("/foo");
        verify(connector).getBagInfo("/foo");
        verify(mockBI).getProperties();
        props = store.getProperties("/non/existent");
        assertEquals(BagItExtraPropertiesStore.EMPTY, props);
    }
    
    @Test
    public void testUpdateProperties() throws IOException {
        BagInfo mockBI = getMockBagInfo();
        when(connector.getBagInfo("/foo")).thenReturn(mockBI);
        Map<Name, Property> mockProps = mock(Map.class);
        store.updateProperties("/foo", mockProps);
        verify(connector).getBagInfo("/foo");
        verify(mockBI).setProperties(any(Map.class));
        verify(mockBI).save();
        verify(mockBI).getProperties();
    }

    @Test
    public void testStoreProperties() throws IOException {
        BagInfo mockBI = getMockBagInfo();
        when(connector.getBagInfo("/foo")).thenReturn(mockBI);
        Map<Name, Property> mockProps = mock(Map.class);
        store.storeProperties("/foo", mockProps);
        verify(connector).getBagInfo("/foo");
        verify(mockBI).setProperties(any(Map.class));
        verify(mockBI).save();
    }


    @Test
    public void testRemove() throws IOException {
        BagInfo mockBI = getMockBagInfo();
        when(mockBI.exists()).thenReturn(true);
        when(connector.getBagInfo("/foo")).thenReturn(mockBI);
        Map<Name, Property> mockProps = mock(Map.class);
        store.removeProperties("/foo");
        verify(connector).getBagInfo("/foo");
        verify(mockBI).delete();
        verify(mockBI).save();
    }

    private BagInfo getMockBagInfo() {
    	BagInfo mock = mock(BagInfo.class);
    	return mock;
    }

}

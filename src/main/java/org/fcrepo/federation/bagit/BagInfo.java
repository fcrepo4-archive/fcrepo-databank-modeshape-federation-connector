
package org.fcrepo.federation.bagit;

import gov.loc.repository.bagit.Bag.BagConstants;
import gov.loc.repository.bagit.BagFile;
import gov.loc.repository.bagit.BagInfoTxtWriter;
import gov.loc.repository.bagit.impl.BagInfoTxtImpl;
import gov.loc.repository.bagit.utilities.namevalue.NameValueReader.NameValue;

import java.io.IOException;
import java.util.Map;

import org.fcrepo.federation.bagit.functions.GetBagInfoTxtWriter;
import org.modeshape.jcr.value.Name;
import org.modeshape.jcr.value.NameFactory;
import org.modeshape.jcr.value.Property;
import org.modeshape.jcr.value.PropertyFactory;

import com.google.common.collect.ImmutableMap;

public class BagInfo extends BagInfoTxtImpl {

    /**
     * The ID under which this bag is stored.
     */
    public String bagID;
    
    private PropertyFactory m_propertyFactory;
    
    private NameFactory m_nameFactory;
        
    private static final long serialVersionUID = 1L;
    
    static GetBagInfoTxtWriter getBagInfoTxtWriter = new GetBagInfoTxtWriter();

    public BagInfo(
    		String bagID, BagFile bagFile, PropertyFactory propertyFactory,
    		NameFactory nameFactory, BagConstants bagConstants) {
        super(bagFile, bagConstants);
        this.bagID = bagID;
        m_propertyFactory = propertyFactory;
        m_nameFactory = nameFactory;
    }
    
    /**
     * Stores this bag-info.txt into its bag.
     */
    public void save() throws IOException {
    	;
        try (BagInfoTxtWriter writer = getBagInfoTxtWriter.apply(this.getFilepath())) {
            Map<Name, Property> properties = getProperties();
        	for (Property jcrProp : properties.values()) {
                NameValue prop = toBagitProperty(jcrProp);
                writer.write(prop.getName(), prop.getValue());
            }
        }
    }
    
    public boolean delete() throws IOException {
    	int len = getProperties().size();
    	for (String key: this.keySet()) {
    		this.removeAllList(key);
    	}
    	setProperties(BagItExtraPropertiesStore.EMPTY);
    	return len > 0;
    }

    private Property makeProperty(Name name, String value) {
        return m_propertyFactory.create(name, value);
    }
    
    private Name toPropertyName(NameValue bagitProperty) {
        return m_nameFactory.create("info:fedora/bagit/", bagitProperty.getName().replace('-', '.'));
    }
    
    private Property toJcrProperty(NameValue bagitProperty) {
        return m_propertyFactory.create(toPropertyName(bagitProperty), bagitProperty.getValue());
    }
    
    private NameValue toBagitProperty(Property jcrProperty) {
    	NameValue prop = new NameValue();
    	prop.setName(jcrProperty.getName().getLocalName().replace('.', '-'));
    	prop.setValue(jcrProperty.getString());
    	return prop;
    }

    public Map<Name, Property> getProperties() {
        ImmutableMap.Builder<Name, Property> properties =
                ImmutableMap.builder();
        for (NameValue key: this.asList()) {
            Property prop = toJcrProperty(key);
            properties.put(prop.getName(), prop);
        }
        return properties.build();
    }
    
    public void setProperties(Map<Name, Property> properties) {
    	for(Property entry: properties.values()) {
    		NameValue bagitProperty = toBagitProperty(entry);
    		this.removeAllList(bagitProperty.getName());
    		this.putList(bagitProperty);
    	}
    }

}


package org.fcrepo.federation.bagit;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Map;

import org.modeshape.jcr.value.Name;
import org.modeshape.jcr.value.Property;
import org.modeshape.jcr.value.PropertyFactory;
import org.modeshape.jcr.value.StringFactory;
import org.modeshape.jcr.value.ValueFactories;

import com.google.common.collect.ImmutableMap;

import gov.loc.repository.bagit.Bag.BagConstants;
import gov.loc.repository.bagit.BagFile;
import gov.loc.repository.bagit.impl.BagInfoTxtImpl;
import gov.loc.repository.bagit.utilities.namevalue.NameValueReader.NameValue;

public class BagInfo extends BagInfoTxtImpl {

    /**
     * The ID under which this bag is stored.
     */
    public String bagID;
    
    private PropertyFactory m_propertyFactory;
    
    private ValueFactories m_valueFactory;
    
    private StringFactory m_stringFactory;
    
    private static final long serialVersionUID = 1L;

    public BagInfo(String bagID, BagFile bagFile, PropertyFactory propertyFactory, ValueFactories valueFactory, BagConstants bagConstants) {
        super(bagFile, bagConstants);
        this.bagID = bagID;
        m_propertyFactory = propertyFactory;
        m_valueFactory = valueFactory;
        m_stringFactory = valueFactory.getStringFactory();
    }
    
    private OutputStream getOutputStream() throws FileNotFoundException {
    	return new FileOutputStream(new File(this.getFilepath()));
    }
    
    /**
     * Stores this bag-info.txt into its bag.
     */
    public void save() throws IOException {
        try (PrintWriter out = new PrintWriter(getOutputStream())) {
            Map<Name, Property> properties = getProperties();
        	for (Map.Entry<Name, Property> entry : properties.entrySet()) {
                Name name = entry.getKey();
                Property prop = entry.getValue();
                String line =
                        m_stringFactory.create(name) + ": " +
                                m_stringFactory.create(prop);
                out.println(wrapLine(line));
            }
        }
    }
    
    public boolean delete() throws IOException {
    	return new File(this.getFilepath()).delete();
    }

    private Property makeProperty(Name name, String value) {
        return m_propertyFactory.create(name, value);
    }
    
    private Name toPropertyName(NameValue bagitProperty) {
        return m_valueFactory.getNameFactory().create("info:fedora/bagit/", bagitProperty.getName().replace('-', '.'));
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
    	for(Entry<Name, Property> entry: properties.entrySet()) {
    		NameValue bagitProperty = toBagitProperty(entry.getValue());
    		this.removeAllList(bagitProperty.getName());
    		this.putList(bagitProperty);
    	}
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

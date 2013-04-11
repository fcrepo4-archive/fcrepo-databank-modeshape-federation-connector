package org.fcrepo.federation.bagit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.fcrepo.federation.bagit.functions.GetBagInfoTxtWriter;
import org.junit.Before;
import org.junit.Test;
import org.modeshape.jcr.value.Name;
import org.modeshape.jcr.value.NameFactory;
import org.modeshape.jcr.value.Property;
import org.modeshape.jcr.value.PropertyFactory;
import org.modeshape.jcr.value.StringFactory;

import gov.loc.repository.bagit.Bag.BagConstants;
import gov.loc.repository.bagit.BagFile;

public class BagInfoTest {

	private BagFile mockBF;
	private PropertyFactory mockPF;
	private NameFactory mockNF;
	private BagConstants mockBC;
	private GetBagInfoTxtWriter mockWriterFunc;
	private BagInfo testObj;
	
	@Before
	public void setUp() throws IOException {
		mockBF = mock(BagFile.class);
		ByteArrayInputStream bytes = new ByteArrayInputStream(
				"Bag-Count: 1 of 1".getBytes("UTF-8"));
		when(mockBF.newInputStream()).thenReturn(bytes);
		mockPF = mock(PropertyFactory.class);
		mockNF = mock(NameFactory.class);
		mockBC = mock(BagConstants.class);
		when(mockBC.getBagInfoTxt()).thenReturn("bag-info.txt");
		when(mockBC.getBagEncoding()).thenReturn("UTF-8");
		mockWriterFunc = mock(GetBagInfoTxtWriter.class);
		BagInfo.getBagInfoTxtWriter = mockWriterFunc;
		testObj = new BagInfo("/foo", mockBF, mockPF, mockNF, mockBC);
	}
	
	// Public API test
	
	@Test
	public void testSave() throws IOException {
		BagInfoTxtWriter mockWriter = mock(BagInfoTxtWriter.class);
		when(mockWriterFunc.apply(any(String.class))).thenReturn(mockWriter);
		Property mockProp = mockProperty("Bag.Count", "2 of 5");
		when(mockPF.create(any(Name.class), eq("1 of 1"))).thenReturn(mockProp);
		testObj.save();
		verify(mockWriter).write("Bag-Count", "2 of 5");
	}

	@Test
	public void testDelete() throws IOException {
		//nb: this does not save the changes to the file, that's done in save()
		Property mockProp = mockProperty("Bag.Count", "2 of 5");
		when(mockPF.create(any(Name.class), eq("1 of 1"))).thenReturn(mockProp);
		assertEquals(1, testObj.getProperties().size());
		testObj.delete();
		assertEquals(0, testObj.getProperties().size());
	}

	@Test
	public void testGetProperties() {
		Property mockProp = mockProperty("Bag.Count", "2 of 5");
		when(mockPF.create(any(Name.class), eq("1 of 1"))).thenReturn(mockProp);
		Map<Name, Property> props = testObj.getProperties();
		assertEquals(1, props.size());
		Property prop = props.values().iterator().next();
		assertEquals("Bag.Count", prop.getName().getLocalName());
		assertEquals("2 of 5", prop.getString());
	}

	@Test
	public void testSetProperties() {
		Property mockProp = mockProperty("Bag.Count", "1 of 5");
		Property[] props = new Property[]{mockProp};
		Collection<Property> values = Arrays.asList(props);
		Map<Name, Property> mockProps = mock(Map.class);
		when(mockProps.values()).thenReturn(values);
		testObj.setProperties(mockProps);
		assertEquals("1 of 5", testObj.get("Bag-Count"));
		mockProp = mockProperty("Bag.Count", "2 of 5");
		props = new Property[]{mockProp};
		values = Arrays.asList(props);
		when(mockProps.values()).thenReturn(values);
		testObj.setProperties(mockProps);
		assertEquals("2 of 5", testObj.get("Bag-Count"));
	}
	
	private static Property mockProperty(String name, String value) {
		Property mockProp = mock(Property.class);
		Name mockName = mock(Name.class);
		when(mockName.getLocalName()).thenReturn(name);
		when(mockName.getNamespaceUri()).thenReturn("info:fedora/bagit/");
		when(mockProp.getName()).thenReturn(mockName);
		when(mockProp.getString()).thenReturn(value);
		return mockProp;
	}
	
	//TODO Conversion methods that should be tested
}

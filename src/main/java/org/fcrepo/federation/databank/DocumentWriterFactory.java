package org.fcrepo.federation.databank;

import org.modeshape.jcr.cache.document.DocumentTranslator;
import org.modeshape.jcr.federation.FederatedDocumentWriter;
import org.modeshape.jcr.federation.spi.DocumentWriter;

/**
 * This class really exists only to facilitate testing around some
 * cyclical dependencies in MODE
 * @author ba2213
 *
 */
public class DocumentWriterFactory {
	
	private DocumentTranslator m_translator;

	public DocumentWriterFactory() {
		
	}
	
	public DocumentWriterFactory(DocumentTranslator translator) {
		setTranslator(translator);
	}
	
	public void setTranslator(DocumentTranslator translator) {
		m_translator = translator;
	}
	
	public DocumentWriter getDocumentWriter(String id) {
		return new FederatedDocumentWriter(m_translator).setId(id);
	}
}

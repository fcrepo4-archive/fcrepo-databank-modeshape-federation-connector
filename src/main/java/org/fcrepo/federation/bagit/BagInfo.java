
package org.fcrepo.federation.bagit;

import gov.loc.repository.bagit.Bag.BagConstants;
import gov.loc.repository.bagit.impl.BagItTxtImpl;

public class BagInfo extends BagItTxtImpl {

    /**
     * The ID under which this bag is stored.
     */
    public String bagID;
    
    /**
     * Stores this bag-info.txt into its bag.
     */
    public void save() {
        
    }
    
    public static BagInfo fromId(final String bagId) {
        return null;
    }

    private static final long serialVersionUID = 1L;

    public BagInfo(BagConstants bagConstants) {
        super(bagConstants);
    }

}

package gridRenderer;

import java.nio.file.Files;
import java.nio.file.Paths;

import org.globus.io.urlcopy.UrlCopy;
import org.globus.util.ConfigUtil;
import org.globus.util.GlobusURL;
import org.gridforum.jgss.ExtendedGSSCredential;
import org.gridforum.jgss.ExtendedGSSManager;
import org.ietf.jgss.GSSCredential;

public class GridRenderer {
	public static void main(String[] args) {
		try {			
			GlobusURL src = new GlobusURL("gsiftp://karwendel.dps.uibk.ac.at/~/test.txt");
			GlobusURL dest = new GlobusURL("gsiftp://login.leo1.uibk.ac.at/~/test.txt");

			UrlCopy u = new UrlCopy();
			u.setSourceUrl(src);
			u.setDestinationUrl(dest);

			u.setCredentials(getDefaultCredential());

			u.copy();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static GSSCredential getDefaultCredential() {
        try {
        	String proxy = ConfigUtil.discoverProxyLocation();
        	byte[] bytes = Files.readAllBytes(Paths.get(proxy));
        	return ((ExtendedGSSManager) ExtendedGSSManager.getInstance())
                        .createCredential(bytes, ExtendedGSSCredential.IMPEXP_OPAQUE,
                                        GSSCredential.DEFAULT_LIFETIME, null,
                                        GSSCredential.INITIATE_AND_ACCEPT);
        } catch(Exception e){
        	e.printStackTrace();
        }

        return null;
	}
}

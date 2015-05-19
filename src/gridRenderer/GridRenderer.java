package gridRenderer;

import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.globus.io.urlcopy.UrlCopy;
import org.globus.util.ConfigUtil;
import org.globus.util.GlobusURL;
import org.gridforum.jgss.ExtendedGSSCredential;
import org.gridforum.jgss.ExtendedGSSManager;
import org.ietf.jgss.GSSCredential;

public class GridRenderer {
	// Povray
	private final static String POVRAY = "povray";
	private final static String RESULTS = "results";
	
	// Local dirs and files
	private final static Path CWD = Paths.get(System.getProperty("user.dir"));
	private final static Path POVRAY_DIR = CWD.resolve(POVRAY);
	private final static Path RESULT_DIR = CWD.resolve(RESULTS);
	private final static Path RESULT_FILE = RESULT_DIR.resolve("result.gif");
	
	// Remote dirs
	private final static String REMOTE_DIR = "/tmp/homework_5_2";
	private final static String REMOTE_POVRAY_DIR = REMOTE_DIR + "/" + POVRAY;
	private final static String REMOTE_OUTPUT_DIR = REMOTE_DIR + "/" + RESULTS;
	private final static String REMOTE_OUTPUT = REMOTE_OUTPUT_DIR + "/" + "scherk.png";
	
	// Nodes
	private final static String[] NODES = new String[]{ "karwendel.dps.uibk.ac.at", "login.leo1.uibk.ac.at" };
	private final static String FTP_PROTOCOL = "gsiftp";
		
	public static void main(String[] args) {
		try {
			String localhost = InetAddress.getLocalHost().getHostName();
			GlobusURL src = new GlobusURL(FTP_PROTOCOL + "://" + localhost + POVRAY_DIR + "/");
			
			for (String node : NODES) {
				// Copy files to node
				if(!localhost.equals(node)) {
					GlobusURL dest = new GlobusURL(FTP_PROTOCOL + "://" + localhost + REMOTE_POVRAY_DIR + "/");
	
					UrlCopy u = new UrlCopy();
					u.setSourceUrl(src);
					u.setDestinationUrl(dest);
	
					u.setCredentials(getDefaultCredential());
	
					u.copy();
				}
			}
		} catch (Exception e) {
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

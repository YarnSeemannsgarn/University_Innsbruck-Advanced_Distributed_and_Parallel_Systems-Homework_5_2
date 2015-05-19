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
	private static String POVRAY = "povray";
	private static String GM = "gm";
	private static String SCHERK = "scherk.pov";
	private static String RESULTS = "results";
	
	// Local dirs and files
	private static Path CWD = Paths.get(System.getProperty("user.dir"));
	private static Path POVRAY_DIR = CWD.resolve(POVRAY);
	private static Path GM_PATH = POVRAY_DIR.resolve(GM);
	private static Path SCHERK_PATH = POVRAY_DIR.resolve(SCHERK);
	private static Path RESULT_DIR = CWD.resolve(RESULTS);
	private static Path RESULT_FILE = RESULT_DIR.resolve("result.gif");	
	
	// Remote dirs
	private static String REMOTE_DIR = "/tmp/homework_5";
	private static String REMOTE_POVRAY = REMOTE_DIR + "/" + POVRAY;
	private static String REMOTE_SCHERK = REMOTE_DIR + "/" + SCHERK;
	private static String REMOTE_OUTPUT_DIR = REMOTE_DIR + "/" + RESULTS;
	private static String REMOTE_OUTPUT = REMOTE_OUTPUT_DIR + "/" + "scherk.png";
	
	// Nodes
	private final static String[] NODES = new String[]{ "karwendel.dps.uibk.ac.at", "login.leo1.uibk.ac.at" };
	private final static String FTP_PROTOCOL = "gsiftp";
		
	public static void main(String[] args) {
		try {
			String localhost = InetAddress.getLocalHost().getHostName();
			GlobusURL src = new GlobusURL(FTP_PROTOCOL + "://" + localhost + POVRAY_DIR + "/");
			
			//TODO: proxy-init check
			
			for (String node : NODES) {
				// Copy files to node
				if(!localhost.equals(node)) {
					GlobusURL dest = new GlobusURL(FTP_PROTOCOL + "://" + localhost + SCHERK_PATH);
	
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

package gridRenderer;

import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.globus.gram.Gram;
import org.globus.gram.GramJob;
import org.globus.io.urlcopy.UrlCopy;
import org.globus.util.ConfigUtil;
import org.globus.util.GlobusURL;
import org.globus.util.deactivator.Deactivator;
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
	private static Path HOME_DIR = Paths.get(System.getProperty("user.home"));
	private static Path POVRAY_DIR = CWD.resolve(POVRAY);
	private static Path POVRAY_FILE = POVRAY_DIR.resolve(POVRAY);
	private static Path SCHERK_FILE = POVRAY_DIR.resolve(SCHERK);
	private static Path GM_FILE = POVRAY_DIR.resolve(GM);	
	private static Path RESULT_DIR = CWD.resolve(RESULTS);
	private static Path RESULT_FILE = RESULT_DIR.resolve("result.gif");	

	// Remote dirs and files
	private static Path REMOTE_DIR = Paths.get("/tmp/homework_5_2");
	private static Path REMOTE_POVRAY_DIR = REMOTE_DIR.resolve(POVRAY);
	private static Path REMOTE_POVRAY_FILE = REMOTE_POVRAY_DIR.resolve(POVRAY);
	private static Path REMOTE_SCHERK_FILE = REMOTE_POVRAY_DIR.resolve(SCHERK);
	private static Path REMOTE_OUTPUT_DIR = REMOTE_DIR.resolve(RESULTS);
	private static Path REMOTE_OUTPUT_FILES = REMOTE_OUTPUT_DIR.resolve("scherk.png");

	// Nodes
	private final static String[] NODES = new String[]{ "karwendel.dps.uibk.ac.at", "login.leo1.uibk.ac.at" };
	private final static String FTP_PROTOCOL = "gsiftp";

	public static void main(String[] args) {
		try {
			String localhost = InetAddress.getLocalHost().getHostName();
			// Relativize path for globus url
			GlobusURL povraySrc = new GlobusURL(FTP_PROTOCOL + "://" + localhost + "/" + HOME_DIR.relativize(POVRAY_FILE));
			GlobusURL scherkSrc = new GlobusURL(FTP_PROTOCOL + "://" + localhost + "/" + HOME_DIR.relativize(SCHERK_FILE));

			UrlCopy u = new UrlCopy();
			GSSCredential cred = getDefaultCredential();
			u.setCredentials(cred);

			//TODO: proxy-init check

			for (String node : NODES) {
				// Copy files to node (if not localhost)
				// It is not possible to copy directories with GlobusURL class, so just copy files individually
				if(!localhost.equals(node)) {
					//TODO: threads?
					String rsl = "&(executable=/bin/mkdir)(arguments='-p' '" + REMOTE_POVRAY_DIR + "')";
					GramJob mkdirJob = new GramJob(cred, rsl);
					mkdirJob.request(node);
					waitForJob(mkdirJob);

					u.setSourceUrl(povraySrc);
					GlobusURL povrayDest = new GlobusURL(FTP_PROTOCOL + "://" + node + REMOTE_POVRAY_FILE);
					u.setDestinationUrl(povrayDest);
					u.copy();					

					u.setSourceUrl(scherkSrc);
					GlobusURL scherkDest = new GlobusURL(FTP_PROTOCOL + "://" + node + REMOTE_SCHERK_FILE);
					u.setDestinationUrl(scherkDest);
					u.copy();					
				}
			}

			//TODO: Delete remote files
			//TODO: proxy destroy
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		// Deactivate jobs
		Deactivator.deactivateAll();
	}

	private static GSSCredential getDefaultCredential() {
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
	
	private static void waitForJob(GramJob job) {
        try {
            while ( job.getStatus() != GramJob.STATUS_DONE) {
                Thread.sleep(1000);
            }
        } catch(Exception e) {
        	e.printStackTrace();
        }
	}
}

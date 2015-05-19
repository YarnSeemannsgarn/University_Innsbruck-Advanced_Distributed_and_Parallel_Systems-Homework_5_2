package gridRenderer;

import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.globus.gram.GramJob;
import org.globus.io.urlcopy.UrlCopy;
import org.globus.util.ConfigUtil;
import org.globus.util.GlobusURL;
import org.globus.util.deactivator.Deactivator;
import org.gridforum.jgss.ExtendedGSSCredential;
import org.gridforum.jgss.ExtendedGSSManager;
import org.ietf.jgss.GSSCredential;


public class GridRenderer {
	// Args
	public static int PARAMETERS = 1;
	public static String INVALID_SYNTAX = "Invalid number of parameters. Syntax is: "
			+ "frames";	

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
	private static Path REMOTE_DIR = Paths.get("tmp/homework_5_2");
	private static Path REMOTE_POVRAY_DIR = REMOTE_DIR.resolve(POVRAY);
	private static Path REMOTE_POVRAY_FILE = REMOTE_POVRAY_DIR.resolve(POVRAY);
	private static Path REMOTE_SCHERK_FILE = REMOTE_POVRAY_DIR.resolve(SCHERK);
	private static Path REMOTE_OUTPUT_DIR = REMOTE_DIR.resolve(RESULTS);
	private static Path REMOTE_OUTPUT_FILES = REMOTE_OUTPUT_DIR.resolve("scherk.png");

	// JGlobus
	private final static String[] NODES = new String[]{ "karwendel.dps.uibk.ac.at", "login.leo1.uibk.ac.at" };
	private final static String FTP_PROTOCOL = "gsiftp";
	private final static GSSCredential cred = getDefaultCredential();
	private static GlobusURL povraySrc;
	private static GlobusURL scherkSrc;

	public static void main(String[] args) {
		// Check parameters
		if (args.length < PARAMETERS)
			throw new IllegalArgumentException(INVALID_SYNTAX);
		int frames = Integer.parseInt(args[0]);

		String localhost = null;
		try{
			localhost = InetAddress.getLocalHost().getHostName();
			// Relativize path for globus url
			povraySrc = new GlobusURL(FTP_PROTOCOL + "://" + localhost + "/" + HOME_DIR.relativize(POVRAY_FILE));
			scherkSrc = new GlobusURL(FTP_PROTOCOL + "://" + localhost + "/" + HOME_DIR.relativize(SCHERK_FILE));
		} catch (Exception e) {
			e.printStackTrace();
		}

		//TODO: proxy-init check
		int subsetStartFrame = 1;
		int subsetEndFrame = -1;
		int subsetPerProcessor = (int) ((double) frames / (double) NODES.length); // Round down
		int modulo = frames % NODES.length;			

		RESULT_DIR.toFile().mkdirs();
		int i = 0;
		Thread[] threadPool = new Thread[NODES.length];
		long startTime = System.currentTimeMillis();
		for (String node : NODES) {
			subsetEndFrame = subsetStartFrame + subsetPerProcessor - 1;
			if((i+1) <= modulo)
				subsetEndFrame++;			

			// Copy files to node (if not localhost) and render images
			Thread t = null;
			if(localhost.equals(node))
				t = new Thread(new RenderFilesOnNode(node, subsetStartFrame, subsetEndFrame, frames, i, false));
			else
				t = new Thread(new RenderFilesOnNode(node, subsetStartFrame, subsetEndFrame, frames, i, true));

			t.start();
			threadPool[i] = t;			

			subsetStartFrame = subsetEndFrame + 1;
			i++;			
		}

		// Join threads
		for(i = 0; i < threadPool.length; i++) {
			try {
				threadPool[i].join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		System.out.println("Copy and rendering time: " + ((System.currentTimeMillis() - startTime)/1000) + "s");

		//TODO: Delete remote files
		//TODO: proxy destroy


		// Deactivate jobs
		Deactivator.deactivateAll();
	}

	private static class RenderFilesOnNode implements Runnable{
		private String node;
		private int subsetStartFrame;
		private int subsetEndFrame;
		private int frames;
		private int i;
		private boolean localhost;

		public RenderFilesOnNode(String node, int subsetStartFrame, int subsetEndFrame, int frames, int i, boolean localhost) {
			this.node = node;
			this.subsetStartFrame = subsetStartFrame;
			this.subsetEndFrame = subsetEndFrame;
			this.frames = frames;
			this.i = i;
			this.localhost = localhost;
		}

		public void run() {
			try{
				System.out.println("Render frames " + subsetStartFrame + "-" + subsetEndFrame + " on node: " + node);
				if(!localhost) {
					System.out.println("Cpoy files to node: " + node);
					// Create povray directory
					String rsl = "&(executable=/bin/mkdir)(arguments='-p' '" + REMOTE_POVRAY_DIR + "')";
					GramJob mkdirJob = new GramJob(cred, rsl);
					mkdirJob.request(node);
					waitForJob(mkdirJob);

					// It is not possible to copy directories with GlobusURL class, so just copy files individually
					UrlCopy u = new UrlCopy();
					u.setCredentials(cred);

					u.setSourceUrl(povraySrc);
					GlobusURL povrayDest = new GlobusURL(FTP_PROTOCOL + "://" + node + "/" + REMOTE_POVRAY_FILE);
					u.setDestinationUrl(povrayDest);
					u.copy();					

					u.setSourceUrl(scherkSrc);
					GlobusURL scherkDest = new GlobusURL(FTP_PROTOCOL + "://" + node + "/" + REMOTE_SCHERK_FILE);
					u.setDestinationUrl(scherkDest);
					u.copy();
					
					// Make povray runnable
					rsl = "&(executable=/bin/chmod)(arguments='+x' '" + REMOTE_POVRAY_FILE + "')";
					GramJob chmodJob = new GramJob(cred, rsl);
					chmodJob.request(node);

					// Create result directory
					rsl = "&(executable=/bin/mkdir)(arguments='-p' '" + REMOTE_OUTPUT_DIR + "')";
					GramJob mkdirJob2 = new GramJob(cred, rsl);
					mkdirJob2.request(node);

					// Wait for jobs
					waitForJob(chmodJob);
					waitForJob(mkdirJob2);

					// Render Files
					rsl = "&(executable=~/ " + REMOTE_POVRAY_FILE + ")(arguments='+I" + REMOTE_SCHERK_FILE + 
							" +O" + REMOTE_OUTPUT_FILES + " +FN +W1024 +H768" + " +KFI" + 1 + " +KFF" + frames + 
							" +SF" + subsetStartFrame + " +EF" + subsetEndFrame + " -A0.1 +R2 +KI0 +KF1 +KC -P')";
					GramJob renderJob = new GramJob(cred, rsl);
					waitForJob(renderJob);					
				}
				else {
					//TODO
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}		
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

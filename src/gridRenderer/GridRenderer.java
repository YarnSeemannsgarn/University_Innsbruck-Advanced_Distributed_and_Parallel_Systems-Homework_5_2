package gridRenderer;

import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.globus.gram.GramJob;
import org.globus.gram.GramJobListener;
import org.globus.io.urlcopy.UrlCopy;
import org.globus.util.ConfigUtil;
import org.globus.util.GlobusURL;
import org.globus.util.deactivator.Deactivator;
import org.gridforum.jgss.ExtendedGSSCredential;
import org.gridforum.jgss.ExtendedGSSManager;
import org.ietf.jgss.GSSCredential;


public class GridRenderer {
	// Args
	public static final int PARAMETERS = 1;
	public static final String INVALID_SYNTAX = "Invalid number of parameters. Syntax is: frames";	

	// Povray
	private static final String POVRAY = "povray";
	private static final String POVRAY_RENDER = "povray_render.sh";
	private static final String GM = "gm";
	private static final String SCHERK = "scherk.pov";

	// Local dirs and files
	private static final Path CWD = Paths.get(System.getProperty("user.dir"));
	private static final Path HOME_DIR = Paths.get(System.getProperty("user.home"));
	private static final Path POVRAY_DIR = CWD.resolve(POVRAY);
	private static final Path POVRAY_FILE = POVRAY_DIR.resolve(POVRAY);
	private static final Path POVRAY_RENDER_FILE = POVRAY_DIR.resolve(POVRAY_RENDER);
	private static final Path SCHERK_FILE = POVRAY_DIR.resolve(SCHERK);
	private static final Path GM_FILE = POVRAY_DIR.resolve(GM);
	private static final Path RESULT_FILE = CWD.resolve("result.gif");	

	// Remote dirs and files
	private static final Path REMOTE_DIR = Paths.get("tmp/homework_5_2");
	private static final Path REMOTE_POVRAY_FILE = REMOTE_DIR.resolve(POVRAY);
	private static final Path REMOTE_POVRAY_RENDER_FILE = REMOTE_DIR.resolve(POVRAY_RENDER);
	private static final Path REMOTE_SCHERK_FILE = REMOTE_DIR.resolve(SCHERK);

	// JGlobus
	private static final String[] NODES = new String[]{ "karwendel.dps.uibk.ac.at", "login.leo1.uibk.ac.at" };
	private static String localhost;
	private static final String FTP_PROTOCOL = "gsiftp";
	private static GSSCredential cred = getDefaultCredential();;
	private static GlobusURL povraySrc;
	private static GlobusURL povrayRenderSrc;
	private static GlobusURL scherkSrc;

	public static void main(String[] args) {
		// Check parameters
		if (args.length < PARAMETERS)
			throw new IllegalArgumentException(INVALID_SYNTAX);
		int frames = Integer.parseInt(args[0]);

		// Initialize JGlobus stuff
		try{
			localhost = InetAddress.getLocalHost().getHostName();

			// Relativize path for globus url
			povraySrc = new GlobusURL(FTP_PROTOCOL + "://" + localhost + "/" + HOME_DIR.relativize(POVRAY_FILE)); 
			povrayRenderSrc = new GlobusURL(FTP_PROTOCOL + "://" + localhost + "/" + HOME_DIR.relativize(POVRAY_RENDER_FILE));
			scherkSrc = new GlobusURL(FTP_PROTOCOL + "://" + localhost + "/" + HOME_DIR.relativize(SCHERK_FILE));
		} catch (Exception e) {
			e.printStackTrace();
		}

		//TODO: proxy-init check
		int subsetStartFrame = 1;
		int subsetEndFrame = -1;
		int subsetPerProcessor = (int) ((double) frames / (double) NODES.length); // Round down
		int modulo = frames % NODES.length;			

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
				t = new Thread(new RenderFilesOnNode(node, subsetStartFrame, subsetEndFrame, frames, false, i));
			else
				t = new Thread(new RenderFilesOnNode(node, subsetStartFrame, subsetEndFrame, frames, true, i));

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


		// Deactivate jobs (otherwise screen stucks)
		Deactivator.deactivateAll();
	}

	private static class RenderFilesOnNode implements Runnable{
		private String node;
		private int subsetStartFrame;
		private int subsetEndFrame;
		private int frames;
		private boolean isRemoteNode;
		private int ctr;

		public RenderFilesOnNode(String node, int subsetStartFrame, int subsetEndFrame, int frames, boolean isRemoteNode, int ctr) {
			this.node = node;
			this.subsetStartFrame = subsetStartFrame;
			this.subsetEndFrame = subsetEndFrame;
			this.frames = frames;
			this.isRemoteNode = isRemoteNode;
			this.ctr = ctr;
		}

		public void run() {
			String renderRsl;
			String tarRsl;
			String rsl;
			if(isRemoteNode) {
				// Create remote directory
				System.out.println("Copy files to node " + node);
				rsl = "&(executable=/bin/mkdir)(arguments='-p' '" + REMOTE_DIR + "')";
				submitAndWaitForJob(rsl, node);

				// Copy files
				// It is not possible to copy directories with GlobusURL class, so just copy files individually
				try{
					GlobusURL povrayDest = new GlobusURL(FTP_PROTOCOL + "://" + node + "/" + REMOTE_POVRAY_FILE);
					urlCopy(povraySrc, povrayDest);

					GlobusURL povrayRenderDest = new GlobusURL(FTP_PROTOCOL + "://" + node + "/" + REMOTE_POVRAY_RENDER_FILE);
					urlCopy(povrayRenderSrc, povrayRenderDest);

					GlobusURL scherkDest = new GlobusURL(FTP_PROTOCOL + "://" + node + "/" + REMOTE_SCHERK_FILE);
					urlCopy(scherkSrc, scherkDest);
				} catch (Exception e) {
					e.printStackTrace();
				}

				// Make povray runnable
				rsl = "&(executable=/bin/chmod)(arguments='+x' '" + REMOTE_POVRAY_FILE + "' '" + REMOTE_POVRAY_RENDER_FILE + "')";
				submitAndWaitForJob(rsl, node);

				// Render rsl (remote node)
				renderRsl = "&(executable=" + REMOTE_POVRAY_RENDER_FILE + ")"
						+ "(arguments='1' '" + frames + "' '" + subsetStartFrame + "' '" + subsetEndFrame + "')";
			}
			else {
				// Render rsl (localhost)
				renderRsl = "&(executable=" + HOME_DIR.relativize(POVRAY_RENDER_FILE) + ")"
						+ "(arguments='1' '" + frames + "' '" + subsetStartFrame + "' '" + subsetEndFrame + "')";
			}

			// Render Files
			System.out.println("Render frames " + subsetStartFrame + "-" + subsetEndFrame + " on node: " + node);
			GramJobListener renderJobListener = new GramJobListener() {
				@Override
				public void statusChanged(GramJob job) {
					System.out.println("Render job status on node " + node + ": " + job.getStatusAsString());
				}
			};
			submitAndWaitForJob(renderRsl, node, renderJobListener);

			// Tar remote result files and get results
			if(isRemoteNode) {
				System.out.println("Tar result files on node " + node);
				String tarName = "results" + ctr + ".tar.gz";
				String resultTarPath = REMOTE_DIR + "/" + tarName;
				tarRsl = "&(executable=/bin/tar)(arguments='-c' '-z' '-f' '" + resultTarPath + "'";
				for(int j = this.subsetStartFrame; j < (this.subsetEndFrame+1); j++)
					tarRsl += " '" + REMOTE_DIR + "/scherk" + j + ".png'";
				tarRsl += ")";

				submitAndWaitForJob(tarRsl, node);

				System.out.println("Collect Tar result file from node " + node);
				try{
					GlobusURL tarSrc = new GlobusURL(FTP_PROTOCOL + "://" + node + "/" + resultTarPath);
					GlobusURL tarDest = new GlobusURL(FTP_PROTOCOL + "://" + localhost + "/" + HOME_DIR.relativize(POVRAY_DIR) + "/" + tarName);
					urlCopy(tarSrc, tarDest);
				} catch (Exception e) {
					e.printStackTrace();
				}
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

	private static void urlCopy(GlobusURL src, GlobusURL dest) {
		UrlCopy u = new UrlCopy();
		u.setCredentials(cred);
		try {
			u.setSourceUrl(src);
			u.setDestinationUrl(dest);
			u.copy();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void submitAndWaitForJob(String rsl, String node) {
		submitAndWaitForJob(rsl, node, null);
	}

	private static void submitAndWaitForJob(String rsl, String node, GramJobListener jobListener) {
		GramJob job = new GramJob(cred, rsl);

		if(jobListener != null)
			job.addListener(jobListener);
		try {
			job.request(node);
		} catch (Exception e) {
			e.printStackTrace();
		}
		waitForJob(job);		
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

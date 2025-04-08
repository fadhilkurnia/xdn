package edu.umass.cs.xdn.recorder;

import edu.umass.cs.xdn.utils.Shell;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FuselogStateDiffRecorder extends AbstractStateDiffRecorder {

    private static final String FUSELOG_BIN_PATH = "/usr/local/bin/fuselog";
    private static final String FUSELOG_APPLY_BIN_PATH = "/usr/local/bin/fuselog-apply";

    private static final String defaultWorkingBasePath = "/tmp/xdn/state/fuselog/";

    private final String baseMountDirPath;
    private final String baseSocketDirPath;
    private final String baseDiffDirPath;

    // important locations:
    // /tmp/xdn/state/fuselog/<node-id>/                                    the base directory
    // /tmp/xdn/state/fuselog/<node-id>/mnt/                                the mount directory
    // /tmp/xdn/state/fuselog/<node-id>/mnt/<service-name>/e<epoch>/        mount dir of specific service
    // /tmp/xdn/state/fuselog/<node-id>/sock/                               the socket directory
    // /tmp/xdn/state/fuselog/<node-id>/sock/<service-name>::e<epoch>.sock  socket to fs of specific service
    // /tmp/xdn/state/fuselog/<node-id>/diff/                               the stateDiff directory
    // /tmp/xdn/state/fuselog/<node-id>/diff/<service-name>::e<epoch>.diff  the stateDiff file

    // mapping service name and epoch into its fuselog filesystem socket
    private final Map<String, Map<Integer, SocketChannel>> serviceFsSocket;

    public FuselogStateDiffRecorder(String nodeID) {
        super(nodeID, defaultWorkingBasePath + nodeID + "/");
        System.out.println(">>> initializing fuse statediff recorder");

        // make sure that fuselog and fuselog-apply are exist
        File fuselog = new File(FUSELOG_BIN_PATH);
        if (!fuselog.exists()) {
            String errMessage = "fuselog binary does not exist at " + FUSELOG_BIN_PATH;
            System.out.println("ERROR: " + errMessage);
            throw new RuntimeException(errMessage);
        }
        File fuselogApplicator = new File(FUSELOG_APPLY_BIN_PATH);
        if (!fuselogApplicator.exists()) {
            String errMessage = "fuselog-apply binary does not exist at " + FUSELOG_APPLY_BIN_PATH;
            System.out.println("ERROR: " + errMessage);
            throw new RuntimeException(errMessage);
        }

        // create working mount dir, if not exist
        // e.g., /tmp/xdn/state/fuselog/node1/mnt/
        this.baseMountDirPath = this.baseDirectoryPath + "mnt/";
        try {
            Files.createDirectories(Paths.get(this.baseMountDirPath));
        } catch (IOException e) {
            System.err.println("ERROR: " + e);
            throw new RuntimeException(e);
        }

        // create socket dir, if not exist
        // e.g., /tmp/xdn/state/fuselog/node1/sock/
        this.baseSocketDirPath = defaultWorkingBasePath + nodeID + "/sock/";
        try {
            Files.createDirectories(Paths.get(this.baseSocketDirPath));
        } catch (IOException e) {
            System.err.println("ERROR: " + e);
            throw new RuntimeException(e);
        }

        // create diff dir, if not exist
        // e.g., /tmp/xdn/state/fuselog/node1/diff/
        this.baseDiffDirPath = defaultWorkingBasePath + nodeID + "/diff/";
        try {
            Files.createDirectories(Paths.get(this.baseDiffDirPath));
        } catch (IOException e) {
            System.err.println("ERROR: " + e);
            throw new RuntimeException(e);
        }

        // initialize mapping between serviceName to the FS socket
        this.serviceFsSocket = new ConcurrentHashMap<>();

    }

    @Override
    public String getTargetDirectory(String serviceName, int placementEpoch) {
        // location: /tmp/xdn/state/fuselog/<node-id>/mnt/<service-name>/e<epoch>/
        return String.format("%s%s/e%d/",
                baseMountDirPath, serviceName, placementEpoch);
    }

    @Override
    public boolean preInitialization(String serviceName, int placementEpoch) {
        String targetDir = this.getTargetDirectory(serviceName, placementEpoch);
        String socketFile = baseSocketDirPath + serviceName + "::" + placementEpoch + ".sock";

        // create target mnt dir, if not exist
        // e.g., /tmp/xdn/state/fuselog/node1/mnt/service1/
        Shell.runCommand("sudo umount " + targetDir + " > /dev/null 2>&1");
        Shell.runCommand("rm -rf " + targetDir);
        int code = Shell.runCommand("mkdir -p " + targetDir);
        assert code == 0 : "statediff dir creation must success";

        // initialize file system in the mnt dir, with socket
        assert targetDir.length() > 1 : "invalid target mount directory";
        // remove the trailing '/' at the end of targetDir
        String targetDirPath = targetDir.substring(0, targetDir.length() - 1);
        String cmd = String.format("%s -s -o allow_other -o allow_root %s",
                FUSELOG_BIN_PATH, targetDirPath);
        Map<String, String> env = new HashMap<>();
        env.put("FUSELOG_SOCKET_FILE", socketFile);
        int exitCode = Shell.runCommand(cmd, false, env);
        assert exitCode == 0 : "failed to mount filesystem with exit code " + exitCode;

        // initialize socket client for the filesystem
        SocketChannel socketChannel;
        UnixDomainSocketAddress address = UnixDomainSocketAddress.of(Path.of(socketFile));
        try {
            socketChannel = SocketChannel.open(StandardProtocolFamily.UNIX);
            boolean isConnEstablished = socketChannel.connect(address);
            if (!isConnEstablished) {
                System.err.println("failed to connect to the filesystem");
                return false;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // update the socket metadata
        if (!serviceFsSocket.containsKey(serviceName)) {
            serviceFsSocket.put(serviceName, new ConcurrentHashMap<>());
        }
        serviceFsSocket.get(serviceName).put(placementEpoch, socketChannel);

        return true;
    }

    @Override
    public boolean postInitialization(String serviceName, int placementEpoch) {
        // TODO: read initialization stateDiff and discard it
        return false;
    }

    @Override
    public String captureStateDiff(String serviceName, int placementEpoch) {
        Map<Integer, SocketChannel> epochToChannelMap = serviceFsSocket.get(serviceName);
        assert epochToChannelMap != null : "unknown fs socket client for " + serviceName;
        SocketChannel socketChannel = epochToChannelMap.get(placementEpoch);
        assert socketChannel != null : "unknown fs socket client for " + serviceName;

        // send get command (g) to the filesystem
        try {
            System.out.println(">> sending fuselog command ...");
            socketChannel.write(ByteBuffer.wrap("g".getBytes()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // wait for response indicating the stateDiff size
        ByteBuffer sizeBuffer = ByteBuffer.allocate(8);
        sizeBuffer.order(ByteOrder.LITTLE_ENDIAN);
        sizeBuffer.clear();
        System.out.println(">> reading fuselog response ...");
        int numRead;
        try {
            numRead = socketChannel.read(sizeBuffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        System.out.println(">> got " + numRead + "bytes: " + new String(sizeBuffer.array(),
                StandardCharsets.UTF_8));
        if (numRead < 8) {
            System.err.println("failed to read size of the stateDiff");
            return null;
        }
        long stateDiffSize = sizeBuffer.getLong(0);
        System.out.println(">> stateDiff size=" + stateDiffSize);

        // read all the stateDiff
        ByteBuffer stateDiffBuffer = ByteBuffer.allocate((int) stateDiffSize);
        numRead = 0;
        try {
            while (numRead < stateDiffSize) {
                numRead += socketChannel.read(stateDiffBuffer);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        System.out.println(">> complete reading stateDiff ...");
        String stateDiff = Base64.getEncoder().encodeToString(stateDiffBuffer.array());
        System.out.println(">> read stateDiff: " + stateDiff);

        // convert the stateDiff into String
        return stateDiff;
    }

    @Override
    public boolean applyStateDiff(String serviceName, int placementEpoch, String encodedState) {
        // TODO: directly apply stateDiff from the obtained byte[], not via
        //  the fuselog-apply program, which we currently use.

        String diffFile = this.baseDiffDirPath + serviceName + "::" + placementEpoch + ".diff";
        String targetDir = this.getTargetDirectory(serviceName, placementEpoch);

        // store stateDiff into an external file
        byte[] stateDiff;
        try {
            FileOutputStream outputStream;
            outputStream = new FileOutputStream(diffFile);
            stateDiff = Base64.getDecoder().decode(encodedState);
            outputStream.write(stateDiff);
            outputStream.flush();
            outputStream.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // preparing the shell command to apply stateDiff
        String cmd = String.format("%s %s --silent --statediff=%s",
                FUSELOG_APPLY_BIN_PATH, targetDir, diffFile);
        int exitCode = Shell.runCommand(cmd, true);
        assert exitCode == 0 : "failed to apply stateDiff with exit code " + exitCode;

        return true;
    }

    @Override
    public boolean removeServiceRecorder(String serviceName, int placementEpoch) {
        String targetDir = this.getTargetDirectory(serviceName, placementEpoch);
        int umountRetCode = Shell.runCommand("sudo umount " + targetDir + " > /dev/null 2>&1", false);
        int rmRetCode = Shell.runCommand("rm -rf " + targetDir, false);
        assert rmRetCode == 0;
        return true;
    }
}

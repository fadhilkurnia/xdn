package edu.umass.cs.consistency.EventualConsistency;

import com.fasterxml.jackson.core.type.TypeReference;
import edu.umass.cs.consistency.EventualConsistency.Domain.CheckpointLog;
import edu.umass.cs.consistency.EventualConsistency.Domain.GraphNode;
import edu.umass.cs.consistency.EventualConsistency.Domain.RequestInformation;
import edu.umass.cs.utils.StringLocker;
import org.json.JSONException;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.logging.Level;
import com.fasterxml.jackson.databind.*;
import org.json.JSONObject;

public class DAGLogger {
    private String logFile;
    private String rollForwardLogFile;
    private CheckpointLog checkpointLog;
    private ServerSocket serverSock;
    private ScheduledExecutorService executor;
    private static final StringLocker stringLocker = new StringLocker();
    private boolean closed = false;
    private final String directoryPath = "logs/DAGLogs";

    public DAGLogger(String logFile) throws IOException {
        this.logFile = "/checkpoint."+logFile+".json";
        this.rollForwardLogFile = "/rollForward."+logFile+".txt";
        createNewFileIfNotExists(new File(directoryPath+this.logFile));
        createNewFileIfNotExists(new File(directoryPath+this.rollForwardLogFile));
    }
    private void reinitializeFile(String filePath) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, false))) {
            writer.write("");
        }
    }

    private void createNewFileIfNotExists(File file) throws IOException {
        if (!file.exists()) {
            if (file.createNewFile()) {
                DynamoManager.log.log(Level.INFO, "Log File created: {0}", new Object[]{file.getAbsolutePath()});
            } else {
                DynamoManager.log.log(Level.WARNING, "Failed to create log file: {0}", new Object[]{file.getAbsolutePath()});
            }
        }
        else {
            DynamoManager.log.log(Level.INFO, "Log file already exists: {0}", new Object[]{file.getAbsolutePath()});
        }
    }

    public void rollForward(GraphNode graphNode){
        ObjectMapper objectMapper = new ObjectMapper();
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(directoryPath+this.rollForwardLogFile, true))) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("vectorClock", objectMapper.writeValueAsString(graphNode.getVectorClock()));
            jsonObject.put("requests", objectMapper.writeValueAsString(graphNode.getRequests()));
            writer.write(jsonObject.toString());
            writer.newLine();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

    public ArrayList<GraphNode> readFromRollForwardFile() throws IOException, JSONException {
        ObjectMapper objectMapper = new ObjectMapper();
        List<String> lines = Files.readAllLines(Path.of(directoryPath + this.rollForwardLogFile));
        DynamoManager.log.log(Level.INFO, lines.toString());
        ArrayList<GraphNode> graphNodes = new ArrayList<>();
        for (String line : lines) {
            JSONObject jsonObject = new JSONObject(line);
            DynamoManager.log.log(Level.INFO, jsonObject.toString());
            HashMap<Integer, Integer> vectorClock = objectMapper.readValue(jsonObject.getString("vectorClock"), new TypeReference<HashMap<Integer, Integer>>() {});

            ArrayList<RequestInformation> requests = objectMapper.readValue(jsonObject.getString("requests"), new TypeReference<ArrayList<RequestInformation>>() {});
            graphNodes.add(new GraphNode(vectorClock, requests));
        }
        return graphNodes;
    }

    public void checkpoint(String state, int noOOfCheckpoints,  HashMap<Integer, Integer> vectorClock, String quorumId, ArrayList<GraphNode> leafNodes, int myId) throws JSONException {
        ArrayList<HashMap<Integer, Integer>> latestVectorClocks = new ArrayList<>();
        for (GraphNode node : leafNodes) {
            latestVectorClocks.add(node.getVectorClock());
        }
        this.checkpointLog = new CheckpointLog(state, noOOfCheckpoints, quorumId, vectorClock, latestVectorClocks, myId);
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            objectMapper.writeValue(new File(directoryPath+this.logFile), this.checkpointLog);
            reinitializeFile(directoryPath + this.rollForwardLogFile);
        } catch (Exception e) {
            DynamoManager.log.log(Level.SEVERE, "Could not checkpoint the state");
            throw new RuntimeException(e);
        }
        DynamoManager.log.log(Level.INFO, "Checkpointing completed successfully");
    }

    public synchronized CheckpointLog restore() throws JSONException, RuntimeException {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            File file = new File(directoryPath+this.logFile);
            if (file.exists()) {
                System.out.println("Trying to restore File :"+file.getAbsolutePath());
                this.checkpointLog = objectMapper.readValue(file, CheckpointLog.class);
                return this.checkpointLog;
            }
        } catch (IOException e) {
            DynamoManager.log.log(Level.SEVERE, "Error encountered: "+e);
            throw new RuntimeException("Restoration not completed");
        }
        return null;
    }

    public int checkpointTransferRequest() throws RuntimeException {
        initCheckpointServer(new InetSocketAddress(0));
        DynamoManager.log.log(Level.INFO, "Initiated checkpoint transfer ");
        return this.serverSock.getLocalPort();
    }
    public synchronized void getTransferredCheckpoint(String address, int port, String fileName) throws RuntimeException, IOException {
        reinitializeFile(getCheckpointLogFilePath());
        reinitializeFile(getRollForwardLogFileLogFilePath());
        receiveFile(address, port, fileName, getCheckpointLogFilePath());
    }

    private static final int THREAD_POOL_SIZE = 4;

    private void initCheckpointServer(InetSocketAddress inetSocketAddress) {
        this.executor = Executors.newScheduledThreadPool(THREAD_POOL_SIZE,
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread thread = Executors.defaultThreadFactory()
                                .newThread(r);
                        thread.setName("Checkpoint");
                        return thread;
                    }
                });

        try {
            this.serverSock = new ServerSocket();
            this.serverSock.bind(inetSocketAddress);
            executor.submit(new CheckpointServer());
        } catch (IOException e) {
            DynamoManager.log.severe(this
                    + " unable to open server socket for large checkpoint transfers");
            e.printStackTrace();
        }
    }
    private class CheckpointServer implements Runnable {

        @Override
        public void run() {
            Socket sock = null;
            try {
                DynamoManager.log.log(Level.INFO, "Checkpoint server started at "+DAGLogger.this.serverSock.getInetAddress().getHostAddress()+":"+DAGLogger.this.serverSock.getLocalPort());
                while ((sock = DAGLogger.this.serverSock.accept()) != null) {
                    executor.submit(new CheckpointTransporter(sock));
                }
            } catch (IOException e) {
                if (!isClosed()) {
                    DynamoManager.log.severe("Incurred IOException while processing checkpoint transfer request");
                    e.printStackTrace();
                }
            }
            catch (Exception e) {
                DynamoManager.log.severe(e.toString());

            }
        }
    }

    private class CheckpointTransporter implements Runnable {

        final Socket sock;

        CheckpointTransporter(Socket sock) {
            DynamoManager.log.log(Level.INFO, "Checkpoint transporter init");
            this.sock = sock;
        }

        @Override
        public void run() {
            transferCheckpoint(sock);
        }
    }

    private void transferCheckpoint(Socket sock) {
        {
            DynamoManager.log.log(Level.INFO, "Transferring checkpoint file using socket");
            BufferedReader brSock = null, brFile = null;
            try {
                brSock = new BufferedReader(new InputStreamReader(
                        sock.getInputStream()));
                // first and only line is request
                String request = brSock.readLine();
                DynamoManager.log.log(Level.INFO, "Request received for : "+request);
                // synchronized to prevent concurrent file delete
                synchronized (stringLocker.get(request)) {
                    if ((new File(request).exists())) {
                        DynamoManager.log.log(Level.INFO, "Requested file exists");

                        // request is filename
                        brFile = new BufferedReader(new InputStreamReader(
                                new FileInputStream(request)));
                        final BufferedInputStream inStream = new BufferedInputStream(new FileInputStream(request));
                        // file successfully open if here
                        OutputStream outStream = sock.getOutputStream();
                        // read in the binary file and send out
                        final byte[] buffer = new byte[4096];
                        for (int read = inStream.read(buffer); read >= 0; read = inStream.read(buffer))
                            outStream.write(buffer, 0, read);
                        DynamoManager.log.log(Level.INFO, "Checkpoint Transfer successful");
                        inStream.close();
                        outStream.close();
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (brSock != null)
                        brSock.close();
                    if (brFile != null)
                        brFile.close();
                    close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void receiveFile(String serverAddress, int port, String fileName, String savePath) {
        Socket socket = null;
        BufferedReader brSock = null;
        BufferedOutputStream fileOut = null;
        try {
            // Connect to the server
            DynamoManager.log.log(Level.INFO, "Trying socket connection on "+serverAddress+":"+port);
            socket = new Socket(serverAddress, port);
            DynamoManager.log.log(Level.INFO, "Socket connection established");
            // Send the filename request to the server
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

            // Write a line to the socket
            out.println(fileName);

            out.flush();
            InputStream inStream = socket.getInputStream();
            // Prepare to receive the file data
            fileOut = new BufferedOutputStream(new FileOutputStream(savePath));

            // Read the file data from the socket and write it to the specified file
            byte[] buffer = new byte[4096];
            int bytesRead;
            while ((bytesRead = inStream.read(buffer)) != -1) {
                fileOut.write(buffer, 0, bytesRead);
            }

            System.out.println("File received successfully and saved to " + savePath);

        } catch (IOException e) {
            DynamoManager.log.log(Level.INFO, "State could not be saved");
            DynamoManager.pseudo_failure.set(false);
        } finally {
            try {
                if (brSock != null) brSock.close();
                if (fileOut != null) fileOut.close();
                if (socket != null) socket.close();
            } catch (IOException e) {
                DynamoManager.log.log(Level.INFO, "Close could not be performed");
            }
            DynamoManager.state_transfer.set(false);
        }
    }

    private boolean isClosed() {
        return this.closed;
    }

    public void close() {
        this.closed = true;
        try {
            this.serverSock.close();
        } catch (IOException e) {
            DynamoManager.log.severe(this + " unable to close server socket");
            e.printStackTrace();
        }
        this.executor.shutdownNow();
    }

    public void setCheckpointLog(CheckpointLog checkpointLog) throws IOException {
        this.checkpointLog = checkpointLog;
        reinitializeFile(directoryPath + this.rollForwardLogFile);
    }

    public CheckpointLog getCheckpointLog() {
        return checkpointLog;
    }
    public String getCheckpointLogFilePath(){
        return directoryPath+this.logFile;
    }
    public String getRollForwardLogFileLogFilePath(){
        return directoryPath+this.rollForwardLogFile;
    }
}

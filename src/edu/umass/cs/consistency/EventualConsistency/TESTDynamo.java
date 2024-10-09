package edu.umass.cs.consistency.EventualConsistency;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.umass.cs.consistency.ClientCentric.TESTMW;
import edu.umass.cs.consistency.EventualConsistency.Domain.CheckpointLog;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class TESTDynamo {
    public static AtomicBoolean passed = new AtomicBoolean(true);
    private static TESTDynamoClient testDynamoClient;

    @BeforeClass
    public static void initialize() throws IOException {
        testDynamoClient = new TESTDynamoClient();
    }

    @Test
    public void test00_putOnOneServer() throws Exception {
        System.out.println("TEST 00 starting");
        testDynamoClient.sendAppRequest(TESTDynamoClient.makePutRequest(testDynamoClient, 0), testDynamoClient.ports[1]);
        Thread.sleep(100);
        Assert.assertTrue(passed.get());
    }

    @Test
    public void test01_twoPutOneGetOnDifferentServers() throws Exception {
        System.out.println("TEST 01 starting");
        testDynamoClient.sendAppRequest(TESTDynamoClient.makePutRequest(testDynamoClient, 0), testDynamoClient.ports[1]);
        testDynamoClient.sendAppRequest(TESTDynamoClient.makePutRequest(testDynamoClient, 0), testDynamoClient.ports[0]);
        Thread.sleep(100);
        testDynamoClient.sendAppRequest(TESTDynamoClient.makeGetRequest(testDynamoClient, 0), testDynamoClient.ports[2]);
        Thread.sleep(200);
        Assert.assertTrue(passed.get());
    }

    @Test
    public void test02_twoPutOneGetOnDifferentServersForDifferentObjects() throws Exception {
        System.out.println("TEST 02 starting");
        testDynamoClient.sendAppRequest(TESTDynamoClient.makePutRequest(testDynamoClient, 0), testDynamoClient.ports[2]);
        testDynamoClient.sendAppRequest(TESTDynamoClient.makePutRequest(testDynamoClient, 1), testDynamoClient.ports[0]);
        Thread.sleep(100);
        testDynamoClient.sendAppRequest(TESTDynamoClient.makeGetRequest(testDynamoClient, 0), testDynamoClient.ports[1]);
        Thread.sleep(100);
        Assert.assertTrue(passed.get());
    }

    @Test
    public void test03_getAnObjectNeverPut() throws Exception {
        System.out.println("TEST 03 starting");
        testDynamoClient.sendAppRequest(TESTDynamoClient.makeGetRequest(testDynamoClient, 2), testDynamoClient.ports[0]);
        Thread.sleep(100);
        Assert.assertTrue(passed.get());
    }

    @Test
    public void test04_sendRandomNumerousRequests() throws Exception {
        Thread.sleep(100);
        System.out.println("TEST 04 starting");
        for (int i = 0; i < 40; i++) {
            int port = (int) (Math.random() * ((testDynamoClient.ports.length - 1) + 1));
            DynamoRequestPacket dynamoRequestPacket = i % 2 == 0 ? TESTDynamoClient.makePutRequest(testDynamoClient, -1) : TESTDynamoClient.makeGetRequest(testDynamoClient, -1);
            testDynamoClient.sendAppRequest(dynamoRequestPacket, testDynamoClient.ports[port]);
            Thread.sleep(1000);
        }
        Assert.assertTrue(passed.get());
    }

    @Test
    public void test05_sendStopRequest() throws Exception {
        System.out.println("TEST 05 starting");
        testDynamoClient.sendAppRequest(TESTDynamoClient.makeStopRequest(), testDynamoClient.ports[1]);
        Thread.sleep(100);
        Assert.assertTrue(true);
    }

    @Test
    public void test06_failureAndRecovery() throws Exception {
        try {
            System.out.println("TEST 06 starting");

            //kill the AR3 Active replica
            String[] getPidCommand = {"/bin/sh", "-c", "ps -ef | grep 'reconfiguration.ReconfigurableNode AR3' | grep -v grep | awk '{print $2}'"};
            Process getPidProcess = Runtime.getRuntime().exec(getPidCommand);
            BufferedReader reader = new BufferedReader(new InputStreamReader(getPidProcess.getInputStream()));
            String pid = reader.readLine();

            if (pid != null && !pid.isEmpty()) {
                String killCommand = "kill -9 " + pid;
                Process killProcess = Runtime.getRuntime().exec(killCommand);
                killProcess.waitFor();
                System.out.println("Process " + pid + " has been killed.");
            } else {
                System.out.println("No matching process found.");
                throw new Exception("No matching process found.");
            }

            //Send requests to other nodes
            for (int i = 0; i < 40; i++) {
                int port = (int) (Math.random() * ((testDynamoClient.ports.length - 1) + 1));
                DynamoRequestPacket dynamoRequestPacket = i % 2 == 0 ? TESTDynamoClient.makePutRequest(testDynamoClient, -1) : TESTDynamoClient.makeGetRequest(testDynamoClient, -1);
                testDynamoClient.sendAppRequest(dynamoRequestPacket, testDynamoClient.ports[port]);
                Thread.sleep(1000);
            }

            Thread.sleep(5000);
            //Restart AR3
            String startCommand = System.getProperty("user.dir") + "/bin/gpServer.sh" + " start AR3";
            System.out.println("executing "+startCommand);
            Process startProcess = Runtime.getRuntime().exec(startCommand);
            startProcess.waitFor();

            //Send a PUT request to AR0. AR3 receives PUT_FWD with a higher checkpoint version. Triggers state transfer
            testDynamoClient.sendAppRequest(TESTDynamoClient.makePutRequest(testDynamoClient, 0), testDynamoClient.ports[0]);
            Thread.sleep(500);

            //Check if state of AR3 is most recent
            List<CheckpointLog> checkpointFiles = mapJsonFilesToObjects(getJsonFiles(System.getProperty("user.dir") + "/logs/DAGLogs/"), CheckpointLog.class);
            Assert.assertTrue(isMaxVersion(checkpointFiles, 65058));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static List<File> getJsonFiles(String directoryPath) {
        File directory = new File(directoryPath);
        File[] files = directory.listFiles((dir, name) -> name.toLowerCase().endsWith(".json"));

        if (files != null) {
            return List.of(files);
        } else {
            return new ArrayList<>();
        }
    }

    private static <T> List<T> mapJsonFilesToObjects(List<File> jsonFiles, Class<T> clazz) {
        ObjectMapper objectMapper = new ObjectMapper();
        List<T> objects = new ArrayList<>();

        for (File file : jsonFiles) {
            try {
                T obj = objectMapper.readValue(file, clazz);
                objects.add(obj);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return objects;
    }

    private boolean isMaxVersion(List<CheckpointLog> checkpointLogs, int serverId){
        int maxVersion = 0;
        int serverVersion = 0;
        for(CheckpointLog checkpointLog: checkpointLogs){
            if(checkpointLog.getMyId() == serverId){
                serverVersion = checkpointLog.getNoOfCheckpoints();
            }
            else {
                maxVersion = Math.max(maxVersion, checkpointLog.getNoOfCheckpoints());
            }
        }
        return serverVersion == maxVersion;
    }

    public static void main(String[] args) {
        Class<?> testClass = TESTMW.class;
        Method[] methods = testClass.getDeclaredMethods();

        for (Method method : methods) {
            if (method.isAnnotationPresent(Test.class)) {
                System.out.println("Test: " + method.getName() + " initialized");
            }
        }

        JUnitCore runner = new JUnitCore();
        Result r = runner.run(TESTDynamo.class);
        if (r.getFailures().isEmpty()) {
            System.out.println("All test cases passed");
        } else {
            for (Failure failure : r.getFailures()) {
                System.out.println("Test case failed: " + failure.getDescription());
            }
        }
    }
}

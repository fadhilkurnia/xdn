package edu.umass.cs.consistency.ClientCentric;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TESTMR {
    private final int noOfClients = 5;
    private AtomicBoolean keepRunning = new AtomicBoolean(true);
    public static AtomicBoolean passed = new AtomicBoolean(true);
    static final Logger log = TESTMRClient.log;
    private void threadSendingWriteRequests(TESTMRClient testmrClient){
        Runnable clientTask = () -> {
            while (keepRunning.get()) {
                try {
                    CCRequestPacket request = testmrClient.makeWriteRequest(testmrClient, CCRequestPacket.CCPacketType.MR_WRITE);
                    log.log(Level.INFO, "Sent write request from client 0");
                    testmrClient.sendAppRequest(testmrClient, request, testmrClient.ports[(int) (Math.random() * (testmrClient.ports.length))]);
                    Thread.sleep(500);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
        Thread clientThread = new Thread(clientTask);
        clientThread.start();
    }
    @Test
    public void test01_sendRequestToOneExtraServer() throws Exception{
        TESTMRClient testmrClient = new TESTMRClient();
        testmrClient.sendAppRequest(testmrClient, testmrClient.makeWriteRequest(testmrClient, CCRequestPacket.CCPacketType.MR_WRITE), 2000);
        Thread.sleep(500);
        testmrClient.sendAppRequest(testmrClient, testmrClient.makeWriteRequest(testmrClient, CCRequestPacket.CCPacketType.MR_WRITE), 2000);
        Thread.sleep(500);
        testmrClient.sendAppRequest(testmrClient, testmrClient.makeReadRequest(testmrClient, CCRequestPacket.CCPacketType.MR_READ), 2001);
        Thread.sleep(500);
        Assert.assertTrue(passed.get());
    }
    @Test
    public void test02_sendRequestToRandomServers() throws Exception{
        passed.set(true);
        TESTMRClient testmrClient = new TESTMRClient();
        testmrClient.sendAppRequest(testmrClient, testmrClient.makeWriteRequest(testmrClient, CCRequestPacket.CCPacketType.MR_WRITE), testmrClient.ports[(int) (Math.random() * (testmrClient.ports.length))]);
        Thread.sleep(500);
        testmrClient.sendAppRequest(testmrClient, testmrClient.makeWriteRequest(testmrClient, CCRequestPacket.CCPacketType.MR_WRITE), testmrClient.ports[(int) (Math.random() * (testmrClient.ports.length))]);
        Thread.sleep(500);
        testmrClient.sendAppRequest(testmrClient, testmrClient.makeReadRequest(testmrClient, CCRequestPacket.CCPacketType.MR_READ), testmrClient.ports[(int) (Math.random() * (testmrClient.ports.length))]);
        Thread.sleep(500);
        Assert.assertTrue(passed.get());
    }
    @Test
    public void test03_sendRandomRequestsToRandomServers() throws Exception{
        passed.set(true);
        TESTMRClient testmrClient = new TESTMRClient();
        for (int i = 0; i < 100; i++) {
            CCRequestPacket request = i % 2 == 0 ? testmrClient.makeWriteRequest(testmrClient, CCRequestPacket.CCPacketType.MR_WRITE) : testmrClient.makeReadRequest(testmrClient, CCRequestPacket.CCPacketType.MR_READ);
            testmrClient.sendAppRequest(testmrClient, request, testmrClient.ports[(int) (Math.random() * (testmrClient.ports.length))]);
            Thread.sleep(500);
        }
        Assert.assertTrue(passed.get());
    }
    @Test
    public void test04_sendReadRequestFromTwoClientsToRandomServer() throws Exception {
        passed.set(true);
        ArrayList<TESTMRClient> clients = new ArrayList<TESTMRClient>();
        for (int i = 0; i < 2; i++) {
            clients.add(new TESTMRClient());
        }
        threadSendingWriteRequests(clients.get(0));
        for (int i = 0; i < 10; i++) {
            TESTMRClient mrClient = clients.get((int) (Math.random() * (2)));
            CCRequestPacket request = mrClient.makeReadRequest(mrClient, CCRequestPacket.CCPacketType.MR_READ);
            mrClient.sendAppRequest(mrClient, request, mrClient.ports[(int) (Math.random() * (mrClient.ports.length))]);
            Thread.sleep(500);
        }
        keepRunning.set(false);
        Assert.assertTrue(passed.get());
    }
    @Test
    public void test05_sendReadRequestFromMultipleClientsToRandomServer() throws Exception {
        passed.set(true);
        keepRunning.set(true);
        ArrayList<TESTMRClient> clients = new ArrayList<TESTMRClient>();
        for (int i = 0; i < noOfClients; i++) {
            clients.add(new TESTMRClient());
        }
        threadSendingWriteRequests(clients.get(0));
        for (int i = 0; i < 10; i++) {
            TESTMRClient mrClient = clients.get((int) (Math.random() * (noOfClients)));
            CCRequestPacket request = mrClient.makeReadRequest(mrClient, CCRequestPacket.CCPacketType.MR_READ);
            mrClient.sendAppRequest(mrClient, request, mrClient.ports[(int) (Math.random() * (mrClient.ports.length))]);
            Thread.sleep(500);
        }
        keepRunning.set(false);
        Assert.assertTrue(passed.get());
    }
    public static void main(String[] args) {
        Class<?> testClass = TESTMR.class;
        Method[] methods = testClass.getDeclaredMethods();

        for (Method method : methods) {
            if (method.isAnnotationPresent(Test.class)) {
                System.out.println("Test: " + method.getName() + " initialized");
            }
        }

        JUnitCore runner = new JUnitCore();
        Result r = runner.run(TESTMR.class);
        if(r.getFailures().isEmpty()){
            System.out.println("All test cases passed");
        }
        else {
            for (Failure failure : r.getFailures()) {
               System.out.println("Test case failed: "+failure.getDescription());
            }
        }
    }
}

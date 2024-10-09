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

public class TESTMW {
    private final int noOfClients = 5;
    private AtomicBoolean keepRunning = new AtomicBoolean(true);
    public static AtomicBoolean passed = new AtomicBoolean(true);
    static final Logger log = TESTMRClient.log;
    @Test
    public void test01_sendRequestToOneExtraServer() throws IOException{
        TESTMRClient testmrClient = new TESTMRClient();
        testmrClient.sendAppRequest(testmrClient, testmrClient.makeWriteRequest(testmrClient, CCRequestPacket.CCPacketType.MW_WRITE), 2000);
        testmrClient.sendAppRequest(testmrClient, testmrClient.makeWriteRequest(testmrClient, CCRequestPacket.CCPacketType.MW_WRITE), 2000);
        testmrClient.sendAppRequest(testmrClient, testmrClient.makeWriteRequest(testmrClient, CCRequestPacket.CCPacketType.MW_WRITE), 2001);
        Assert.assertTrue(passed.get());
    }
    @Test
    public void test02_sendRequestToRandomServers() throws IOException{
        passed.set(true);
        TESTMRClient testmrClient = new TESTMRClient();
        testmrClient.sendAppRequest(testmrClient, testmrClient.makeWriteRequest(testmrClient, CCRequestPacket.CCPacketType.MW_WRITE), testmrClient.ports[(int) (Math.random() * (testmrClient.ports.length))]);
        testmrClient.sendAppRequest(testmrClient, testmrClient.makeWriteRequest(testmrClient, CCRequestPacket.CCPacketType.MW_WRITE), testmrClient.ports[(int) (Math.random() * (testmrClient.ports.length))]);
        testmrClient.sendAppRequest(testmrClient, testmrClient.makeWriteRequest(testmrClient, CCRequestPacket.CCPacketType.MW_WRITE), testmrClient.ports[(int) (Math.random() * (testmrClient.ports.length))]);
        Assert.assertTrue(passed.get());
    }
    @Test
    public void test03_sendRandomRequestsToRandomServers() throws IOException{
        passed.set(true);
        TESTMRClient testmrClient = new TESTMRClient();
        for (int i = 0; i < 100; i++) {
            CCRequestPacket request = i % 2 == 0 ? testmrClient.makeWriteRequest(testmrClient, CCRequestPacket.CCPacketType.MW_WRITE) : testmrClient.makeReadRequest(testmrClient, CCRequestPacket.CCPacketType.MW_READ);
            testmrClient.sendAppRequest(testmrClient, request, testmrClient.ports[(int) (Math.random() * (testmrClient.ports.length))]);
        }
        Assert.assertTrue(passed.get());
    }
    @Test
    public void test04_sendReadRequestFromTwoClientsToRandomServer() throws IOException {
        passed.set(true);
        ArrayList<TESTMRClient> clients = new ArrayList<TESTMRClient>();
        for (int i = 0; i < 2; i++) {
            clients.add(new TESTMRClient());
        }
        for (int i = 0; i < 10; i++) {
            TESTMRClient mrClient = clients.get((int) (Math.random() * (2)));
            CCRequestPacket request = mrClient.makeWriteRequest(mrClient, CCRequestPacket.CCPacketType.MW_WRITE);
            mrClient.sendAppRequest(mrClient, request, mrClient.ports[(int) (Math.random() * (mrClient.ports.length))]);
        }
        keepRunning.set(false);
        Assert.assertTrue(passed.get());
    }
    @Test
    public void test05_sendReadRequestFromMultipleClientsToRandomServer() throws IOException {
        passed.set(true);
        keepRunning.set(true);
        ArrayList<TESTMRClient> clients = new ArrayList<TESTMRClient>();
        for (int i = 0; i < noOfClients; i++) {
            clients.add(new TESTMRClient());
        }
        for (int i = 0; i < 10; i++) {
            TESTMRClient mrClient = clients.get((int) (Math.random() * (noOfClients)));
            CCRequestPacket request = mrClient.makeReadRequest(mrClient, CCRequestPacket.CCPacketType.MW_READ);
            mrClient.sendAppRequest(mrClient, request, mrClient.ports[(int) (Math.random() * (mrClient.ports.length))]);
        }
        keepRunning.set(false);
        Assert.assertTrue(passed.get());
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
        Result r = runner.run(TESTMW.class);
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

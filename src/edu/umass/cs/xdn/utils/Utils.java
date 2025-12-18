package edu.umass.cs.xdn.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterOutputStream;

public class Utils {

  public static int getUid() {
    int uid = 0;

    String getUidCommand = "id -u";
    ProcessBuilder pb = new ProcessBuilder(getUidCommand.split(" "));
    try {
      Process p = pb.start();
      int ret = p.waitFor();
      if (ret != 0) {
        return uid;
      }
      String output = new String(p.getInputStream().readAllBytes());
      output = output.replace("\n", "");
      uid = Integer.parseInt(output);
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
    }

    return uid;
  }

  public static int getGid() {
    int gid = 0;

    String getGidCommand = "id -g";
    ProcessBuilder pb = new ProcessBuilder(getGidCommand.split(" "));
    try {
      Process p = pb.start();
      int ret = p.waitFor();
      if (ret != 0) {
        return gid;
      }
      String output = new String(p.getInputStream().readAllBytes());
      output = output.replace("\n", "");
      gid = Integer.parseInt(output);
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
    }

    return gid;
  }

  public static byte[] compressBytes(byte[] input) throws IOException {
    byte[] compressedResult;
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    DeflaterOutputStream dos = new DeflaterOutputStream(os);
    dos.write(input);
    dos.flush();
    dos.close();
    compressedResult = os.toByteArray();

    return compressedResult;
  }

  public static byte[] decompressBytes(byte[] input) throws IOException {
    byte[] decompressed;
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    OutputStream ios = new InflaterOutputStream(os);
    ios.write(input);
    ios.flush();
    ios.close();
    decompressed = os.toByteArray();

    return decompressed;
  }
}

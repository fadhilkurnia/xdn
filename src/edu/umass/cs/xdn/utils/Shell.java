package edu.umass.cs.xdn.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class Shell {

  public static int runCommand(
      String command, boolean isSilent, Map<String, String> environmentVariables) {
    try {
      // prepare to start the command
      ProcessBuilder pb = new ProcessBuilder(command.split("\\s+"));
      if (isSilent) {
        pb.redirectOutput(ProcessBuilder.Redirect.DISCARD);
        pb.redirectError(ProcessBuilder.Redirect.DISCARD);
      }

      if (environmentVariables != null) {
        Map<String, String> processEnv = pb.environment();
        processEnv.putAll(environmentVariables);
      }

      if (!isSilent) {
        System.out.println("command: " + command);
        if (environmentVariables != null) {
          System.out.println(environmentVariables.toString());
        }
      }

      // run the command as a new OS process
      Process process = pb.start();

      // print out the output in stderr, if needed
      if (!isSilent) {
        InputStream inputStream = process.getInputStream();
        String output = new String(inputStream.readAllBytes(), StandardCharsets.ISO_8859_1);
        if (!output.isEmpty()) System.out.println("output:\n" + output);

        InputStream errStream = process.getErrorStream();
        String err = new String(errStream.readAllBytes(), StandardCharsets.ISO_8859_1);
        if (!err.isEmpty()) System.out.println("error:\n" + err);
      }

      int exitCode = process.waitFor();

      if (!isSilent) {
        System.out.println("exit code: " + exitCode);
      }

      return exitCode;
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public static int runCommand(String command, boolean isSilent) {
    return runCommand(command, isSilent, null);
  }

  public static int runCommand(String command) {
    return runCommand(command, true);
  }

  public static int runCommand(
      List<String> commandParts, boolean isSilent, Map<String, String> environmentVariables) {
    try {
      ProcessBuilder pb = new ProcessBuilder(commandParts);

      if (isSilent) {
        pb.redirectOutput(ProcessBuilder.Redirect.DISCARD);
        pb.redirectError(ProcessBuilder.Redirect.DISCARD);
      }

      if (environmentVariables != null) {
        Map<String, String> processEnv = pb.environment();
        processEnv.putAll(environmentVariables);
      }

      if (!isSilent) {
        System.out.println("command: " + String.join(" ", commandParts));
        if (environmentVariables != null) {
          System.out.println(environmentVariables.toString());
        }
      }

      Process process = pb.start();

      if (!isSilent) {
        try (InputStream inputStream = process.getInputStream()) {
          String output = new String(inputStream.readAllBytes(), StandardCharsets.ISO_8859_1);
          if (!output.isEmpty()) {
            System.out.println("output:\n" + output);
          }
        }

        try (InputStream errStream = process.getErrorStream()) {
          String err = new String(errStream.readAllBytes(), StandardCharsets.ISO_8859_1);
          if (!err.isEmpty()) {
            System.out.println("error:\n" + err);
          }
        }
      }

      int exitCode = process.waitFor();

      if (!isSilent) {
        System.out.println("exit code: " + exitCode);
      }

      return exitCode;

    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public static int runCommand(List<String> commandParts, boolean isSilent) {
    return runCommand(commandParts, isSilent, null);
  }

  public static int runCommand(List<String> commandParts) {
    return runCommand(commandParts, true);
  }

  public static ShellOutput runCommandWithOutput(
      String command, boolean isSilent, Map<String, String> environmentVariables) {
    try {
      ProcessBuilder pb = new ProcessBuilder(command.split("\\s+"));

      if (environmentVariables != null) {
        Map<String, String> processEnv = pb.environment();
        processEnv.putAll(environmentVariables);
      }

      if (!isSilent) {
        System.out.println("command: " + command);
        if (environmentVariables != null) {
          System.out.println(environmentVariables.toString());
        }
      }

      Process process = pb.start();

      String stdout =
          new String(process.getInputStream().readAllBytes(), StandardCharsets.ISO_8859_1);
      String stderr =
          new String(process.getErrorStream().readAllBytes(), StandardCharsets.ISO_8859_1);

      int exitCode = process.waitFor();

      return new ShellOutput(stdout, stderr, exitCode);
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public static ShellOutput runCommandWithOutput(String command, boolean isSilent) {
    return runCommandWithOutput(command, isSilent, null);
  }

  public static ShellOutput runCommandWithOutput(String command) {
    return runCommandWithOutput(command, true);
  }

  // process output is not printed out due to thread blocking
  public static int runCommandThread(
      String command, boolean isSilent, Map<String, String> environmentVariables) {
    Thread processThread =
        new Thread(
            () -> {
              try {
                ProcessBuilder pb = new ProcessBuilder(command.split("\\s+"));

                if (environmentVariables != null) {
                  Map<String, String> processEnv = pb.environment();
                  processEnv.putAll(environmentVariables);
                }

                if (!isSilent) {
                  System.out.println("command: " + command);
                  if (environmentVariables != null) {
                    System.out.println(environmentVariables.toString());
                  }
                }

                Process process = pb.start();

                Thread stdoutThread = null;
                Thread stderrThread = null;

                if (!isSilent) {
                  stdoutThread =
                      new Thread(
                          () -> {
                            try (BufferedReader reader =
                                new BufferedReader(
                                    new InputStreamReader(process.getInputStream()))) {
                              String line;
                              while ((line = reader.readLine()) != null) {
                                System.out.println("[stdout] " + line);
                              }
                            } catch (IOException e) {
                              e.printStackTrace();
                            }
                          });

                  stderrThread =
                      new Thread(
                          () -> {
                            try (BufferedReader reader =
                                new BufferedReader(
                                    new InputStreamReader(process.getErrorStream()))) {
                              String line;
                              while ((line = reader.readLine()) != null) {
                                System.err.println("[stderr] " + line);
                              }
                            } catch (IOException e) {
                              e.printStackTrace();
                            }
                          });

                  stdoutThread.start();
                  stderrThread.start();
                }

                int exitCode = process.waitFor();

                if (!isSilent) {
                  stdoutThread.join();
                  stderrThread.join();
                  System.out.println("exit code: " + exitCode);
                }
              } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
              }
            });

    try {
      processThread.start();
    } catch (IllegalThreadStateException e) {
      throw new RuntimeException(e);
    }

    return 0;
  }
}

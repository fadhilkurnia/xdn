package edu.umass.cs.xdn.utils;

public class ShellOutput {
  public final String stdout;
  public final String stderr;
  public final int exitCode;

  public ShellOutput(String stdout, String stderr, int exitCode) {
    this.stdout = stdout;
    this.stderr = stderr;
    this.exitCode = exitCode;
  }

  @Override
  public String toString() {
    return "exitCode: " + exitCode + "\nstdout:\n" + stdout + "\nstderr:\n" + stderr;
  }
}

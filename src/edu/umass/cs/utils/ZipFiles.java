package edu.umass.cs.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

/**
 * ZipFiles is a utility class to archive file or directory.
 * Adapted from:
 * - https://www.digitalocean.com/community/tutorials/java-zip-file-folder-example
 * - https://www.digitalocean.com/community/tutorials/java-unzip-file-example
 */
public class ZipFiles {

    /**
     * This method zips the directory
     *
     * @param dir        the directory to be archived.
     * @param zipDirName the target archived file location.
     */
    public static void zipDirectory(File dir, String zipDirName) {
	try {
	    List<String> files = populateFilesList(dir);
	    // now zip files one by one
	    // create ZipOutputStream to write to the zip file
	    FileOutputStream fos = new FileOutputStream(zipDirName);
	    ZipOutputStream zos = new ZipOutputStream(fos);

	    for (String filePath : files) {
		File file = new File(filePath);
		if (filePath.equals(dir.getAbsolutePath())) {
		    continue;
		}

		if (!file.exists()) {
		    continue;
		}

		if (file.isDirectory()) {
		    String entryName = filePath.substring(dir.getAbsolutePath().length() + 1) + File.separator;
		    zos.putNextEntry(new ZipEntry(entryName));
		    zos.closeEntry();  // Empty directory, just create the entry
		} else {
		    String entryName = filePath.substring(dir.getAbsolutePath().length() + 1);

		    zos.putNextEntry(new ZipEntry(entryName));
		    FileInputStream fis = new FileInputStream(filePath);
		    byte[] buffer = new byte[1024];
		    int len;

		    while ((len = fis.read(buffer)) > 0) {
			zos.write(buffer, 0, len);
		    }

		    zos.closeEntry();
		    fis.close();
		}
	    }
	    zos.close();
	    fos.close();
	} catch (IOException e) {
	    e.printStackTrace();
	}
    }

    /**
     * This method populates all the files in a directory to a List
     *
     * @param dir
     * @throws IOException
     */
    private static List<String> populateFilesList(File dir) throws IOException {
        List<String> result = new ArrayList<>();
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isFile()) {
                    result.add(file.getAbsolutePath());
                } else {
                    result.addAll(populateFilesList(file));
                }
            }
        }
        return result;
    }

    public static void unzip(String zipFilePath, String destDir) {
        File dir = new File(destDir);
        // create output directory if it doesn't exist
        if (!dir.exists()) dir.mkdirs();
        FileInputStream fis;
        //buffer for read and write data to file
        byte[] buffer = new byte[1024];
        try {
            fis = new FileInputStream(zipFilePath);
            ZipInputStream zis = new ZipInputStream(fis);
            ZipEntry ze = zis.getNextEntry();
            while (ze != null) {
                String fileName = ze.getName();
                File newFile = new File(destDir + File.separator + fileName);
                //create directories for sub directories in zip
                new File(newFile.getParent()).mkdirs();
                FileOutputStream fos = new FileOutputStream(newFile);
                int len;
                while ((len = zis.read(buffer)) > 0) {
                    fos.write(buffer, 0, len);
                }
                fos.close();
                //close this ZipEntry
                zis.closeEntry();
                ze = zis.getNextEntry();
            }
            //close last ZipEntry
            zis.closeEntry();
            zis.close();
            fis.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}

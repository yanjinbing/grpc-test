package com.alipay.sofa.jraft.util;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public final class FileHelper {
    private static final Logger LOG             = LoggerFactory.getLogger(FileHelper.class);

    public static Path createLink(String sourceDir, String destDir, String fileName) {
        final String sourcePath = sourceDir + File.separator + fileName;
        final String destPath = destDir + File.separator + fileName;
        return createLink(sourcePath, destPath);
    }

    public static Path createLink(String sourcePathString, String destPathString) {
        final Path sourcePath = Paths.get(sourcePathString);
        final Path destPath = Paths.get(destPathString);

        try {
            FileUtils.forceMkdirParent(new File(destPathString));
            return Files.createLink(destPath, sourcePath);
        } catch (final IOException e) {
            LOG.error("Fail to link {} to {}", sourcePath, destPath, e);
            return null;
        }
    }
}

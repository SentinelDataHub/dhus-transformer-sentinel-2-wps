/*
 * Data Hub Service (DHuS) - For Space data distribution.
 * Copyright (C) 2020 GAEL Systems
 *
 * This file is part of DHuS software sources.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package fr.gael.dhus.transformation;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DownloadManager
{
   private static final Logger LOGGER = LogManager.getLogger();

   private final Map<String, Future<URL>> downloads = new ConcurrentHashMap<>();

   private final ThreadPoolExecutor threadPool = new ThreadPoolExecutor(
         4 /* initial size */, 200 /* maximum size */,
         60, TimeUnit.SECONDS,
         new LinkedBlockingDeque<>(),
         runnable -> {
            Thread thread = new Thread(runnable, Sentinel2L2ATransformer.TRANSFORMER_NAME + "-download");
            thread.setDaemon(true);
            return thread;
         }
   );

   public void submitDownload(String uuid, URL remoteTarUrl)
   {
      downloads.computeIfAbsent(uuid, key -> threadPool.submit(() -> prepareOutput(remoteTarUrl)));

      LOGGER.info("Starting result download of Transformation '{}' ({})", uuid, remoteTarUrl);
      LOGGER.info("{} transformation downloads now running", threadPool.getActiveCount() + threadPool.getQueue().size());
   }

   public boolean hasDownload(String uuid)
   {
      return downloads.containsKey(uuid);
   }

   public boolean isDownloadDone(String uuid)
   {
      Future<URL> downloadFuture = downloads.get(uuid);
      return downloadFuture != null && downloadFuture.isDone();
   }

   public void removeDownload(String uuid)
   {
      downloads.remove(uuid);
   }

   public URL getDownloadResultURL(String uuid) throws InterruptedException, ExecutionException
   {
      try
      {
         URL resultUrl = downloads.get(uuid).get();
         LOGGER.info("Finished result download of Transformation '{}' ({})", uuid, resultUrl);
         LOGGER.info("{} transformation downloads now running", threadPool.getActiveCount() + threadPool.getQueue().size());
         return resultUrl;
      }
      catch(InterruptedException | ExecutionException e)
      {
         LOGGER.error("Failed result download of Transformation '{}'", uuid, e);
         throw e;
      }
   }

   /**
    * Downloads the result TAR and unpacks it in the configured tmp directory.
    */
   private URL prepareOutput(URL remoteTarUrl) throws IOException
   {
      try (TarArchiveInputStream input = new TarArchiveInputStream(remoteTarUrl.openStream()))
      {
         // skip unnecessary elements
         input.getNextTarEntry();
         TarArchiveEntry entry = input.getNextTarEntry();

         // generate output file
         String filename = entry.getName().split(File.separator)[1];
         filename = filename.replace(".SAFE.", ".");
         Path output = Configuration.getInstance().getTmpDirectory().resolve(filename);

         // write product data
         Files.copy(input, output, StandardCopyOption.REPLACE_EXISTING);

         // return URL
         return output.toUri().toURL();
      }
   }
}

/*
 * Data Hub Service (DHuS) - For Space data distribution.
 * Copyright (C) 2018-2020 GAEL Systems
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

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.dhus.api.JobStatus;
import org.dhus.api.transformation.ProductInfo;
import org.dhus.api.transformation.TransformationException;
import org.dhus.api.transformation.TransformationParameter;
import org.dhus.api.transformation.TransformationStatus;
import org.dhus.api.transformation.Transformer;

import fr.gael.dhus.webprocess.NonCriticalWPSException;
import fr.gael.dhus.webprocess.ProcessExec;
import fr.gael.dhus.webprocess.ProcessExecStatus;
import fr.gael.dhus.webprocess.WPSException;
import fr.gael.dhus.webprocess.sentinel2.Sentinel2WebProcessService;


public class Sentinel2L2ATransformer implements Transformer
{
   private static final Logger LOGGER = LogManager.getLogger();

   public static final String TRANSFORMER_NAME = "L2AOnDemand";
   private static final String TRANSFORMER_DESCRIPTION =
         "Generate a new product Sentinel-2 L2A from a Sentinel-2 L1C";

   // supported sentinel 2 process
   private static final String L2A_PROCESS_NAME = "l2a";

   // required metadata for transformations
   private static final String ATTRIBUTE_TILE_ID = "Level-1C PDI Identifier";

   // configuration
   private Configuration conf;

   // web processing service
   private Sentinel2WebProcessService wps;

   // download manager
   private DownloadManager downloadManager = new DownloadManager();

   @Override
   public String getName()
   {
      return TRANSFORMER_NAME;
   }

   @Override
   public String getDescription()
   {
      return TRANSFORMER_DESCRIPTION;
   }

   public Sentinel2WebProcessService getWPS()
   {
      return wps;
   }

   private void init() throws TransformationException
   {
      if (wps != null && conf != null)
      {
         return;
      }
      try
      {
         this.conf = Configuration.getInstance();

         // temporary directory
         Path tmpDir = this.conf.getTmpDirectory();
         if (!Files.exists(tmpDir) || !Files.isDirectory(tmpDir))
         {
            Files.createDirectories(tmpDir);
         }
         this.wps = Sentinel2WebProcessService.loadWPS(new URL(conf.getServiceUrl()));
      }
      catch (IOException | WPSException e)
      {
         throw new TransformationException("Service not reachable: " + e.getMessage(), e);
      }
   }

   @Override
   public void isTransformable(ProductInfo product, Map<String, String> parameters)
         throws TransformationException
   {
      init();

      // check parameters
      if(!parameters.isEmpty())
      {
         throw new TransformationException("This transformer takes no parameters.");
      }

      // check that product is Sentinel 2
      String satName = product.getMetadata().get("Satellite name");
      if (!"Sentinel-2".equals(satName))
      {
          throw new TransformationException("Product is not a Sentinel-2 product.");
      }

      // check product type
      String prodType = product.getMetadata().get("Product type");
      if (!"S2MSI1C".equals(prodType))
      {
          throw new TransformationException("Product is not a Sentinel-2 L1C product.");
      }

      // check product has PDI metadata
      if(!product.getMetadata().containsKey(ATTRIBUTE_TILE_ID))
      {
         throw new TransformationException("Product attribute missing: " + ATTRIBUTE_TILE_ID);
      }

      // check product sensing end date
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
      try
      {
         // product sensing date
         Date date = sdf.parse(product.getMetadata().get("Sensing stop"));

         // limit start
         Date limitStart = conf.getL2aDateStart();
         if (limitStart != null && date.before(limitStart))
         {
            // product too old
            LOGGER.debug("product :{} -- limit start:{} (too old)", date, limitStart);
            throw new TransformationException("Product is too old to allow its transformation.");
         }

         // limit end
         Date limitEnd = conf.getL2aDateEnd();
         if (limitEnd != null && date.after(limitEnd))
         {
            // product too young
            LOGGER.debug("product :{} -- limit end:{} (too young)", date, limitEnd);
            throw new TransformationException("The corresponding Level-2 product will be soon online as output of the nominal systematic processing flow. No dedicated On-Demand order is submitted. Please check the product availability later.");
         }
      }
      catch (ParseException e)
      {
         throw new TransformationException("Cannot parse product sensing date");
      }
   }

   @Override
   public List<TransformationParameter> getParameters()
   {
      return Collections.emptyList();
   }

   @Override
   public TransformationStatus submitTransformation(String transformationUuid, ProductInfo productInfo, Map<String, String> parameters)
         throws TransformationException
   {
      // initialize
      init();

      // note: this transformation doesn't take any parameters, ignore the argument

      try
      {
         // execute processing
         ProcessExec execution = wps.queryProcessExecution(L2A_PROCESS_NAME, productInfo.getMetadata().get(ATTRIBUTE_TILE_ID));

         // return status and data
         return new TransformationStatus(JobStatus.RUNNING, null, execution.getMonitoringUrl().toString());
      }
      catch (WPSException | RuntimeException e)
      {
         throw new TransformationException("Could not start transformation.", e);
      }
   }

   @Override
   // TODO check data is not null
   public TransformationStatus getTransformationStatus(String transformationUuid, String data) throws TransformationException
   {
      if (data == null)
      {
         throw new TransformationException("Execution status URL cannot be null");
      }

      // initialize
      init();

      try
      {
         // check existing downloads
         if(downloadManager.hasDownload(transformationUuid))
         {
            if(downloadManager.isDownloadDone(transformationUuid))
            {
               // download is done, transformation considered completed
               return new TransformationStatus(
                     JobStatus.COMPLETED,
                     downloadManager.getDownloadResultURL(transformationUuid),
                     data);
            }
            else
            {
               // download ongoing, transformation considered running
               return new TransformationStatus(JobStatus.RUNNING, null, data);
            }
         }

         // no download found, check status at WPS
         URL url = new URL(data);
         ProcessExecStatus executionStatus = wps.queryExecutionStatus(url);

         switch(executionStatus.getStatus())
         {
            case ACCEPTED:
            case STARTED:
               return new TransformationStatus(JobStatus.RUNNING, null, data);
            case SUCCEEDED:
               downloadManager.submitDownload(transformationUuid, new URL(executionStatus.getOutput()));
               return new TransformationStatus(JobStatus.RUNNING, null, data);
            case FAILED:
               return new TransformationStatus(JobStatus.FAILED, null, data);
            case PAUSED:
            default:
               return new TransformationStatus(JobStatus.UNKNOWN, null, data);
         }
      }
      catch (NonCriticalWPSException e)
      {
         LOGGER.warn("Could not retrieve status of Transformation '{}'", transformationUuid, e);
         LOGGER.warn("Transformation '{}' assumed RUNNING", transformationUuid);
         return new TransformationStatus(JobStatus.RUNNING, null, data);
      }
      catch (WPSException | IOException | RuntimeException e)
      {
         throw new TransformationException("Could not handle status of Transformation '"+transformationUuid+"'", e);
      }
      catch (InterruptedException e)
      {
         LOGGER.debug("Download interrupted for Transformation '{}' and should start again next run", transformationUuid);
         return new TransformationStatus(JobStatus.RUNNING, null, data);
      }
      catch (ExecutionException e)
      {
         throw new TransformationException("Could not download or extract result of Transformation '"+transformationUuid+"'", e);
      }
   }

   @Override
   public void terminateTransformation(String transformationUuid)
   {
      try
      {
         init();
         downloadManager.removeDownload(transformationUuid);
      }
      catch (TransformationException e)
      {
         LOGGER.error("Could not terminate Transformation '{}'", transformationUuid, e);
      }
   }
}

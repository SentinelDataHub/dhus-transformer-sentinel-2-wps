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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Properties;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class Configuration
{
   private static final Logger LOGGER = LogManager.getLogger();

   public static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

   private static final String CONFIGURATION_FILE = "/l2aOnDemand.properties";

   // service
   private static final String PROPERTY_WPS_URL = "wps.url";
   private static final String PROPERTY_L2A_USER_ID = "wps.l2a.userId";
   private static final String PROPERTY_L2A_PROCESSOR_VERSION = "wps.l2a.processor.version";
   private static final String PROPERTY_L2A_RESOLUTION = "wps.l2a.resolution";

   // data
   private static final String PROPERTY_TMP_DIR = "wps.tmp.dir";

   // accepted product
   private static final String PROPERTY_L2A_DATE_START = "wps.l2a.product.date.start";
   private static final String PROPERTY_L2A_DATE_END = "wps.l2a.product.date.stop";

   private static final Configuration INSTANCE;
   static
   {
      Properties properties = new Properties();
      try
      {
         properties.load(Configuration.class.getResourceAsStream(CONFIGURATION_FILE));
         INSTANCE = new Configuration(properties);
      }
      catch (Exception e)
      {
         throw new IllegalStateException("Failed to load 'L2A On Demand' transformer", e);
      }
   }

   public static Configuration getInstance()
   {
      return INSTANCE;
   }

   // service
   private final String serviceUrl;
   private final String l2aUserId;
   private final String l2aProcessorVersion;
   private final String l2aResolution;

   // data storage
   private final String tmpDir;

   // accepted products
   private final Date l2aDateStart;
   private final Date l2aDateStop;

   private Configuration(Properties properties)
   {
      // service
      this.serviceUrl = Objects.requireNonNull(properties.getProperty(PROPERTY_WPS_URL));
      this.l2aUserId = Objects.requireNonNull(properties.getProperty(PROPERTY_L2A_USER_ID));
      this.l2aProcessorVersion = Objects.requireNonNull(properties.getProperty(PROPERTY_L2A_PROCESSOR_VERSION));
      this.l2aResolution = Objects.requireNonNull(properties.getProperty(PROPERTY_L2A_RESOLUTION));

      // data storage
      this.tmpDir = properties.getProperty(PROPERTY_TMP_DIR, System.getProperty("java.io.tmpdir"));

      // accepted products
      String l2aDeltaStartProperty = (String) properties.get(PROPERTY_L2A_DATE_START);
      String l2aDeltaStopProperty = (String) properties.get(PROPERTY_L2A_DATE_END);
      this.l2aDateStart = getL2aDate(l2aDeltaStartProperty);
      this.l2aDateStop = getL2aDate(l2aDeltaStopProperty);
   }

   private Date getL2aDate(String propertyValue)
   {
      if (propertyValue != null)
      {
         if (propertyValue.toLowerCase().startsWith("now"))
         {
            String duration = propertyValue.substring(4);
            Duration durationValue = Duration.parse(duration);

            Date date = new Date(durationValue.getSeconds() * 1000);

            long dateFinal = new Date().getTime() - date.getTime();
            return new Date(dateFinal);
         }
         else
         {
            try
            {
               return DATE_FORMATTER.parse(propertyValue);
            }
            catch (ParseException e)
            {
               LOGGER.error("Cannot parse l2aDate");
               return null;
            }
         }
      }
      return null;
   }

   String getServiceUrl()
   {
      return serviceUrl;
   }

   public String getL2aUserId()
   {
      return l2aUserId;
   }

   public String getL2aProcessorVersion()
   {
      return l2aProcessorVersion;
   }

   public String getL2aResolution()
   {
      return l2aResolution;
   }

   Date getL2aDateStart()
   {
      return l2aDateStart;
   }

   Date getL2aDateEnd()
   {
      return l2aDateStop;
   }

   Path getTmpDirectory()
   {
      return Paths.get(tmpDir);
   }
}

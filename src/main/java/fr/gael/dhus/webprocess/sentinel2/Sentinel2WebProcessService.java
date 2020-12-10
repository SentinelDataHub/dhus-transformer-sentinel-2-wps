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
package fr.gael.dhus.webprocess.sentinel2;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.text.ParseException;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.http.client.utils.URIBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import fr.gael.dhus.transformation.Configuration;
import fr.gael.dhus.webprocess.NonCriticalWPSException;
import fr.gael.dhus.webprocess.ProcessExec;
import fr.gael.dhus.webprocess.ProcessExecStatus;
import fr.gael.dhus.webprocess.ProcessStatus;
import fr.gael.dhus.webprocess.WPSException;
import fr.gael.drb.DrbNode;
import fr.gael.drb.impl.xml.XmlNode;

public class Sentinel2WebProcessService
{
   private static final Logger LOGGER = LogManager.getLogger();

   // main http parameters
   private static final String PARAM_SERVICE = "SERVICE";
   private static final String PARAM_WPS = "WPS";
   private static final String PARAM_VERSION = "VERSION";
   private static final String PARAM_IDENTIFIER = "IDENTIFIER";
   private static final String PARAM_REQUEST = "REQUEST";
   private static final String PARAM_DATA_INPUTS = "DATAINPUTS";

   // other http parameters
   private static final String STORE_EXEC_RESPONSE = "storeExecuteResponse";
   private static final String STATUS = "status";
   private static final String LINEAGE = "lineage";

   // REQUEST parameter values
   private static final String REQ_CAPABILITIES = "GetCapabilities";
   @SuppressWarnings("unused")
   private static final String REQ_PROCESS_DESCRIPTION = "DescribeProcess";
   private static final String REQ_EXECUTE = "Execute";

   // other values
   private static final String TRUE = "true";

   private final URL url;
   private final String name;
   private final String description;
   private final String version;

   private Sentinel2WebProcessService(URL url, String name, String description, String version,
         Set<String> processNames)
   {
      this.url = url;
      this.name = name;
      this.description = description;
      this.version = version;
   }

   public String getLabel()
   {
      return name;
   }

   public String getDescription()
   {
      return description;
   }

   public String getVersion()
   {
      return version;
   }

   public static Sentinel2WebProcessService loadWPS(URL url) throws WPSException
   {
      LOGGER.debug("Loading web process service at: {}", url);
      Map<String, String> parameters = new HashMap<>();
      parameters.put(PARAM_SERVICE, PARAM_WPS);
      parameters.put(PARAM_REQUEST, REQ_CAPABILITIES);

      InputStream stream = performQueryRetry(url, parameters, 5);
      DrbNode xmlNode = new XmlNode(stream, null);

      DrbNode node = xmlNode.getNamedChild("ServiceIdentification", 1);
      String name = node.getNamedChild("Title", 1).getValue().toString();
      String description = node.getNamedChild("Abstract", 1).getValue().toString();
      String version = node.getNamedChild("ServiceTypeVersion", 1).getValue().toString();

      node = xmlNode.getNamedChild("ProcessOfferings", 1);
      Set<String> processes = new HashSet<>();
      for (int i = 0; i < node.getChildrenCount(); i++)
      {
         processes.add(node.getChildAt(i).getNamedChild("Title", 1).getValue().toString());
      }

      Sentinel2WebProcessService service = new Sentinel2WebProcessService(url, name, description, version, processes);
      LOGGER.debug("Web process service successfully loaded from: {}", url);
      return service;
   }

   /**
    * Launches a process request and returns a link allowing to monitor the requested process.
    *
    * @return an URL allowing to monitor the process.
    * @throws WPSException if the process can not be performed.
    */
   public ProcessExec queryProcessExecution(String processId, String tileId) throws WPSException
   {
      // prepare and format special process nested parameters
      String dataInputParameters = formatDataInputParameters(processId, tileId);

      // prepare http parameters
      Map<String, String> queryParameters = new HashMap<>();
      queryParameters.put(PARAM_SERVICE, PARAM_WPS);
      queryParameters.put(PARAM_REQUEST, REQ_EXECUTE);
      queryParameters.put(PARAM_VERSION, version);
      queryParameters.put(PARAM_IDENTIFIER, processId);
      queryParameters.put(STORE_EXEC_RESPONSE, TRUE);
      queryParameters.put(STATUS, TRUE);
      queryParameters.put(LINEAGE, TRUE);
      queryParameters.put(PARAM_DATA_INPUTS, dataInputParameters);

      // execute processing request
      InputStream response = performQueryRetry(url, queryParameters, 5);

      // return parsed response
      return toProcessExec(response);
   }

   /**
    * Returns status of a process execution.
    *
    * @param url monitor url of process execution
    * @return an object allowing to monitor a process execution
    * @throws WPSException
    */
   public ProcessExecStatus queryExecutionStatus(URL url) throws WPSException
   {
      InputStream stream = performQueryRetry(url, Collections.emptyMap(), 5);

      XmlNode xmlNode = new XmlNode(stream, null);
      DrbNode node = xmlNode.getNamedChild("Status", 1).getFirstChild();
      ProcessStatus status = ProcessStatus.fromString(node.getName());
      switch (status)
      {
         case ACCEPTED:
            return new ProcessExecStatus(status, 0, null);

         case PAUSED:
         case STARTED:
            int progression =
                  Integer.parseInt(node.getAttribute("percentCompleted").getValue().toString());
            return new ProcessExecStatus(status, progression, null);

         case SUCCEEDED:
            String output = xmlNode.getNamedChild("ProcessOutputs", 1).getFirstChild()
                  .getNamedChild("Data", 1)
                  .getNamedChild("LiteralData", 1).getValue().toString();
            return new ProcessExecStatus(status, 100, output);

         case FAILED:
            LOGGER.info("Web process service returned status '{}', message: {}", node.getName(), node.getValue());
            return new ProcessExecStatus(status, -1, null);

         default:
            throw new WPSException("Unknown status: " + node.getName());
      }
   }

   /**
    * Performs a GET request at the specified URL, using the specified parameters.
    *
    * @param serviceUrl the URL
    * @param parameters the HTTP GET parameters
    * @return the response stream
    * @throws WPSException
    * @throws NonCriticalWPSException
    */
   private static InputStream performQuery(URL serviceUrl, Map<String, String> parameters)
         throws WPSException
   {
      try
      {
         // prepare url of the query
         URIBuilder builder = new URIBuilder(serviceUrl.toURI());

         // set parameters of the query
         parameters.entrySet().forEach(entry -> builder.addParameter(entry.getKey(), entry.getValue()));

         // build
         URI uri = builder.build();

         // perform http request
         LOGGER.debug("try to perform request : {}", uri);
         HttpURLConnection connection = (HttpURLConnection) uri.toURL().openConnection();
         connection.setConnectTimeout(30000);
         connection.setReadTimeout(3000);

         // success?
         if (200 != connection.getResponseCode())
         {
            if(504 == connection.getResponseCode())
            {
               // error considered "normal"
               throw new NonCriticalWPSException("Sentinel-2 WPS raised non-critical unexpected status ("
                     + connection.getResponseCode() + "): " + connection.getResponseMessage());
            }
            else
            {
               throw new WPSException("Sentinel-2 WPS raised an unexpected status ("
                     + connection.getResponseCode() + "): " + connection.getResponseMessage());
            }
         }

         return connection.getInputStream();
      }
      catch (SocketTimeoutException e)
      {
         throw new NonCriticalWPSException("Sentinel-2 WPS is not responding: ", e);
      }
      catch (URISyntaxException | IOException e)
      {
         throw new WPSException("Cannot reach service at : " + serviceUrl + ": " + e.getMessage(), e);
      }
   }

   private static InputStream performQueryRetry(URL serviceUrl, Map<String, String> parameters, int retries) throws WPSException
   {
      for(int i = 0; i < retries; i++)
      {
         try
         {
            return performQuery(serviceUrl, parameters);
         }
         catch (NonCriticalWPSException e)
         {
            LOGGER.debug("Non-critical exception while performing query: {}, retrying", e.getMessage());
         }
      }
      // no successful attempt
      throw new WPSException("Cannot reach service at : " + serviceUrl);
   }

   private ProcessExec toProcessExec(InputStream response) throws WPSException
   {
      XmlNode xmlNode = new XmlNode(response, null);
      DrbNode statusNode = xmlNode.getNamedChild("Status", 1);
      if (statusNode != null && statusNode.getNamedChild("ProcessAccepted", 1) != null)
      {
         // process successfully submitted, reading status
         try
         {
            ProcessStatus status = ProcessStatus.fromString(statusNode.getFirstChild().getName());
            Date date = Configuration.DATE_FORMATTER.parse(statusNode.getAttribute("creationTime").getValue().toString());
            URL url = new URL(xmlNode.getAttribute("statusLocation").getValue().toString());
            return new ProcessExec(status, date, url);
         }
         catch (ParseException | MalformedURLException e)
         {
            throw new WPSException(e);
         }
      }
      else if (statusNode != null && statusNode.getNamedChild("ProcessFailed", 1) != null)
      {
         DrbNode exceptionNode = xmlNode.getNamedChild("ExceptionReport", 1);
         if (exceptionNode != null)
         {
            exceptionNode = exceptionNode.getNamedChild("Exception", 1);
            String exceptionCode = exceptionNode.getAttribute("exceptionCode").getValue().toString();
            String exceptionMessage = exceptionNode.getNamedChild("ExceptionText", 1).getValue().toString();
            throw new WPSException("Process failed: "+exceptionMessage + " (code: "+exceptionCode+")");
         }
      }

      // no supported status, process considered failed
      throw new WPSException("Process failed with unknown status: " + statusNode);
   }

   private String formatDataInputParameters(String processId, String tileId) throws WPSException
   {
      switch(processId)
      {
         case "l2a":
            return formatL2AProcessParameters(tileId);
         case "TCI":
            throw new WPSException("Process '" + processId + "' not implemented");
         default:
            throw new WPSException("Unknown process : " + processId);
      }
   }

   private String formatL2AProcessParameters(String tileId)
   {
      Configuration conf = Configuration.getInstance();

      // processor version
      String processVersion = conf.getL2aProcessorVersion();

      // user id
      String userId = conf.getL2aUserId();

      // resolution
      String resolution = conf.getL2aResolution();


      StringBuilder sb = new StringBuilder();
      sb.append("versionNumber=").append(processVersion).append(';');
      sb.append("userId=").append(userId).append(';');
      sb.append("userPriority=1;");
      sb.append("resolution=").append(resolution).append(';');
      sb.append("InputProducts=s2pdi://PDI=").append(tileId)
            .append("|DW_ID=").append(System.currentTimeMillis())
            .append("|DW_OPT=%7BfullDatatake:NO,fullSwath:NO%7D");

      // return formatted process parameters
      return sb.toString();
   }
}

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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

import io.netty.handler.codec.http.HttpMethod;
import org.mockserver.client.MockServerClient;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.mock.action.ExpectationResponseCallback;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.model.HttpStatusCode;
import org.mockserver.model.Parameter;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import fr.gael.dhus.webprocess.ProcessExec;
import fr.gael.dhus.webprocess.ProcessExecStatus;
import fr.gael.dhus.webprocess.ProcessStatus;
import fr.gael.dhus.webprocess.WPSException;

public class Sentinel2WPSTest
{
   private static final int PORT = 1234;
   private static final String RESPONSE_CAPABILITIES = "get_capabilities.xml";
   private static final String RESPONSE_DESCRIBE_PROCESS = "process_description_ok.xml";
   private static final String RESPONSE_EXECUTE_OK = "l2a_execute_ok.xml";
   private static final String RESPONSE_STATUS_PROCESSING = "l2a_status_processing.xml";
   private static final String RESPONSE_STATUS_COMPLETED = "l2a_status_completed.xml";

   private static final String DOMAIN = "localhost";
   private static final String REQUEST_PATH = "/cgi-bin/pywps.cgi";
   private static final String STATUS_PATH = "/cgi-bin/pywpsmon.cgi";
   private static final String SERVICE_URL = "http://" + DOMAIN + ":" + PORT + REQUEST_PATH;
   private static final String MONITORING_URL = "http://" + DOMAIN + ":" + PORT + STATUS_PATH;
   private static final String STATUS_ID_EXEC = "789c558e4d4bc4301445ff4d76493d";
   private static final String STATUS_ID_MONITORING_PROCESS = "1";
   private static final String STATUS_ID_MONITORING_COMPLETED = "2";

   private static final Parameter PARAM_GET_CAPABILITIES =
         new Parameter("REQUEST", "GetCapabilities");
   private static final Parameter PARAM_DESCRIBE_PROCESS =
         new Parameter("REQUEST", "DescribeProcess");
   private static final Parameter PARAM_EXECUTE_OK =
         new Parameter("REQUEST", "Execute");
   private static final Parameter PARAM_IDENTIFIER_L2A =
         new Parameter("IDENTIFIER", "l2a");

   private ClientAndServer mockServer;
   private MockServerClient request;
   private Sentinel2WebProcessService wps;

   private static byte[] resourceToByteArray(String resourcePath) throws IOException
   {
      InputStream input = ClassLoader.getSystemResourceAsStream(resourcePath);
      ByteArrayOutputStream output = new ByteArrayOutputStream();
      byte[] buffer = new byte[1024];
      int read;
      while ((read = input.read(buffer)) != -1)
      {
         output.write(buffer, 0, read);
      }

      input.close();
      output.close();

      return output.toByteArray();
   }

   @BeforeClass
   public void setUp() throws MalformedURLException, WPSException
   {
      Sentinel2PDGSPWSCallback callback = new Sentinel2PDGSPWSCallback();
      mockServer = ClientAndServer.startClientAndServer(PORT);
      request = new MockServerClient(DOMAIN, PORT);
      request.when(HttpRequest.request().withMethod(HttpMethod.GET.name()).withPath(REQUEST_PATH))
            .respond(callback);
      request.when(HttpRequest.request().withMethod(HttpMethod.GET.name()).withPath(STATUS_PATH))
            .respond(callback);

      wps = Sentinel2WebProcessService.loadWPS(new URL(SERVICE_URL));
   }

   @AfterClass
   public void tearDown()
   {
      request.stop();
      mockServer.stop();
   }

   @Test
   public void testInit()
   {
      Assert.assertEquals(wps.getLabel(), "DAG-B WPS Server");
      Assert.assertEquals(wps.getDescription(),
            "See http://pywps.wald.intevation.org and http://www.opengeospatial.org/standards/wps");
      Assert.assertEquals(wps.getVersion(), "1.0.0");
   }

   @Test(dependsOnMethods = {"testInit"})
   public void testExecuteOk() throws WPSException
   {
      ProcessExec execution = wps.queryProcessExecution("l2a", "S2B_OPER_MSI_L2A_TL_MPS__20180222T110232_A005038_T35TNK_N02.06");

      Assert.assertNotNull(execution);
      Assert.assertEquals(execution.getStatus(), ProcessStatus.ACCEPTED);
      Assert.assertEquals(execution.getCreationTime().getTime(), 1000);
      String expected = MONITORING_URL + "?Id=" + STATUS_ID_EXEC;
      Assert.assertEquals(execution.getMonitoringUrl().toString(), expected);
   }

   @Test(dependsOnMethods = {"testExecuteOk"})
   public void testStatusOnGoing() throws MalformedURLException, WPSException
   {
      ProcessExecStatus status = wps.queryExecutionStatus(
            new URL(MONITORING_URL + "?Id=" + STATUS_ID_MONITORING_PROCESS));
      Assert.assertNotNull(status);
      Assert.assertEquals(status.getStatus(), ProcessStatus.STARTED);
      Assert.assertEquals(status.getProgression(), 6);
      Assert.assertNull(status.getOutput());
   }

   @Test(dependsOnMethods = {"testExecuteOk"})
   public void testStatusCompleted() throws MalformedURLException, WPSException
   {
      ProcessExecStatus status = wps.queryExecutionStatus(
            new URL(MONITORING_URL + "?Id=" + STATUS_ID_MONITORING_COMPLETED));
      Assert.assertNotNull(status);
      Assert.assertEquals(status.getStatus(), ProcessStatus.SUCCEEDED);
      Assert.assertEquals(status.getProgression(), 100);
      Assert.assertEquals(status.getOutput(), "https://pac1dag.sentinel2.eo.esa.int/restsrv/" +
            "rest/ngEO?PdiID=S2_EPA__l2a_20180305_3.tar&userPriority=1&user=test_user_2");
   }

   private static class Sentinel2PDGSPWSCallback implements ExpectationResponseCallback
   {
      @Override
      public HttpResponse handle(HttpRequest httpRequest)
      {
         List<Parameter> parameters = httpRequest.getQueryStringParameterList();
         if (httpRequest.getMethod().getValue().equals("GET")
               && httpRequest.getPath().getValue().equals(REQUEST_PATH))
         {
            if (parameters.contains(PARAM_GET_CAPABILITIES))
            {
               return generateResponseFromResource(RESPONSE_CAPABILITIES);
            }

            if (parameters.contains(PARAM_DESCRIBE_PROCESS)
                  && parameters.contains(PARAM_IDENTIFIER_L2A))
            {
               return generateResponseFromResource(RESPONSE_DESCRIBE_PROCESS);
            }

            if (parameters.contains(PARAM_EXECUTE_OK) && parameters.contains(PARAM_IDENTIFIER_L2A))
            {
               return generateResponseFromResource(RESPONSE_EXECUTE_OK);
            }
         }

         if (httpRequest.getMethod().getValue().equals("GET")
               && httpRequest.getPath().getValue().equals(STATUS_PATH))
         {
            if (parameters.contains(new Parameter("Id", STATUS_ID_MONITORING_PROCESS)))
            {
               return generateResponseFromResource(RESPONSE_STATUS_PROCESSING);
            }

            if (parameters.contains(new Parameter("Id", STATUS_ID_MONITORING_COMPLETED)))
            {
               return generateResponseFromResource(RESPONSE_STATUS_COMPLETED);
            }
         }

         return HttpResponse.response().withStatusCode(HttpStatusCode.BAD_REQUEST_400.code())
               .withReasonPhrase("Unknown request");
      }

      private HttpResponse generateResponseFromResource(String resource)
      {
         try
         {
            return HttpResponse.response()
                  .withStatusCode(HttpStatusCode.OK_200.code())
                  .withHeader("Content-Type", "text/xml")
                  .withBody(new String(resourceToByteArray(resource)));
         }
         catch (IOException e)
         {
            return HttpResponse.response()
                  .withStatusCode(HttpStatusCode.INTERNAL_SERVER_ERROR_500.code())
                  .withReasonPhrase("Failed to load response: " + resource);
         }
      }
   }
}

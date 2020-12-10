/*
 * Data Hub Service (DHuS) - For Space data distribution.
 * Copyright (C) 2018 GAEL Systems
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

import org.testng.Assert;
import org.testng.annotations.Test;

public class ConfigurationTest
{
   @Test
   public void testConfiguration()
   {
      Configuration conf = Configuration.getInstance();
      Assert.assertNotNull(conf);
      Assert.assertEquals(conf.getServiceUrl(), "https://domain.cgi");
      Assert.assertEquals(conf.getL2aUserId(), "foo");
      Assert.assertEquals(conf.getL2aProcessorVersion(), "0.0.7");
      Assert.assertEquals(conf.getL2aResolution(), "60");
      Assert.assertEquals(conf.getTmpDirectory().toString(), "tmp");
   }
}

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
package fr.gael.dhus.webprocess;

public enum ProcessStatus
{
   ACCEPTED("ProcessAccepted"),
   STARTED("ProcessStarted"),
   PAUSED("ProcessPaused"),
   SUCCEEDED("ProcessSucceeded"),
   FAILED("ProcessFailed"),
   UNKNOWN("UNKNOWN");

   public static ProcessStatus fromString(String name) throws IllegalStateException
   {
      if (ProcessStatus.ACCEPTED.getValue().equals(name))
      {
         return ProcessStatus.ACCEPTED;
      }
      if (ProcessStatus.STARTED.getValue().equals(name))
      {
         return ProcessStatus.STARTED;
      }
      if (ProcessStatus.PAUSED.getValue().equals(name))
      {
         return ProcessStatus.PAUSED;
      }
      if (ProcessStatus.SUCCEEDED.getValue().equals(name))
      {
         return ProcessStatus.SUCCEEDED;
      }
      if (ProcessStatus.FAILED.getValue().equals(name))
      {
         return ProcessStatus.FAILED;
      }
      return ProcessStatus.UNKNOWN;
   }

   private final String value;

   ProcessStatus(String value)
   {
      this.value = value;
   }

   public String getValue()
   {
      return value;
   }
}

/*
 * *****************************************************************************
 * Copyright (C) 2014-2026 Dennis Sheirer
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 * ****************************************************************************
 */
package io.github.dsheirer.monitor;

import java.util.prefs.Preferences;

/**
 * Rod build: the scanner's address and API key that SDR-Trunk reports to (status, remote control from the scanner's
 * admin page). Its own setting, kept apart from the streaming configurations, so no stream is tied to the admin page.
 * Set in User Preferences > Application > Scanner. On the first start of a build that has it, it is filled in from
 * the first enabled Radio Scanner stream - what the reports used before.
 */
public final class ScannerLink
{
    private static final Preferences PREFS = Preferences.userNodeForPackage(ScannerLink.class);
    private static final String KEY_URL = "scanner.link.url";
    private static final String KEY_API_KEY = "scanner.link.apikey";
    private static final String KEY_SET = "scanner.link.set";   //true once filled in (by hand or from a stream)
    private static final String UPLOAD_PATH = "/api/call-upload";

    private ScannerLink()
    {
    }

    public static String getUrl()
    {
        return PREFS.get(KEY_URL, "");
    }

    public static String getApiKey()
    {
        return PREFS.get(KEY_API_KEY, "");
    }

    /**
     * True once it has been filled in, by hand or copied from a stream - after that, streams never change it.
     */
    public static boolean isSet()
    {
        return PREFS.getBoolean(KEY_SET, false);
    }

    public static boolean isUsable()
    {
        return !getUrl().isBlank() && !getApiKey().isBlank();
    }

    /**
     * Saves the scanner address (just the server, e.g. https://scanner.example.com - an /api/call-upload ending is
     * dropped) and API key.
     */
    public static void set(String url, String apiKey)
    {
        String u = url == null ? "" : url.trim();
        if(u.endsWith(UPLOAD_PATH))
        {
            u = u.substring(0, u.length() - UPLOAD_PATH.length());
        }
        while(u.endsWith("/"))
        {
            u = u.substring(0, u.length() - 1);
        }
        PREFS.put(KEY_URL, u);
        PREFS.put(KEY_API_KEY, apiKey == null ? "" : apiKey.trim());
        PREFS.putBoolean(KEY_SET, true);
        try
        {
            PREFS.flush();
        }
        catch(Exception e)
        {
            //saved with the next flush
        }
    }
}

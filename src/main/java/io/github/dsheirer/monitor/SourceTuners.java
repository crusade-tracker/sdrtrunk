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

import io.github.dsheirer.source.Source;

import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;

/**
 * Rod build: which tuner (dongle) handed out each channel source, so a call can report the dongle it was heard on.
 * Weak keys: an entry goes away with its source.
 */
public final class SourceTuners
{
    private static final Map<Source,String> TUNERS = Collections.synchronizedMap(new WeakHashMap<>());

    private SourceTuners()
    {
    }

    public static void put(Source source, String tunerName)
    {
        if(source != null && tunerName != null)
        {
            TUNERS.put(source, tunerName);
        }
    }

    /**
     * Tuner name for the source, or null when it isn't known (not a tuner channel, or from before the build)
     */
    public static String get(Source source)
    {
        return source == null ? null : TUNERS.get(source);
    }
}

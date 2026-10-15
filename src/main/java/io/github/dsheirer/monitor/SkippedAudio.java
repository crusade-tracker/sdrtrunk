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

import io.github.dsheirer.alias.Alias;
import io.github.dsheirer.audio.AudioSegment;
import io.github.dsheirer.identifier.Identifier;
import io.github.dsheirer.identifier.patch.PatchGroupIdentifier;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Rod build: calls to Skip Decode talkgroups whose audio was decoded anyway (a channel already running on that
 * frequency picked them up) and dropped. Sent to the scanner with the status reports as "skip_audio" events, so the
 * SDR-Trunk page can show how often it happens.
 */
public class SkippedAudio
{
    private static final ConcurrentLinkedQueue<Map<String,Object>> QUEUE = new ConcurrentLinkedQueue<>();
    private static final int MAX = 500;

    private SkippedAudio() {}

    public static void note(AudioSegment segment)
    {
        try
        {
            if(segment == null || !segment.hasAudio())
            {
                return;
            }
            Identifier to = segment.getIdentifierCollection().getToIdentifier();
            Map<String,Object> e = new LinkedHashMap<>();
            e.put("t", segment.getStartTimestamp());
            e.put("kind", "skip_audio");
            if(to != null)
            {
                e.put("to", to instanceof PatchGroupIdentifier p && p.getValue() != null && p.getValue().getPatchGroup() != null
                    ? "P" + p.getValue().getPatchGroup().getValue() : String.valueOf(to.getValue()));
                if(segment.getAliasList() != null)
                {
                    List<Alias> aliases = segment.getAliasList().getAliases(to);
                    if(aliases != null && !aliases.isEmpty())
                    {
                        e.put("toAlias", aliases.get(0).getName());
                    }
                }
            }
            e.put("details", "audio dropped (" + Math.round(segment.getDuration() / 100.0) / 10.0 + " s)");
            QUEUE.add(e);
            while(QUEUE.size() > MAX)
            {
                QUEUE.poll();
            }
        }
        catch(Exception ex)
        {
            //never let this get in the way of audio
        }
    }

    public static Map<String,Object> poll()
    {
        return QUEUE.poll();
    }
}

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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.dsheirer.alias.Alias;
import io.github.dsheirer.alias.AliasList;
import io.github.dsheirer.audio.AudioSegment;
import io.github.dsheirer.identifier.Form;
import io.github.dsheirer.identifier.Identifier;
import io.github.dsheirer.identifier.IdentifierClass;
import io.github.dsheirer.identifier.IdentifierCollection;
import io.github.dsheirer.identifier.Role;
import io.github.dsheirer.identifier.configuration.DecoderTypeConfigurationIdentifier;
import io.github.dsheirer.identifier.patch.PatchGroup;
import io.github.dsheirer.identifier.patch.PatchGroupIdentifier;
import io.github.dsheirer.identifier.radio.RadioIdentifier;
import io.github.dsheirer.identifier.talkgroup.TalkgroupIdentifier;
import io.github.dsheirer.module.decode.event.DecodeEventType;
import io.github.dsheirer.module.decode.event.IDecodeEvent;
import io.github.dsheirer.module.decode.p25.P25ChannelGrantEvent;
import io.github.dsheirer.module.decode.p25.phase2.enumeration.ScrambleParameters;
import io.github.dsheirer.module.decode.p25.reference.ServiceOptions;
import io.github.dsheirer.module.decode.p25.reference.VoiceServiceOptions;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Rod build: the extra details sent with each call upload to the scanner (form field "details", JSON). The scanner
 * shows them in a call's details window. Servers that don't know the field ignore it.
 *
 * What's in it, and where it comes from:
 *  - talkers: every radio heard talking in the call, in order, with the second it started (AudioSegment)
 *  - emergency / priority: from the P25 control channel grant for the talkgroup (only P25 grants carry these)
 *  - call type: group, patch, private (unit to unit) or phone - from what the call was addressed to
 *  - encrypted, timeslot, decoder, network ids (NAC, WACN, system, site... whatever the decoder knows)
 *  - patch: the patch group and its members, even when patches are streamed as their talkgroups
 */
public class CallDetails
{
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Map<String,Grant> GRANTS = new ConcurrentHashMap<>();
    private static final long GRANT_KEEP_MS = 10 * 60 * 1000;
    private static String sInstance;

    private record Grant(long start, long seen, boolean emergency, Integer priority, String from, boolean encrypted) {}

    private CallDetails() {}

    /**
     * Notes channel grants (emergency, priority) and emergency events, by talkgroup. Called for every decode event.
     */
    public static void noteEvent(IDecodeEvent event)
    {
        try
        {
            if(event == null || event.getIdentifierCollection() == null)
            {
                return;
            }
            IdentifierCollection ic = event.getIdentifierCollection();
            Identifier to = ic.getToIdentifier();
            if(to == null || to.getValue() == null)
            {
                return;
            }
            String key = key(to);
            Identifier from = ic.getFromIdentifier();
            String fromValue = from instanceof RadioIdentifier r && r.getValue() != null ? String.valueOf(r.getValue()) : null;
            long start = event.getTimeStart() > 0 ? event.getTimeStart() : System.currentTimeMillis();
            long now = System.currentTimeMillis();

            if(event instanceof P25ChannelGrantEvent grant && grant.hasServiceOptions())
            {
                ServiceOptions options = grant.getServiceOptions();
                Integer priority = options instanceof VoiceServiceOptions v ? v.getPriority() : null;
                Grant old = GRANTS.get(key);
                //Keep an emergency flag for the same call even if a later update doesn't repeat it
                boolean emergency = options.isEmergency() || (old != null && old.emergency() && old.start() == start);
                String by = options.isEmergency() ? fromValue : (emergency ? old.from() : fromValue);
                GRANTS.put(key, new Grant(start, now, emergency, priority, by, options.isEncrypted()));
            }
            else if(event.getEventType() == DecodeEventType.EMERGENCY)
            {
                Grant old = GRANTS.get(key);
                GRANTS.put(key, new Grant(start, now, true, old != null ? old.priority() : null, fromValue,
                    old != null && old.encrypted()));
            }
            else
            {
                return;
            }

            if(GRANTS.size() > 500)
            {
                GRANTS.values().removeIf(g -> now - g.seen() > GRANT_KEEP_MS);
            }
        }
        catch(Exception e)
        {
            //never let this get in the way of decoding
        }
    }

    private static String key(Identifier to)
    {
        if(to instanceof PatchGroupIdentifier p && p.getValue() != null && p.getValue().getPatchGroup() != null)
        {
            return "P" + p.getValue().getPatchGroup().getValue();
        }
        return to.getForm() + ":" + to.getValue();
    }

    /**
     * Builds the details JSON for a finished audio segment. Returns null if anything goes wrong - the upload goes
     * ahead without details.
     *
     * @param segment the finished audio segment (still has its talkers and aliases)
     * @param ic identifiers used for this upload (may be a single talkgroup out of a patch)
     * @param lengthMs audio length
     */
    public static String build(AudioSegment segment, IdentifierCollection ic, long lengthMs)
    {
        try
        {
            Map<String,Object> d = new LinkedHashMap<>();
            d.put("v", 1);
            d.put("instance", instance());
            long start = segment.getStartTimestamp();
            d.put("start", start);
            d.put("end", start + lengthMs);

            Identifier decoder = ic.getIdentifier(IdentifierClass.CONFIGURATION, Form.DECODER_TYPE, Role.ANY);
            if(decoder instanceof DecoderTypeConfigurationIdentifier dt && dt.getValue() != null)
            {
                d.put("decoder", dt.getValue().getDisplayString());
                String name = dt.getValue().name();
                if(name.contains("PHASE2") || name.contains("DMR"))
                {
                    d.put("timeslot", segment.getTimeslot());
                }
            }

            //What the call was addressed to. The segment's own TO is the real one (a patch stays a patch even when
            //patches are uploaded as their talkgroups).
            Identifier to = segment.getIdentifierCollection().getToIdentifier();
            if(to == null)
            {
                to = ic.getToIdentifier();
            }
            if(to != null)
            {
                d.put("callType", switch(to.getForm())
                {
                    case TALKGROUP -> "group";
                    case PATCH_GROUP -> "patch";
                    case RADIO -> "private";
                    case TELEPHONE_NUMBER -> "phone";
                    default -> to.getForm().name().toLowerCase();
                });
                if(to.getForm() == Form.RADIO || to.getForm() == Form.TELEPHONE_NUMBER)
                {
                    d.put("to", String.valueOf(to.getValue()));
                }
            }

            if(to instanceof PatchGroupIdentifier pgi && pgi.getValue() != null)
            {
                PatchGroup pg = pgi.getValue();
                Map<String,Object> patch = new LinkedHashMap<>();
                if(pg.getPatchGroup() != null)
                {
                    patch.put("group", pg.getPatchGroup().getValue());
                }
                List<Object> tgs = new ArrayList<>();
                for(TalkgroupIdentifier t: pg.getPatchedTalkgroupIdentifiers())
                {
                    tgs.add(t.getValue());
                }
                patch.put("talkgroups", tgs);
                List<Object> radios = new ArrayList<>();
                for(RadioIdentifier r: pg.getPatchedRadioIdentifiers())
                {
                    radios.add(r.getValue());
                }
                if(!radios.isEmpty())
                {
                    patch.put("radios", radios);
                }
                d.put("patch", patch);
            }

            //Talkers in order
            AliasList aliasList = segment.getAliasList();
            List<Map<String,Object>> talkers = new ArrayList<>();
            for(AudioSegment.Talker t: segment.getTalkers())
            {
                Map<String,Object> m = new LinkedHashMap<>();
                m.put("t", Math.max(0, Math.min(t.offset(), lengthMs)));
                m.put("id", t.radio());
                if(aliasList != null && t.identifier() != null)
                {
                    List<Alias> aliases = aliasList.getAliases(t.identifier());
                    if(aliases != null && !aliases.isEmpty() && aliases.get(0).getName() != null)
                    {
                        m.put("alias", aliases.get(0).getName());
                    }
                }
                if(t.talkerAlias() != null && !t.talkerAlias().isBlank())
                {
                    m.put("ta", t.talkerAlias());
                }
                talkers.add(m);
            }
            if(!talkers.isEmpty())
            {
                d.put("talkers", talkers);
            }

            //Emergency / priority from the grant for this call
            if(to != null && to.getValue() != null)
            {
                Grant g = GRANTS.get(key(to));
                long end = start + lengthMs;
                if(g != null && g.seen() >= start - 30_000 && g.start() <= end + 2_000)
                {
                    if(g.emergency())
                    {
                        d.put("emergency", true);
                        if(g.from() != null)
                        {
                            d.put("emergencyBy", g.from());
                        }
                    }
                    //Encrypted comes from the grant: the audio segment's own flag was wrong on clear calls
                    if(g.encrypted())
                    {
                        d.put("encrypted", true);
                    }
                    if(g.priority() != null)
                    {
                        d.put("priority", g.priority());
                    }
                }
            }

            //Network ids the decoder knows. NAC, WACN and system in hex, site and RFSS as numbers (the way
            //RadioReference lists them)
            Map<String,String> network = new LinkedHashMap<>();
            for(Identifier id: segment.getIdentifierCollection().getIdentifiers())
            {
                if(id == null || id.getIdentifierClass() != IdentifierClass.NETWORK || id.getValue() == null)
                {
                    continue;
                }
                Object value = id.getValue();
                if(value instanceof ScrambleParameters sp)
                {
                    network.putIfAbsent("wacn", String.format("%05X", sp.getWACN()));
                    network.putIfAbsent("system", String.format("%03X", sp.getSystem()));
                    network.putIfAbsent("nac", String.format("%03X", sp.getNAC()));
                }
                else if(value instanceof Integer n)
                {
                    switch(id.getForm())
                    {
                        case NETWORK_ACCESS_CODE -> network.put("nac", String.format("%03X", n));
                        case WACN -> network.put("wacn", String.format("%05X", n));
                        case SYSTEM -> network.put("system", String.format("%03X", n));
                        case SITE -> network.put("site", String.valueOf(n));
                        case RF_SUBSYSTEM -> network.put("rfss", String.valueOf(n));
                        case LOCATION_REGISTRATION_AREA -> network.put("lra", String.valueOf(n));
                        default -> { }
                    }
                }
            }
            if(!network.isEmpty())
            {
                d.put("network", network);
            }

            //Voice quality (P25 Phase 1): frames, frames the codec had to repeat or mute, bit errors found
            if(segment.getVoiceFrames() > 0)
            {
                Map<String,Object> voice = new LinkedHashMap<>();
                voice.put("frames", segment.getVoiceFrames());
                voice.put("bad", segment.getBadVoiceFrames());
                voice.put("bits", segment.getVoiceBitErrors());
                d.put("voice", voice);
            }

            //Why the call ended (P25 Phase 1): channel state it changed to, last message received, ms since last voice frame
            if(segment.getEndState() != null)
            {
                Map<String,Object> end = new LinkedHashMap<>();
                end.put("state", segment.getEndState());
                if(segment.getEndLastMessage() != null)
                {
                    end.put("last", segment.getEndLastMessage());
                }
                if(segment.getEndGapMs() >= 0)
                {
                    end.put("gapMs", segment.getEndGapMs());
                }
                d.put("ended", end);   //not "end" - that key is the call's end time
            }

            return MAPPER.writeValueAsString(d);
        }
        catch(Exception e)
        {
            return null;
        }
    }

    private static String instance()
    {
        if(sInstance == null)
        {
            try
            {
                sInstance = InetAddress.getLocalHost().getHostName();
            }
            catch(Exception e)
            {
                sInstance = "sdrtrunk";
            }
        }
        return sInstance;
    }
}

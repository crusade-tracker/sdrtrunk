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
import com.fasterxml.jackson.dataformat.xml.JacksonXmlModule;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import io.github.dsheirer.module.ProcessingChain;
import io.github.dsheirer.alias.Alias;
import io.github.dsheirer.alias.AliasList;
import io.github.dsheirer.alias.AliasModel;
import io.github.dsheirer.audio.broadcast.BroadcastConfiguration;
import io.github.dsheirer.audio.broadcast.BroadcastModel;
import io.github.dsheirer.audio.broadcast.rdioscanner.RdioScannerConfiguration;
import io.github.dsheirer.channel.metadata.ChannelMetadata;
import io.github.dsheirer.channel.metadata.ChannelMetadataModel;
import io.github.dsheirer.controller.channel.Channel;
import io.github.dsheirer.controller.channel.ChannelEvent;
import io.github.dsheirer.controller.channel.ChannelException;
import io.github.dsheirer.controller.channel.ChannelModel;
import io.github.dsheirer.controller.channel.ChannelProcessingManager;
import io.github.dsheirer.alias.id.priority.Priority;
import io.github.dsheirer.identifier.Form;
import io.github.dsheirer.identifier.Identifier;
import io.github.dsheirer.identifier.IdentifierClass;
import io.github.dsheirer.identifier.Role;
import io.github.dsheirer.identifier.IdentifierCollection;
import io.github.dsheirer.module.decode.event.DecodeEventType;
import io.github.dsheirer.module.decode.event.IDecodeEvent;
import io.github.dsheirer.playlist.PlaylistManager;
import io.github.dsheirer.properties.SystemProperties;
import io.github.dsheirer.source.config.SourceConfigTuner;
import io.github.dsheirer.source.config.SourceConfigTunerMultipleFrequency;
import io.github.dsheirer.source.tuner.configuration.TunerConfiguration;
import io.github.dsheirer.source.tuner.Tuner;
import io.github.dsheirer.source.tuner.TunerController;
import io.github.dsheirer.source.tuner.manager.DiscoveredTuner;
import io.github.dsheirer.source.tuner.manager.TunerManager;
import io.github.dsheirer.source.tuner.manager.TunerStatus;
import io.github.dsheirer.source.tuner.ui.DiscoveredTunerModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.EventQueue;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Rod build: sends a small status report (tuners, running channels, problem events, CPU/memory) to the Rdio Scanner
 * server every few seconds, so the scanner's admin page can show what SDR-Trunk is doing without a remote desktop.
 *
 * Uses the first enabled Rdio Scanner streaming configuration: same server and API key as the call uploads, posted
 * to /api/sdrtrunk/status on that server. Outbound only - nothing listens here. If there is no Rdio Scanner
 * streaming configuration, or the server doesn't answer, nothing happens apart from an occasional log line.
 *
 * Remote control (stage 2): the server's reply to a report can carry commands queued by the admin - start, stop or
 * restart a channel; enable, disable or restart a tuner; set a tuner's PPM. They are carried out here the same way
 * SDR-Trunk's own buttons do them, and the result of each goes back with the next report.
 */
public class StatusReporter
{
    private static final Logger mLog = LoggerFactory.getLogger(StatusReporter.class);
    private static final String STATUS_PATH = "/api/sdrtrunk/status";
    private static final int INTERVAL_SECONDS = 5;
    private static final int MAX_QUEUED_EVENTS = 2000;

    private final TunerManager mTunerManager;
    private final ChannelProcessingManager mChannelProcessingManager;
    private final BroadcastModel mBroadcastModel;
    private final AliasModel mAliasModel;
    private final ChannelModel mChannelModel;
    private final PlaylistManager mPlaylistManager;
    private final ConcurrentLinkedQueue<Map<String,Object>> mResults = new ConcurrentLinkedQueue<>();
    private final Set<String> mDoneCommands = Collections.synchronizedSet(new LinkedHashSet<>());
    //Talkgroups heard in calls that don't reach the scanner (no alias, or alias not streamed) - sent with each report
    private final Map<String,Map<String,Object>> mUnstreamed = new ConcurrentHashMap<>();
    private volatile Set<String> mStreamNames = Set.of();
    //Which playlist channels each tuner was carrying (last seen while it was working), so Enable / Restart can bring
    //them back - SDR-Trunk stops a tuner's channels when it's disabled or fails, and doesn't restart them itself
    private final Map<String,List<Integer>> mTunerChannels = new ConcurrentHashMap<>();
    private final ObjectMapper mMapper = new ObjectMapper();
    private final HttpClient mHttpClient = HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_1_1)   //no h2c upgrade - the scanner's main port treats upgrades as websockets
        .connectTimeout(Duration.ofSeconds(5))
        .build();
    private final ConcurrentLinkedQueue<Map<String,Object>> mEvents = new ConcurrentLinkedQueue<>();
    private final Set<IDecodeEvent> mSeenEvents = Collections.synchronizedSet(Collections.newSetFromMap(new WeakHashMap<>()));
    private final Map<String,String> mLastTunerStatus = new HashMap<>();
    private final long mStartMillis = System.currentTimeMillis();
    private final String mInstance;
    private ScheduledExecutorService mExecutor;
    //While someone has the scanner's SDR-Trunk page open, the scanner asks for a report every second instead of every 5
    private volatile long mFastUntil = 0;
    private static final int FAST_INTERVAL_SECONDS = 1;
    private static final int MAX_EVENTS = 300;
    private String mLastProblem;

    public StatusReporter(TunerManager tunerManager, PlaylistManager playlistManager)
    {
        mTunerManager = tunerManager;
        mChannelProcessingManager = playlistManager.getChannelProcessingManager();
        mBroadcastModel = playlistManager.getBroadcastModel();
        mAliasModel = playlistManager.getAliasModel();
        mChannelModel = playlistManager.getChannelModel();
        mPlaylistManager = playlistManager;

        String host;
        try
        {
            host = InetAddress.getLocalHost().getHostName();
        }
        catch(Exception e)
        {
            host = "sdrtrunk";
        }
        mInstance = host;
    }

    public void start()
    {
        mChannelProcessingManager.addDecodeEventListener(this::receiveDecodeEvent);
        mChannelProcessingManager.addChannelEventListener(this::receiveChannelEvent);

        mExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "sdrtrunk status reporter");
            t.setDaemon(true);
            return t;
        });
        mExecutor.schedule(this::reportAndReschedule, 10, TimeUnit.SECONDS);
        mLog.info("Status reporter started - reports to the Rdio Scanner server " + STATUS_PATH);
    }

    private void reportAndReschedule()
    {
        try
        {
            report();
        }
        finally
        {
            ScheduledExecutorService executor = mExecutor;
            if(executor != null && !executor.isShutdown())
            {
                executor.schedule(this::reportAndReschedule,
                    System.currentTimeMillis() < mFastUntil ? FAST_INTERVAL_SECONDS : INTERVAL_SECONDS, TimeUnit.SECONDS);
            }
        }
    }

    public void stop()
    {
        if(mExecutor != null)
        {
            mExecutor.shutdownNow();
            mExecutor = null;
        }
    }

    // ---------------------------------------------------------------- events

    private void receiveDecodeEvent(IDecodeEvent event)
    {
        try
        {
            DecodeEventType type = event.getEventType();
            String kind = switch(type)
            {
                case CALL_NO_TUNER -> "no_tuner";
                case CALL_DO_NOT_MONITOR -> "do_not_monitor";
                case CALL_ENCRYPTED, CALL_GROUP_ENCRYPTED, CALL_PATCH_GROUP_ENCRYPTED, CALL_UNIT_TO_UNIT_ENCRYPTED,
                     CALL_INTERCONNECT_ENCRYPTED -> "encrypted";
                case EMERGENCY -> "emergency";
                default -> null;
            };

            //Calls this build skipped for Do Not Monitor arrive as normal call events with an IGNORED note
            if(kind == null && event.getDetails() != null &&
                (event.getDetails().startsWith(io.github.dsheirer.module.decode.p25.P25TrafficChannelManager.IGNORED_DO_NOT_MONITOR) ||
                 event.getDetails().startsWith(io.github.dsheirer.module.decode.p25.P25TrafficChannelManager.IGNORED_SKIP_DECODE)))
            {
                kind = "do_not_monitor";
            }

            //The same event object is updated and re-sent while a call continues - count it once
            if(kind == null)
            {
                if((type == DecodeEventType.CALL_GROUP || type == DecodeEventType.CALL) && mSeenEvents.add(event))
                {
                    checkStreamed(event);
                }
                return;
            }
            if(!mSeenEvents.add(event))
            {
                return;
            }

            Map<String,Object> e = new LinkedHashMap<>();
            e.put("t", event.getTimeStart() > 0 ? event.getTimeStart() : System.currentTimeMillis());
            e.put("kind", kind);
            if(event.getChannelDescriptor() != null)
            {
                e.put("freq", event.getChannelDescriptor().getDownlinkFrequency());
            }
            IdentifierCollection ic = event.getIdentifierCollection();
            if(ic != null)
            {
                Identifier to = ic.getToIdentifier();
                if(to != null)
                {
                    e.put("to", String.valueOf(to.getValue()));
                    e.put("toAlias", aliasName(ic, to));
                }
            }
            e.put("details", event.getDetails());
            queue(e);
        }
        catch(Exception ex)
        {
            //Never let reporting break decoding
        }
    }

    private void receiveChannelEvent(ChannelEvent event)
    {
        try
        {
            //Traffic channels that can't get a tuner already arrive as "Call - No Tuner" decode events
            if(event.getEvent() == ChannelEvent.Event.NOTIFICATION_PROCESSING_START_REJECTED &&
                event.getChannel() != null && !event.getChannel().isTrafficChannel())
            {
                Channel c = event.getChannel();
                Map<String,Object> e = new LinkedHashMap<>();
                e.put("t", System.currentTimeMillis());
                e.put("kind", "start_rejected");
                e.put("channel", c.getName());
                e.put("site", c.getSite());
                e.put("system", c.getSystem());
                e.put("details", event.getDescription());
                queue(e);
            }
        }
        catch(Exception ex)
        {
            //ignore
        }
    }

    /**
     * Notes a talkgroup call that won't reach the scanner: no alias for the talkgroup, or its alias has no broadcast
     * channel for the Rdio Scanner stream. Talkgroups set to Do Not Monitor (Listen off) are left out - that's on purpose.
     */
    private void checkStreamed(IDecodeEvent event)
    {
        IdentifierCollection ic = event.getIdentifierCollection();
        if(ic == null)
        {
            return;
        }
        Identifier to = ic.getToIdentifier();
        if(to == null || to.getForm() != Form.TALKGROUP)
        {
            return;
        }

        List<Alias> aliases = List.of();
        AliasList list = mAliasModel.getAliasList(ic);
        if(list != null)
        {
            List<Alias> found = list.getAliases(to);
            if(found != null)
            {
                aliases = found;
            }
        }

        String reason;
        if(aliases.isEmpty())
        {
            reason = "no alias";
        }
        else
        {
            Set<String> streams = mStreamNames;
            if(streams.isEmpty())
            {
                streams = streamNames();
            }
            for(Alias alias: aliases)
            {
                if(alias.getPlaybackPriority() == Priority.DO_NOT_MONITOR || alias.isSkipDecode())
                {
                    return;
                }
                for(String stream: streams)
                {
                    if(alias.hasBroadcastChannel(stream))
                    {
                        return;   //streamed - the scanner gets it
                    }
                }
            }
            reason = "not streamed";
        }

        String system = configValue(ic, Form.SYSTEM);
        String site = configValue(ic, Form.SITE);
        String tg = String.valueOf(to.getValue());
        String key = system + "|" + site + "|" + tg;
        String alias = aliases.isEmpty() ? null : aliases.get(0).getName();
        Long freq = event.getChannelDescriptor() != null ? event.getChannelDescriptor().getDownlinkFrequency() : null;
        long t = event.getTimeStart() > 0 ? event.getTimeStart() : System.currentTimeMillis();

        mUnstreamed.compute(key, (k, m) -> {
            if(m == null)
            {
                m = new LinkedHashMap<>();
                m.put("system", system);
                m.put("site", site);
                m.put("tg", tg);
                m.put("count", 0);
                m.put("first", t);
            }
            m.put("alias", alias);
            m.put("reason", reason);
            m.put("freq", freq);
            m.put("last", t);
            m.put("count", (Integer)m.get("count") + 1);
            return m;
        });
    }

    private static String configValue(IdentifierCollection ic, Form form)
    {
        Identifier id = ic.getIdentifier(IdentifierClass.CONFIGURATION, form, Role.ANY);
        return id != null && id.getValue() != null ? String.valueOf(id.getValue()) : null;
    }

    /**
     * Names of the enabled Rdio Scanner streams - an alias reaches the scanner when it has a broadcast channel by one of
     * these names
     */
    private Set<String> streamNames()
    {
        Set<String> names = new java.util.HashSet<>();
        try
        {
            for(BroadcastConfiguration config: new ArrayList<>(mBroadcastModel.getBroadcastConfigurations()))
            {
                if(config instanceof RdioScannerConfiguration && config.isEnabled() && config.getName() != null)
                {
                    names.add(config.getName());
                }
            }
        }
        catch(Exception e)
        {
            //changed while reading - next time
        }
        return names;
    }

    private void queue(Map<String,Object> e)
    {
        mEvents.add(e);
        while(mEvents.size() > MAX_QUEUED_EVENTS)
        {
            mEvents.poll();
        }
    }

    private String aliasName(IdentifierCollection ic, Identifier id)
    {
        try
        {
            AliasList list = mAliasModel.getAliasList(ic);
            if(list != null)
            {
                List<Alias> aliases = list.getAliases(id);
                if(aliases != null && !aliases.isEmpty())
                {
                    return aliases.get(0).getName();
                }
            }
        }
        catch(Exception e)
        {
            //ignore
        }
        return null;
    }

    // ---------------------------------------------------------------- report

    private RdioScannerConfiguration getConfiguration()
    {
        for(BroadcastConfiguration config: mBroadcastModel.getBroadcastConfigurations())
        {
            if(config instanceof RdioScannerConfiguration rdio && config.isEnabled() && rdio.getHost() != null &&
                !rdio.getHost().isBlank())
            {
                return rdio;
            }
        }
        return null;
    }

    private void report()
    {
        try
        {
            RdioScannerConfiguration config = getConfiguration();
            if(config == null)
            {
                problem("no enabled Radio Scanner streaming configuration - status reports are off");
                return;
            }

            URI status = URI.create(config.getHost().trim()).resolve(STATUS_PATH);

            List<Map<String,Object>> events = new ArrayList<>();
            Map<String,Object> e;
            while((e = mEvents.poll()) != null)
            {
                events.add(e);
            }

            Map<String,Object> body = new LinkedHashMap<>();
            body.put("key", config.getApiKey());
            body.put("instance", mInstance);
            body.put("version", SystemProperties.getInstance().getApplicationName());
            body.put("uptimeSec", (System.currentTimeMillis() - mStartMillis) / 1000);
            body.put("cpu", processCpu());
            Runtime rt = Runtime.getRuntime();
            body.put("memUsedMB", (rt.totalMemory() - rt.freeMemory()) / 1048576);
            body.put("memMaxMB", rt.maxMemory() / 1048576);
            mStreamNames = streamNames();
            List<Map<String,Object>> tuners = tuners(events);
            List<Map<String,Object>> configured = configuredChannels();
            rememberTunerChannels(tuners, configured);
            body.put("tuners", tuners);
            body.put("channels", channels());
            body.put("configured", configured);
            List<Map<String,Object>> unstreamed = new ArrayList<>();
            for(String k: new ArrayList<>(mUnstreamed.keySet()))
            {
                Map<String,Object> u = mUnstreamed.remove(k);
                if(u != null)
                {
                    unstreamed.add(u);
                }
            }
            body.put("unstreamed", unstreamed);
            body.put("events", events);
            List<Map<String,Object>> results = new ArrayList<>();
            Map<String,Object> r;
            while((r = mResults.poll()) != null)
            {
                results.add(r);
            }
            body.put("results", results);

            HttpRequest request = HttpRequest.newBuilder(status)
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(mMapper.writeValueAsString(body)))
                .build();

            HttpResponse<String> response = mHttpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if(response.statusCode() == 200)
            {
                if(mLastProblem != null)
                {
                    mLog.info("Status reports reaching " + status + " again");
                    mLastProblem = null;
                }
                runCommands(response.body());
            }
            else
            {
                requeue(events);
                results.forEach(mResults::add);
                unstreamed.forEach(u -> mUnstreamed.putIfAbsent(u.get("system") + "|" + u.get("site") + "|" + u.get("tg"), u));
                problem("server answered " + response.statusCode() + " from " + status);
            }
        }
        catch(Exception ex)
        {
            problem(ex.getClass().getSimpleName() + ": " + ex.getMessage());
        }
    }

    private void requeue(List<Map<String,Object>> events)
    {
        //A 404 means the scanner doesn't have the status endpoint - don't pile events up for it
        if(events.size() < MAX_QUEUED_EVENTS)
        {
            events.forEach(this::queue);
        }
    }

    /**
     * Logs a problem once until it changes, so a missing server doesn't flood the log every 5 seconds
     */
    private void problem(String message)
    {
        if(!message.equals(mLastProblem))
        {
            mLog.info("Status report not sent - " + message);
            mLastProblem = message;
        }
    }

    private Double processCpu()
    {
        try
        {
            if(ManagementFactory.getOperatingSystemMXBean() instanceof com.sun.management.OperatingSystemMXBean os)
            {
                double load = os.getProcessCpuLoad();
                return load >= 0 ? Math.round(load * 1000) / 10.0 : null;
            }
        }
        catch(Throwable t)
        {
            //not available in this runtime
        }
        return null;
    }

    private List<Map<String,Object>> tuners(List<Map<String,Object>> events)
    {
        List<Map<String,Object>> out = new ArrayList<>();
        DiscoveredTunerModel model = mTunerManager.getDiscoveredTunerModel();

        for(int i = 0; i < model.getRowCount(); i++)
        {
            DiscoveredTuner dt = model.getDiscoveredTuner(i);
            if(dt == null)
            {
                continue;
            }

            Map<String,Object> m = new LinkedHashMap<>();
            m.put("id", dt.getId());
            TunerStatus status = dt.getTunerStatus();
            m.put("status", status != null ? status.name() : null);
            m.put("error", dt.hasErrorMessage() ? dt.getErrorMessage() : null);
            m.put("class", dt.getTunerClass() != null ? dt.getTunerClass().toString() : null);
            m.put("enabled", dt.isEnabled());

            if(dt.hasTuner())
            {
                Tuner tuner = dt.getTuner();
                m.put("name", tuner.getPreferredName());
                m.put("serial", tuner.getUniqueID());
                TunerController tc = tuner.getTunerController();
                if(tc != null)
                {
                    m.put("centre", tc.getFrequency());
                    m.put("min", tc.getMinimumFrequency());
                    m.put("max", tc.getMaximumFrequency());
                    m.put("sampleRate", (long)tc.getSampleRate());
                    m.put("usable", tc.getUsableBandwidth());
                    m.put("ppm", tc.getFrequencyCorrection());
                    m.put("ppmAuto", tc.getTunerFrequencyErrorManager() != null && tc.getTunerFrequencyErrorManager().isEnabled());
                    try
                    {
                        if(tc.getMeasuredFrequencyError() != 0)
                        {
                            m.put("ppmError", Math.round(tc.getPPMFrequencyError() * 10) / 10.0);
                        }
                    }
                    catch(Exception ex)
                    {
                        //not measured
                    }
                }
                if(tuner.getChannelSourceManager() != null)
                {
                    m.put("channels", tuner.getChannelSourceManager().getTunerChannelCount());
                }
            }

            //Report status changes (e.g. ENABLED -> ERROR) as events too, so they show in the problem list
            String now = m.get("status") + "|" + m.get("error");
            String before = mLastTunerStatus.put(dt.getId(), now);
            if(before != null && !before.equals(now) && status == TunerStatus.ERROR)
            {
                Map<String,Object> e = new LinkedHashMap<>();
                e.put("t", System.currentTimeMillis());
                e.put("kind", "tuner_error");
                e.put("tuner", m.get("name") != null ? m.get("name") : dt.getId());
                e.put("details", m.get("error"));
                events.add(e);
            }

            out.add(m);
        }

        return out;
    }

    private List<Map<String,Object>> channels() throws Exception
    {
        //The metadata model is changed on the Swing thread - read it there so the list can't change under us
        CompletableFuture<List<Map<String,Object>>> future = new CompletableFuture<>();
        EventQueue.invokeLater(() -> {
            try
            {
                future.complete(readChannels());
            }
            catch(Throwable t)
            {
                future.completeExceptionally(t);
            }
        });
        return future.get(3, TimeUnit.SECONDS);
    }

    private List<Map<String,Object>> readChannels()
    {
        List<Map<String,Object>> out = new ArrayList<>();
        ChannelMetadataModel model = mChannelProcessingManager.getChannelMetadataModel();

        for(int row = 0; row < model.getRowCount(); row++)
        {
            ChannelMetadata md = model.getChannelMetadata(row);
            if(md == null)
            {
                continue;
            }

            Map<String,Object> m = new LinkedHashMap<>();
            Channel channel = model.getChannelFromMetadata(md);
            if(channel != null)
            {
                m.put("system", channel.getSystem());
                m.put("site", channel.getSite());
                m.put("name", channel.getName());
                m.put("traffic", channel.isTrafficChannel());
                m.put("channelId", channel.getChannelID());
            }
            if(md.hasChannelConfigurationIdentifier())
            {
                m.put("channelName", String.valueOf(md.getChannelNameConfigurationIdentifier().getValue()));
            }
            if(md.hasFrequencyConfigurationIdentifier())
            {
                m.put("freq", md.getFrequencyConfigurationIdentifier().getValue());
            }
            if(md.hasDecoderStateIdentifier())
            {
                m.put("state", String.valueOf(md.getChannelStateIdentifier().getValue()));
            }
            if(md.hasDecoderTypeIdentifier())
            {
                m.put("decoder", String.valueOf(md.getDecoderTypeConfigurationIdentifier().getValue()));
            }
            if(md.hasTimeslot())
            {
                m.put("timeslot", md.getTimeslot());
            }
            if(md.hasFromIdentifier())
            {
                m.put("from", String.valueOf(md.getFromIdentifier().getValue()));
                m.put("fromAlias", firstAlias(md.getFromIdentifierAliases()));
            }
            if(md.hasToIdentifier())
            {
                m.put("to", String.valueOf(md.getToIdentifier().getValue()));
                m.put("toAlias", firstAlias(md.getToIdentifierAliases()));
            }
            out.add(m);
        }

        return out;
    }

    /**
     * Notes which running playlist channels sit inside each working tuner's band. Only updated while the tuner is working,
     * so after it's disabled or fails we still know what it was carrying.
     */
    @SuppressWarnings("unchecked")
    private void rememberTunerChannels(List<Map<String,Object>> tuners, List<Map<String,Object>> configured)
    {
        for(Map<String,Object> t: tuners)
        {
            if(!"ENABLED".equals(t.get("status")) || !(t.get("centre") instanceof Long centre) ||
                !(t.get("usable") instanceof Integer usable) || !(t.get("channels") instanceof Integer n) || n == 0)
            {
                continue;
            }
            List<Integer> ids = new ArrayList<>();
            for(Map<String,Object> c: configured)
            {
                if(Boolean.TRUE.equals(c.get("processing")) && c.get("freqs") instanceof List<?> freqs &&
                    freqs.stream().anyMatch(f -> f instanceof Long hz && Math.abs(hz - centre) <= usable / 2))
                {
                    ids.add((Integer)c.get("id"));
                }
            }
            if(!ids.isEmpty())
            {
                mTunerChannels.put(String.valueOf(t.get("id")), ids);
            }
        }
    }

    /**
     * Starts the playlist channels a tuner was carrying that aren't running now. Returns a note for the command result.
     */
    private String restartTunerChannels(String tunerId)
    {
        List<Integer> ids = mTunerChannels.get(tunerId);
        if(ids == null || ids.isEmpty())
        {
            return "";
        }
        try
        {
            Thread.sleep(1500);   //let the tuner finish starting
        }
        catch(InterruptedException ie)
        {
            return "";
        }
        List<String> started = new ArrayList<>();
        List<String> failed = new ArrayList<>();
        for(Integer id: ids)
        {
            try
            {
                Channel c = findChannel(String.valueOf(id));
                if(!c.isProcessing())
                {
                    startChannel(c);
                    started.add(c.getName());
                }
            }
            catch(Exception e)
            {
                failed.add(e.getMessage());
            }
        }
        StringBuilder sb = new StringBuilder();
        if(!started.isEmpty())
        {
            sb.append("; started its channels again: ").append(String.join(", ", started));
        }
        if(!failed.isEmpty())
        {
            sb.append("; couldn't start: ").append(String.join("; ", failed));
        }
        return sb.toString();
    }

    /**
     * Every channel in the playlist (not traffic channels), running or not, so the admin can start and stop them
     */
    private List<Map<String,Object>> configuredChannels()
    {
        List<Map<String,Object>> out = new ArrayList<>();
        List<Channel> channels;
        try
        {
            channels = new ArrayList<>(mChannelModel.getChannels());
        }
        catch(Exception e)
        {
            return out;   //changed while copying - next report will have it
        }

        for(Channel c: channels)
        {
            Map<String,Object> m = new LinkedHashMap<>();
            m.put("id", c.getChannelID());
            m.put("name", c.getName());
            m.put("system", c.getSystem());
            m.put("site", c.getSite());
            m.put("processing", c.isProcessing());
            m.put("autoStart", c.isAutoStart());
            if(c.getDecodeConfiguration() != null)
            {
                m.put("decoder", String.valueOf(c.getDecodeConfiguration().getDecoderType()));
            }
            if(c.getSourceConfiguration() instanceof SourceConfigTuner sct)
            {
                m.put("freqs", List.of(sct.getFrequency()));
            }
            else if(c.getSourceConfiguration() instanceof SourceConfigTunerMultipleFrequency sctm)
            {
                m.put("freqs", sctm.getFrequencies());
            }
            out.add(m);
        }
        return out;
    }

    // ---------------------------------------------------------------- remote control

    @SuppressWarnings("unchecked")
    private void runCommands(String responseBody)
    {
        List<Map<String,Object>> commands;
        try
        {
            Map<String,Object> reply = mMapper.readValue(responseBody, Map.class);
            if(Boolean.TRUE.equals(reply.get("fast")))
            {
                mFastUntil = System.currentTimeMillis() + 15000;
            }
            Object list = reply.get("commands");
            if(!(list instanceof List<?>) || ((List<?>)list).isEmpty())
            {
                return;
            }
            commands = (List<Map<String,Object>>)list;
        }
        catch(Exception e)
        {
            return;   //an older scanner answers without commands
        }

        for(Map<String,Object> command: commands)
        {
            String id = String.valueOf(command.get("id"));
            if(!mDoneCommands.add(id))
            {
                continue;   //already carried out
            }
            while(mDoneCommands.size() > 500)
            {
                synchronized(mDoneCommands)
                {
                    mDoneCommands.remove(mDoneCommands.iterator().next());
                }
            }

            String action = String.valueOf(command.get("action"));
            String target = String.valueOf(command.get("target"));
            Object value = command.get("value");
            Map<String,Object> result = new LinkedHashMap<>();
            result.put("id", command.get("id"));

            try
            {
                if(action.startsWith("query_"))
                {
                    //Read-only questions from the admin page: answered with data, not logged
                    result.put("ok", true);
                    result.put("message", "");
                    result.put("data", runQuery(action, target));
                    mResults.add(result);
                    continue;
                }
                String message = runCommand(action, target, value);
                result.put("ok", true);
                result.put("message", message);
                mLog.info("Remote command [" + action + "] on [" + target + "]: " + message);
            }
            catch(Exception e)
            {
                result.put("ok", false);
                result.put("message", e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
                mLog.info("Remote command [" + action + "] on [" + target + "] failed: " + result.get("message"));
            }
            mResults.add(result);
        }
    }

    private String runCommand(String action, String target, Object value) throws Exception
    {
        //Tuner changes update Swing tables through their listeners - do them on the Swing thread like the Tuners tab does
        if(action.startsWith("tuner_"))
        {
            CompletableFuture<String> future = new CompletableFuture<>();
            EventQueue.invokeLater(() -> {
                try
                {
                    future.complete(runAction(action, target, value));
                }
                catch(Throwable t)
                {
                    future.completeExceptionally(t);
                }
            });
            try
            {
                String message = future.get(30, TimeUnit.SECONDS);
                if(("tuner_enable".equals(action) && message.endsWith(" enabled")) ||
                   ("tuner_restart".equals(action) && message.contains(" restarted")))
                {
                    message += restartTunerChannels(target);
                }
                return message;
            }
            catch(java.util.concurrent.ExecutionException ee)
            {
                throw ee.getCause() instanceof Exception ex ? ex : new Exception(ee.getCause());
            }
        }
        return runAction(action, target, value);
    }

    private String runAction(String action, String target, Object value) throws Exception
    {
        switch(action)
        {
            case "channel_start":
            {
                Channel c = findChannel(target);
                if(c.isProcessing())
                {
                    return c.getName() + " was already running";
                }
                startChannel(c);
                return c.getName() + " started";
            }
            case "channel_stop":
            {
                Channel c = findChannel(target);
                if(!c.isProcessing())
                {
                    return c.getName() + " was already stopped";
                }
                mChannelProcessingManager.stop(c);
                return c.getName() + " stopped";
            }
            case "channel_restart":
            {
                Channel c = findChannel(target);
                if(c.isProcessing())
                {
                    mChannelProcessingManager.stop(c);
                    Thread.sleep(1000);   //let the tuner channel and decoder shut down first
                }
                startChannel(c);
                return c.getName() + " restarted";
            }
            case "tuner_enable":
            {
                DiscoveredTuner dt = findTuner(target);
                if(dt.isEnabled())
                {
                    return dt.getId() + " was already enabled";
                }
                dt.setEnabled(true);
                return dt.getId() + (dt.getTunerStatus() == TunerStatus.ERROR ? " enabled, but it reports an error: " +
                    dt.getErrorMessage() : " enabled");
            }
            case "tuner_disable":
            {
                DiscoveredTuner dt = findTuner(target);
                if(!dt.isEnabled())
                {
                    return dt.getId() + " was already disabled";
                }
                List<Integer> carried = mTunerChannels.getOrDefault(dt.getId(), List.of());
                dt.setEnabled(false);
                return dt.getId() + " disabled - SDR-Trunk stopped its channels" +
                    (carried.isEmpty() ? "" : " (" + carried.size() + "); Enable starts them again");
            }
            case "tuner_restart":
            {
                DiscoveredTuner dt = findTuner(target);
                if(dt.getTunerStatus() != TunerStatus.ERROR)
                {
                    return dt.getId() + " isn't in error - nothing to restart";
                }
                dt.restart();
                if(dt.getTunerStatus() == TunerStatus.ERROR)
                {
                    throw new Exception("restart tried, still in error: " + dt.getErrorMessage());
                }
                return dt.getId() + " restarted - status " + dt.getTunerStatus();
            }
            case "tuner_ppm":
            {
                DiscoveredTuner dt = findTuner(target);
                double ppm;
                try
                {
                    ppm = Double.parseDouble(String.valueOf(value));
                }
                catch(NumberFormatException nfe)
                {
                    throw new Exception("PPM must be a number");
                }
                if(ppm < -1000 || ppm > 1000)
                {
                    throw new Exception("PPM must be between -1000 and 1000");
                }
                if(!dt.hasTuner())
                {
                    throw new Exception(dt.getId() + " isn't running - enable it first");
                }
                if(dt.getTuner().getTunerController().getTunerFrequencyErrorManager() != null &&
                    dt.getTuner().getTunerController().getTunerFrequencyErrorManager().isEnabled())
                {
                    throw new Exception(dt.getId() + " has Auto PPM on - it would overwrite a value set by hand");
                }
                ppm = Math.round(ppm * 10) / 10.0;
                double before = dt.getTuner().getTunerController().getFrequencyCorrection();
                dt.getTuner().getTunerController().setFrequencyCorrection(ppm);
                TunerConfiguration config = dt.getTunerConfiguration();
                if(config != null)
                {
                    config.setFrequencyCorrection(ppm);
                    mTunerManager.saveConfigurations();
                }
                return dt.getId() + " PPM " + before + " -> " + ppm + (config != null ? " (saved)" : " (not saved - no tuner configuration)");
            }
            case "alias_set":
            {
                Map<String,Object> change = parseValue(value);
                return onFxThread(() -> setAlias(target, change));
            }
            case "alias_import":
            {
                Map<String,Object> request = parseValue(value);
                return onFxThread(() -> importAliases(request));
            }
            case "tuner_minmax":
            {
                //Same as typing Min / Max on SDR-Trunk's Tuners tab, but set exactly as given: that tab won't keep a
                //range narrower than the sample rate (it moves Min down), which spoils narrow ranges from the planner.
                DiscoveredTuner dt = findTuner(target);
                Map<String,Object> v = parseValue(value);
                long min, max;
                try
                {
                    min = Long.parseLong(String.valueOf(v.get("min")));
                    max = Long.parseLong(String.valueOf(v.get("max")));
                }
                catch(NumberFormatException nfe)
                {
                    throw new Exception("Min and Max must be frequencies in Hz");
                }
                if(min < 1_000_000 || max > 6_000_000_000L || min >= max)
                {
                    throw new Exception("Min must be below Max, both between 1 MHz and 6 GHz");
                }
                TunerConfiguration config = dt.getTunerConfiguration();
                if(config == null)
                {
                    throw new Exception(dt.getId() + " has no saved settings yet - open it once on SDR-Trunk's Tuners tab");
                }
                long beforeMin = config.getMinimumFrequency(), beforeMax = config.getMaximumFrequency();
                if(dt.hasTuner())
                {
                    dt.getTuner().getTunerController().setFrequencyExtents(min, max);
                }
                config.setMinimumFrequency(min);
                config.setMaximumFrequency(max);
                mTunerManager.saveConfigurations();
                return dt.getId() + " Min/Max " + mhz(beforeMin) + "-" + mhz(beforeMax) + " -> " + mhz(min) + "-" + mhz(max) +
                    " MHz (saved)" + (dt.hasTuner() ? "" : "; the tuner isn't running, so it applies when it starts");
            }
            case "tuner_autoppm":
            {
                DiscoveredTuner dt = findTuner(target);
                boolean on = "on".equals(String.valueOf(value));
                if(!dt.hasTuner() || dt.getTuner().getTunerController().getTunerFrequencyErrorManager() == null)
                {
                    throw new Exception(dt.getId() + " isn't running - enable it first");
                }
                dt.getTuner().getTunerController().getTunerFrequencyErrorManager().setEnabled(on);
                TunerConfiguration config = dt.getTunerConfiguration();
                if(config != null)
                {
                    config.setAutoPPMCorrectionEnabled(on);
                    mTunerManager.saveConfigurations();
                }
                return dt.getId() + " Auto PPM " + (on ? "on" : "off") + (config != null ? " (saved)" : "");
            }
            default:
                throw new Exception("unknown command '" + action + "' - this SDR-Trunk build is older than the scanner");
        }
    }

    /**
     * query_channel: the channel's settings exactly as SDR-Trunk saves them in the playlist (XML), plus its event history
     * (newest first) - what the channel editor and the channel's Events tab show.
     */
    private Map<String,Object> runQuery(String action, String target) throws Exception
    {
        if("query_aliases".equals(action))
        {
            return onFxThread(this::readAliases);
        }
        if(!"query_channel".equals(action))
        {
            throw new Exception("unknown query '" + action + "' - this SDR-Trunk build is older than the scanner");
        }
        Channel channel = findChannel(target);
        Map<String,Object> data = new LinkedHashMap<>();
        data.put("id", channel.getChannelID());
        data.put("name", channel.getName());
        data.put("processing", channel.isProcessing());

        JacksonXmlModule xmlModule = new JacksonXmlModule();
        xmlModule.setDefaultUseWrapper(false);
        try
        {
            data.put("xml", new XmlMapper(xmlModule).writeValueAsString(channel));
        }
        catch(Exception e)
        {
            data.put("xmlError", e.getMessage());
        }

        List<Map<String,Object>> events = new ArrayList<>();
        ProcessingChain chain = mChannelProcessingManager.getProcessingChain(channel);
        if(chain != null && chain.getDecodeEventHistory() != null)
        {
            List<IDecodeEvent> items = List.of();
            for(int attempt = 0; attempt < 3; attempt++)
            {
                try
                {
                    items = chain.getDecodeEventHistory().getItems();
                    break;
                }
                catch(java.util.ConcurrentModificationException cme)
                {
                    //an event arrived while copying - try again
                }
            }
            for(int i = items.size() - 1; i >= 0 && events.size() < MAX_EVENTS; i--)
            {
                IDecodeEvent ev = items.get(i);
                if(ev == null)
                {
                    continue;
                }
                Map<String,Object> m = new LinkedHashMap<>();
                m.put("t", ev.getTimeStart());
                m.put("dur", ev.getDuration());
                m.put("type", ev.getEventType() != null ? ev.getEventType().toString() : null);
                IdentifierCollection ic = ev.getIdentifierCollection();
                if(ic != null)
                {
                    Identifier from = ic.getFromIdentifier();
                    if(from != null)
                    {
                        m.put("from", String.valueOf(from.getValue()));
                        m.put("fromAlias", aliasName(ic, from));
                    }
                    Identifier to = ic.getToIdentifier();
                    if(to != null)
                    {
                        m.put("to", String.valueOf(to.getValue()));
                        m.put("toAlias", aliasName(ic, to));
                    }
                }
                if(ev.getChannelDescriptor() != null)
                {
                    m.put("freq", ev.getChannelDescriptor().getDownlinkFrequency());
                }
                if(ev.hasTimeslot())
                {
                    m.put("ts", ev.getTimeslot());
                }
                m.put("details", ev.getDetails());
                events.add(m);
            }
        }
        data.put("events", events);
        return data;
    }

    // ---------------------------------------------------------------- aliases

    @SuppressWarnings("unchecked")
    private Map<String,Object> parseValue(Object value) throws Exception
    {
        if(value instanceof Map<?,?> m)
        {
            return (Map<String,Object>)m;
        }
        try
        {
            return mMapper.readValue(String.valueOf(value), Map.class);
        }
        catch(Exception e)
        {
            throw new Exception("bad command value: " + e.getMessage());
        }
    }

    /**
     * Runs on the JavaFX thread (aliases are shown in the playlist editor, which must only be changed there) and waits.
     */
    private <T> T onFxThread(java.util.concurrent.Callable<T> work) throws Exception
    {
        CompletableFuture<T> future = new CompletableFuture<>();
        try
        {
            javafx.application.Platform.runLater(() -> {
                try
                {
                    future.complete(work.call());
                }
                catch(Throwable t)
                {
                    future.completeExceptionally(t);
                }
            });
        }
        catch(IllegalStateException notStarted)
        {
            return work.call();   //no JavaFX (headless) - nothing to keep in step with
        }
        try
        {
            return future.get(30, TimeUnit.SECONDS);
        }
        catch(java.util.concurrent.ExecutionException ee)
        {
            throw ee.getCause() instanceof Exception ex ? ex : new Exception(ee.getCause());
        }
    }

    /**
     * Identifies an alias for the admin: list, name and identifiers. Aliases have no id of their own, so a command
     * carries this back and finds the alias again - if it was renamed or changed in the meantime it isn't found.
     */
    private static String aliasKey(Alias alias)
    {
        List<String> ids = new ArrayList<>();
        for(io.github.dsheirer.alias.id.AliasID id: alias.getAliasIdentifiers())
        {
            if(id != null && !(id instanceof io.github.dsheirer.alias.id.broadcast.BroadcastChannel) &&
                !(id instanceof Priority) && !(id instanceof io.github.dsheirer.alias.id.record.Record))
            {
                ids.add(id.toString());
            }
        }
        Collections.sort(ids);
        return alias.getAliasListName() + "\u0001" + alias.getName() + "\u0001" + String.join(",", ids);
    }

    private Map<String,Object> readAliases()
    {
        Map<String,Object> data = new LinkedHashMap<>();
        List<Map<String,Object>> streams = new ArrayList<>();
        for(BroadcastConfiguration config: new ArrayList<>(mBroadcastModel.getBroadcastConfigurations()))
        {
            Map<String,Object> st = new LinkedHashMap<>();
            st.put("name", config.getName());
            st.put("type", config.getBroadcastServerType() != null ? config.getBroadcastServerType().toString() : null);
            st.put("enabled", config.isEnabled());
            st.put("rdio", config instanceof RdioScannerConfiguration);
            streams.add(st);
        }
        data.put("streams", streams);
        data.put("lists", new ArrayList<>(mAliasModel.getListNames()));

        List<Map<String,Object>> aliases = new ArrayList<>();
        for(Alias alias: new ArrayList<>(mAliasModel.getAliases()))
        {
            Map<String,Object> a = new LinkedHashMap<>();
            a.put("key", aliasKey(alias));
            a.put("list", alias.getAliasListName());
            a.put("name", alias.getName());
            a.put("group", alias.getGroup());
            List<Integer> tgs = new ArrayList<>();
            List<String> other = new ArrayList<>();
            List<String> streamNames = new ArrayList<>();
            for(io.github.dsheirer.alias.id.AliasID id: alias.getAliasIdentifiers())
            {
                if(id instanceof io.github.dsheirer.alias.id.talkgroup.Talkgroup tg)
                {
                    tgs.add(tg.getValue());
                }
                else if(id instanceof io.github.dsheirer.alias.id.broadcast.BroadcastChannel bc)
                {
                    streamNames.add(bc.getChannelName());
                }
                else if(id != null && !(id instanceof Priority) && !(id instanceof io.github.dsheirer.alias.id.record.Record))
                {
                    other.add(id.toString());
                }
            }
            a.put("tgs", tgs);
            a.put("ids", other);
            a.put("listen", alias.getPlaybackPriority() != Priority.DO_NOT_MONITOR);
            a.put("priority", alias.getPlaybackPriority());
            a.put("record", alias.isRecordable());
            a.put("streams", streamNames);
            a.put("skip", alias.isSkipDecode());
            aliases.add(a);
        }
        data.put("aliases", aliases);
        return data;
    }

    private Alias findAlias(String key) throws Exception
    {
        for(Alias alias: new ArrayList<>(mAliasModel.getAliases()))
        {
            if(aliasKey(alias).equals(key))
            {
                return alias;
            }
        }
        throw new Exception("alias not found - it was changed or deleted in SDR-Trunk since the list was read; reload it");
    }

    private String setAlias(String key, Map<String,Object> change) throws Exception
    {
        Alias alias = findAlias(key);
        List<String> done = new ArrayList<>();
        if(change.get("listen") instanceof Boolean listen)
        {
            alias.setCallPriority(listen ? Priority.DEFAULT_PRIORITY : Priority.DO_NOT_MONITOR);
            done.add("Listen " + (listen ? "on" : "off"));
        }
        if(change.get("record") instanceof Boolean record)
        {
            alias.setRecordable(record);
            done.add("Record " + (record ? "on" : "off"));
        }
        if(change.get("skip") instanceof Boolean skip)
        {
            alias.setSkipDecode(skip);
            done.add("Skip decode " + (skip ? "on" : "off"));
        }
        if(change.get("stream") instanceof String stream && change.get("on") instanceof Boolean on)
        {
            boolean known = new ArrayList<>(mBroadcastModel.getBroadcastConfigurations()).stream()
                .anyMatch(c -> stream.equals(c.getName()));
            if(!known)
            {
                throw new Exception("there's no stream called '" + stream + "' in SDR-Trunk");
            }
            if(on && !alias.hasBroadcastChannel(stream))
            {
                alias.addAliasID(new io.github.dsheirer.alias.id.broadcast.BroadcastChannel(stream));
            }
            else if(!on)
            {
                alias.removeBroadcastChannel(stream);
            }
            done.add("stream " + stream + " " + (on ? "on" : "off"));
        }
        if(done.isEmpty())
        {
            throw new Exception("nothing to change");
        }
        mPlaylistManager.schedulePlaylistSave();
        return alias.getName() + ": " + String.join(", ", done) + " (saved)";
    }

    /**
     * Data Cleaner "Send to SDR-Trunk" through the running SDR-Trunk: same rules as the file write - new talkgroups get
     * an alias, existing ones can be renamed / regrouped, and the chosen streams are added. Nothing is removed.
     */
    @SuppressWarnings("unchecked")
    private String importAliases(Map<String,Object> request) throws Exception
    {
        String list = String.valueOf(request.getOrDefault("list", "")).trim();
        if(list.isEmpty())
        {
            throw new Exception("no alias list given");
        }
        boolean rename = !Boolean.FALSE.equals(request.get("rename"));
        List<String> streams = new ArrayList<>();
        if(request.get("streams") instanceof List<?> ls)
        {
            Set<String> known = new java.util.HashSet<>();
            new ArrayList<>(mBroadcastModel.getBroadcastConfigurations()).forEach(c -> known.add(c.getName()));
            ls.forEach(o -> { if(known.contains(String.valueOf(o))) streams.add(String.valueOf(o)); });
        }

        Map<Integer,Alias> byTg = new HashMap<>();
        for(Alias alias: new ArrayList<>(mAliasModel.getAliases()))
        {
            if(list.equals(alias.getAliasListName()))
            {
                for(io.github.dsheirer.alias.id.AliasID id: alias.getAliasIdentifiers())
                {
                    if(id instanceof io.github.dsheirer.alias.id.talkgroup.Talkgroup tg)
                    {
                        byTg.putIfAbsent(tg.getValue(), alias);
                    }
                }
            }
        }

        int added = 0, renamed = 0, streamed = 0, same = 0;
        List<Alias> newAliases = new ArrayList<>();
        Set<Integer> seen = new java.util.HashSet<>();
        for(Object o: (List<Object>)request.getOrDefault("talkgroups", List.of()))
        {
            if(!(o instanceof Map<?,?> t))
            {
                continue;
            }
            int id;
            try
            {
                id = Integer.parseInt(String.valueOf(t.get("id")).trim());
            }
            catch(NumberFormatException nfe)
            {
                continue;
            }
            if(!seen.add(id))
            {
                continue;
            }
            String label = t.get("label") != null ? String.valueOf(t.get("label")).trim() : "";
            if(label.isEmpty())
            {
                label = "TG " + id;
            }
            label = label.length() > 64 ? label.substring(0, 64) : label;
            String group = t.get("group") != null ? String.valueOf(t.get("group")).trim() : "";
            group = group.length() > 64 ? group.substring(0, 64) : group;

            Alias alias = byTg.get(id);
            if(alias == null)
            {
                Alias a = new Alias(label);
                a.setAliasListName(list);
                a.setGroup(group);
                a.setColor(0);
                a.addAliasID(new io.github.dsheirer.alias.id.talkgroup.Talkgroup(io.github.dsheirer.protocol.Protocol.APCO25, id));
                streams.forEach(n -> a.addAliasID(new io.github.dsheirer.alias.id.broadcast.BroadcastChannel(n)));
                newAliases.add(a);
                byTg.put(id, a);
                added++;
                continue;
            }
            boolean changed = false;
            String oldGroup = alias.getGroup() != null ? alias.getGroup().trim() : "";
            if(rename && (!label.equals(alias.getName() != null ? alias.getName().trim() : "") || !group.equals(oldGroup)))
            {
                alias.setName(label);
                alias.setGroup(group);
                renamed++;
                changed = true;
            }
            boolean gotStream = false;
            for(String n: streams)
            {
                if(!alias.hasBroadcastChannel(n))
                {
                    alias.addAliasID(new io.github.dsheirer.alias.id.broadcast.BroadcastChannel(n));
                    gotStream = true;
                }
            }
            if(gotStream)
            {
                streamed++;
                changed = true;
            }
            if(!changed)
            {
                same++;
            }
        }
        if(!newAliases.isEmpty())
        {
            mAliasModel.addAliasList(list);
            mAliasModel.addAliases(newAliases);
        }
        if(added + renamed + streamed > 0)
        {
            mPlaylistManager.schedulePlaylistSave();
        }
        return added + " added, " + renamed + " renamed, " + streamed + " got a stream, " + same + " already the same";
    }

    private static String mhz(long hz)
    {
        return hz <= 0 ? "none" : String.format("%.4f", hz / 1e6);
    }

    private void startChannel(Channel channel) throws Exception
    {
        try
        {
            mChannelProcessingManager.start(channel);
        }
        catch(ChannelException ce)
        {
            throw new Exception(channel.getName() + " didn't start: " + ce.getMessage());
        }
    }

    private Channel findChannel(String id) throws Exception
    {
        for(Channel c: new ArrayList<>(mChannelModel.getChannels()))
        {
            if(String.valueOf(c.getChannelID()).equals(id))
            {
                return c;
            }
        }
        throw new Exception("channel " + id + " not found - was it deleted or renamed?");
    }

    private DiscoveredTuner findTuner(String id) throws Exception
    {
        DiscoveredTunerModel model = mTunerManager.getDiscoveredTunerModel();
        for(int i = 0; i < model.getRowCount(); i++)
        {
            DiscoveredTuner dt = model.getDiscoveredTuner(i);
            if(dt != null && dt.getId().equals(id))
            {
                return dt;
            }
        }
        throw new Exception("tuner " + id + " not found - unplugged?");
    }

    private static String firstAlias(List<Alias> aliases)
    {
        return aliases != null && !aliases.isEmpty() && aliases.get(0) != null ? aliases.get(0).getName() : null;
    }
}

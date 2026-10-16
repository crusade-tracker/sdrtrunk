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
    //Radio Reference (scanner admin's Radio Reference section): same login and lookups as SDR-Trunk's own tab
    private final io.github.dsheirer.preference.UserPreferences mUserPreferences;
    private io.github.dsheirer.gui.playlist.radioreference.RadioReferenceDecoder mRRDecoder;
    private io.github.dsheirer.gui.playlist.radioreference.SiteEditor mRRSiteEditor;
    private final java.util.concurrent.ExecutorService mRRExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "sdrtrunk-status-radioreference");
        t.setDaemon(true);
        return t;
    });

    public StatusReporter(TunerManager tunerManager, PlaylistManager playlistManager,
                          io.github.dsheirer.preference.UserPreferences userPreferences)
    {
        mUserPreferences = userPreferences;
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
            CallDetails.noteEvent(event);   //grants: emergency / priority for the call details sent with uploads
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
                if(alias.isSkipDecode())
                {
                    return;   //skipped on purpose (Listen off is stock again: speakers only, still streamed)
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
            while((e = SkippedAudio.poll()) != null && events.size() < MAX_QUEUED_EVENTS)
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
            body.put("channelOptions", channelOptions());
            body.put("features", List.of("channel_create", "channel_delete", "radio_reference", "rr_talkgroups", "rr_agencies", "channel_sections"));   //the scanner shows New / Clone / Delete, Radio Reference
            try
            {
                body.put("aliasLists", new ArrayList<>(mAliasModel.getListNames()));   //for the channel Edit form
            }
            catch(Exception listEx)
            {
                //changed while reading - next report
            }
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

    // ---------------------------------------------------------------- decode rate

    /**
     * Rod build: share of control channel messages (P25 Phase 1 TSBKs) that decode cleanly, over the last 5 minutes.
     * SDR-Trunk passes on every TSBK, good or bad (bad = failed its CRC), so good / all is the decode rate. A falling
     * rate is the early sign of a weak signal, a drifting dongle or a site going bad. Read on the Swing thread
     * (readChannels); the counters are filled by the decoder thread.
     */
    private static class DecodeCounter implements io.github.dsheirer.sample.Listener<io.github.dsheirer.message.IMessage>
    {
        final java.util.concurrent.atomic.AtomicLong good = new java.util.concurrent.atomic.AtomicLong();
        final java.util.concurrent.atomic.AtomicLong all = new java.util.concurrent.atomic.AtomicLong();
        final java.util.ArrayDeque<long[]> snapshots = new java.util.ArrayDeque<>();   //{time, good, all}
        final long started = System.currentTimeMillis();

        @Override
        public void receive(io.github.dsheirer.message.IMessage message)
        {
            if(message instanceof io.github.dsheirer.module.decode.p25.phase1.message.tsbk.TSBKMessage)
            {
                all.incrementAndGet();
                if(message.isValid())
                {
                    good.incrementAndGet();
                }
            }
        }
    }

    private static final long DECODE_WINDOW_MS = 5 * 60 * 1000;
    private final Map<ProcessingChain,DecodeCounter> mDecodeCounters = new WeakHashMap<>();

    private void decodeRate(Channel channel, Map<String,Object> m)
    {
        try
        {
            if(channel.getDecodeConfiguration() == null ||
                channel.getDecodeConfiguration().getDecoderType() != io.github.dsheirer.module.decode.DecoderType.P25_PHASE1)
            {
                return;
            }
            ProcessingChain chain = mChannelProcessingManager.getProcessingChain(channel);
            if(chain == null)
            {
                return;
            }
            DecodeCounter counter = mDecodeCounters.get(chain);
            if(counter == null)
            {
                counter = new DecodeCounter();
                mDecodeCounters.put(chain, counter);
                chain.addMessageListener(counter);
            }
            long now = System.currentTimeMillis();
            long good = counter.good.get(), all = counter.all.get();
            counter.snapshots.addLast(new long[]{now, good, all});
            while(counter.snapshots.size() > 1 && now - counter.snapshots.peekFirst()[0] > DECODE_WINDOW_MS)
            {
                counter.snapshots.pollFirst();
            }
            long[] first = counter.snapshots.peekFirst();
            long spanMs = now - first[0];
            long dAll = all - first[2], dGood = good - first[1];
            if(spanMs < 30_000)
            {
                return;   //just started counting - not enough yet to say
            }
            m.put("decodeWindowSec", spanMs / 1000);
            m.put("decodeMsgs", dAll);
            if(dAll > 0)
            {
                m.put("decodeRate", Math.round(dGood * 1000.0 / dAll) / 10.0);
            }
        }
        catch(Exception e)
        {
            //never let this get in the way of the report
        }
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
                if(!channel.isTrafficChannel())
                {
                    decodeRate(channel, m);
                }
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
            m.put("aliasList", c.getAliasListName());
            m.put("autoStartOrder", c.getAutoStartOrder());
            if(c.getDecodeConfiguration() != null)
            {
                m.put("decoder", String.valueOf(c.getDecodeConfiguration().getDecoderType()));
                //for the scanner's channel Edit form
                if(c.getDecodeConfiguration() instanceof io.github.dsheirer.module.decode.p25.phase1.DecodeConfigP25 p25)
                {
                    m.put("poolSize", p25.getTrafficChannelPoolSize());
                    m.put("ignoreDataCalls", p25.getIgnoreDataCalls());
                }
                if(c.getDecodeConfiguration() instanceof io.github.dsheirer.module.decode.p25.phase1.DecodeConfigP25Phase1 p1)
                {
                    m.put("modulation", p1.getModulation() == io.github.dsheirer.module.decode.p25.phase1.Modulation.CQPSK ? "LSM" : "C4FM");
                }
                if(c.getDecodeConfiguration() instanceof io.github.dsheirer.module.decode.nbfm.DecodeConfigNBFM nbfm)
                {
                    putNbfm(m, nbfm);
                }
            }
            //Logging / Recording / Additional Decoders for the channel form
            List<String> logs = new ArrayList<>(), records = new ArrayList<>(), aux = new ArrayList<>();
            if(c.getEventLogConfiguration() != null)
            {
                c.getEventLogConfiguration().getLoggers().forEach(x -> logs.add(x.name()));
            }
            if(c.getRecordConfiguration() != null)
            {
                c.getRecordConfiguration().getRecorders().forEach(x -> records.add(x.name()));
            }
            if(c.getAuxDecodeConfiguration() != null)
            {
                c.getAuxDecodeConfiguration().getAuxDecoders().forEach(x -> aux.add(x.name()));
            }
            m.put("logs", logs);
            m.put("records", records);
            m.put("aux", aux);
            if(c.getSourceConfiguration() instanceof SourceConfigTuner sct)
            {
                m.put("freqs", List.of(sct.getFrequency()));
                m.put("preferredTuner", sct.getPreferredTuner());
                m.put("source", "tuner");
            }
            else if(c.getSourceConfiguration() instanceof SourceConfigTunerMultipleFrequency sctm)
            {
                m.put("freqs", sctm.getFrequencies());
                m.put("preferredTuner", sctm.getPreferredTuner());
                m.put("source", "tuner");
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
                if(action.startsWith("query_rr"))
                {
                    //Radio Reference answers can take a while the first time - answer with a later report
                    mRRExecutor.execute(() -> {
                        try
                        {
                            Object data = runRRQuery(action, target);
                            result.put("ok", true);
                            result.put("message", "");
                            result.put("data", data);
                        }
                        catch(Throwable t)
                        {
                            result.put("ok", false);
                            result.put("message", rrError(t));
                        }
                        mResults.add(result);
                    });
                    continue;
                }
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
            case "channel_edit":
            {
                Map<String,Object> change = parseValue(value);
                return onFxThread(() -> editChannel(target, change));
            }
            case "channel_create":
            {
                Map<String,Object> spec = parseValue(value);
                Channel c = onFxThread(() -> createChannel(spec));
                String message = c.getName() + " added (saved)";
                if(c.isAutoStart())
                {
                    try
                    {
                        startChannel(c);
                        message += " and started";
                    }
                    catch(Exception e)
                    {
                        message += " - couldn't start it: " + (e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
                    }
                }
                return message;
            }
            case "rr_freq_create":
            {
                Map<String,Object> spec = parseValue(value);
                return rrFreqCreate(spec);
            }
            case "rr_alias_import":
            {
                Map<String,Object> spec = parseValue(value);
                return rrAliasImport(spec);
            }
            case "rr_create":
            {
                Map<String,Object> spec = parseValue(value);
                return rrCreate(spec);
            }
            case "channel_delete":
            {
                Channel c = findChannel(target);
                String name = c.getName();
                if(c.isProcessing())
                {
                    mChannelProcessingManager.stop(c);
                    Thread.sleep(500);   //let it shut down before it leaves the playlist
                }
                onFxThread(() -> {
                    mChannelModel.removeChannel(c);
                    mPlaylistManager.schedulePlaylistSave();
                    return null;
                });
                return name + " deleted (saved)";
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
    // ---------------- Radio Reference (the scanner admin's Radio Reference section) ----------------
    // Same login, lookups and channel building as SDR-Trunk's own Radio Reference tab. Browsing answers are sent with
    // a later report (mRRExecutor); creating a channel goes through the tab's own SiteEditor code.

    private static int intAt(String[] parts, int index)
    {
        try
        {
            return index < parts.length && !parts[index].isBlank() ? Integer.parseInt(parts[index].trim()) : 0;
        }
        catch(NumberFormatException nfe)
        {
            return 0;
        }
    }

    private static String rrError(Throwable t)
    {
        String m = t.getMessage() != null ? t.getMessage() : t.getClass().getSimpleName();
        if(m.contains("set username or password"))
        {
            return "SDR-Trunk isn't logged in to Radio Reference - log in on its Radio Reference tab (Playlist Editor)";
        }
        return t instanceof io.github.dsheirer.rrapi.RadioReferenceException ? "Radio Reference: " + m : m;
    }

    private io.github.dsheirer.gui.playlist.radioreference.RadioReferenceDecoder rrDecoder(
        io.github.dsheirer.rrapi.RadioReferenceService service) throws Exception
    {
        if(mRRDecoder == null)
        {
            mRRDecoder = new io.github.dsheirer.gui.playlist.radioreference.RadioReferenceDecoder(mUserPreferences,
                service.getTypesMap(), service.getFlavorsMap(), service.getVoicesMap(), service.getTagsMap());
        }
        return mRRDecoder;
    }

    /**
     * The tab's site editor, made once and never shown - only its create code and controls are used. JavaFX thread.
     */
    private io.github.dsheirer.gui.playlist.radioreference.SiteEditor rrSiteEditor()
    {
        if(mRRSiteEditor == null)
        {
            mRRSiteEditor = new io.github.dsheirer.gui.playlist.radioreference.SiteEditor(mUserPreferences, mPlaylistManager);
        }
        return mRRSiteEditor;
    }

    private List<Map<String,Object>> rrSystems(List<io.github.dsheirer.rrapi.type.System> systems,
                                               io.github.dsheirer.gui.playlist.radioreference.RadioReferenceDecoder decoder)
    {
        List<Map<String,Object>> out = new ArrayList<>();
        if(systems == null)
        {
            return out;
        }
        for(io.github.dsheirer.rrapi.type.System system: systems)
        {
            Map<String,Object> m = new LinkedHashMap<>();
            m.put("id", system.getSystemId());
            m.put("name", system.getName());
            m.put("protocol", rrProtocol(decoder, system));
            m.put("supported", decoder.hasSupportedProtocol(system));
            out.add(m);
        }
        out.sort((a, b) -> String.valueOf(a.get("name")).compareToIgnoreCase(String.valueOf(b.get("name"))));
        return out;
    }

    private static String rrProtocol(io.github.dsheirer.gui.playlist.radioreference.RadioReferenceDecoder decoder,
                                     io.github.dsheirer.rrapi.type.System system)
    {
        io.github.dsheirer.rrapi.type.Type type = decoder.getType(system);
        io.github.dsheirer.rrapi.type.Flavor flavor = decoder.getFlavor(system);
        return (type != null ? type.getName() : "Unknown") + (flavor != null ? " " + flavor.getName() : "");
    }

    private static <T> List<Map<String,Object>> idNames(List<T> items, java.util.function.Function<T,Integer> id,
                                                       java.util.function.Function<T,String> name)
    {
        List<Map<String,Object>> out = new ArrayList<>();
        if(items != null)
        {
            for(T item: items)
            {
                Map<String,Object> m = new LinkedHashMap<>();
                m.put("id", id.apply(item));
                m.put("name", name.apply(item));
                out.add(m);
            }
        }
        out.sort((a, b) -> String.valueOf(a.get("name")).compareToIgnoreCase(String.valueOf(b.get("name"))));
        return out;
    }

    private static boolean hasId(List<Map<String,Object>> list, int id)
    {
        for(Map<String,Object> m: list)
        {
            if(Integer.valueOf(id).equals(m.get("id")))
            {
                return true;
            }
        }
        return false;
    }

    /**
     * The rrapi System for a system id, from the county's or state's system list (the decoder works from these)
     */
    private io.github.dsheirer.rrapi.type.System rrFindSystem(io.github.dsheirer.rrapi.RadioReferenceService service,
                                                             String level, int parentId, int systemId) throws Exception
    {
        List<io.github.dsheirer.rrapi.type.System> systems = "state".equals(level) ?
            service.getStateInfo(parentId).getSystems() : service.getCountyInfo(parentId).getSystems();
        if(systems != null)
        {
            for(io.github.dsheirer.rrapi.type.System system: systems)
            {
                if(system.getSystemId() == systemId)
                {
                    return system;
                }
            }
        }
        throw new Exception("system " + systemId + " isn't listed for that " + ("state".equals(level) ? "state" : "county"));
    }

    private List<io.github.dsheirer.gui.playlist.radioreference.EnrichedSite> rrSites(
        io.github.dsheirer.rrapi.RadioReferenceService service, int systemId) throws Exception
    {
        List<io.github.dsheirer.gui.playlist.radioreference.EnrichedSite> out = new ArrayList<>();
        for(io.github.dsheirer.rrapi.type.Site site: service.getSites(systemId))
        {
            io.github.dsheirer.rrapi.type.CountyInfo countyInfo = null;
            int countyId = site.getCountyId();
            //Same as the tab: 99,999 is RR's "county unknown" for temporary sites
            if(countyId > 0 && countyId != 99999)
            {
                try
                {
                    countyInfo = service.getCountyInfo(countyId);
                }
                catch(Exception e)
                {
                    //county name only - leave it blank
                }
            }
            out.add(new io.github.dsheirer.gui.playlist.radioreference.EnrichedSite(site, countyInfo));
        }
        Collections.sort(out);
        return out;
    }

    private Object runRRQuery(String action, String target) throws Exception
    {
        io.github.dsheirer.service.radioreference.RadioReference rr = mPlaylistManager.getRadioReference();
        io.github.dsheirer.rrapi.RadioReferenceService service = rr.getService();
        io.github.dsheirer.preference.radioreference.RadioReferencePreference pref = mUserPreferences.getRadioReferencePreference();
        io.github.dsheirer.gui.playlist.radioreference.RadioReferenceDecoder decoder = rrDecoder(service);
        String[] parts = (target == null ? "" : target).split("\\|", -1);

        if("query_rr_loc".equals(action))
        {
            //target: country|state|county - empty parts start where SDR-Trunk's tab was last set; a chosen one is
            //remembered there too, like choosing it on the tab
            Map<String,Object> data = new LinkedHashMap<>();
            data.put("user", pref.getUserName());
            data.put("expires", rr.accountExpiresProperty().get());
            data.put("login", String.valueOf(rr.getLoginStatus()));

            List<Map<String,Object>> countries = idNames(service.getCountries(),
                io.github.dsheirer.rrapi.type.Country::getCountryId, io.github.dsheirer.rrapi.type.Country::getName);
            int countryId = intAt(parts, 0) > 0 ? intAt(parts, 0) : pref.getPreferredCountryId();
            if(!hasId(countries, countryId))
            {
                countryId = 0;
                for(Map<String,Object> c: countries)
                {
                    if("United States".equals(c.get("name")))
                    {
                        countryId = (Integer)c.get("id");
                    }
                }
                if(countryId == 0 && !countries.isEmpty())
                {
                    countryId = (Integer)countries.get(0).get("id");
                }
            }
            else if(intAt(parts, 0) > 0)
            {
                pref.setPreferredCountryId(countryId);
            }
            data.put("countries", countries);
            data.put("countryId", countryId);

            List<Map<String,Object>> states = countryId > 0 ? idNames(service.getCountryInfo(countryId).getStates(),
                io.github.dsheirer.rrapi.type.State::getStateId, io.github.dsheirer.rrapi.type.State::getName) : new ArrayList<>();
            int stateId = intAt(parts, 1) > 0 ? intAt(parts, 1) : pref.getPreferredStateId();
            if(!hasId(states, stateId))
            {
                stateId = 0;
            }
            else if(intAt(parts, 1) > 0)
            {
                pref.setPreferredStateId(stateId);
            }
            data.put("states", states);
            data.put("stateId", stateId);

            List<Map<String,Object>> counties = new ArrayList<>();
            List<Map<String,Object>> stateSystems = new ArrayList<>();
            if(stateId > 0)
            {
                io.github.dsheirer.rrapi.type.StateInfo stateInfo = service.getStateInfo(stateId);
                counties = idNames(stateInfo.getCounties(), io.github.dsheirer.rrapi.type.County::getCountyId,
                    io.github.dsheirer.rrapi.type.County::getName);
                stateSystems = rrSystems(stateInfo.getSystems(), decoder);
            }
            int countyId = intAt(parts, 2) > 0 ? intAt(parts, 2) : pref.getPreferredCountyId();
            if(!hasId(counties, countyId))
            {
                countyId = 0;
            }
            else if(intAt(parts, 2) > 0)
            {
                pref.setPreferredCountyId(countyId);
            }
            data.put("counties", counties);
            data.put("countyId", countyId);
            data.put("countySystems", countyId > 0 ? rrSystems(service.getCountyInfo(countyId).getSystems(), decoder) : new ArrayList<>());
            data.put("stateSystems", stateSystems);
            //Agencies, as the tab lists them: the county's own frequencies first ("<county> County (All)", id = -countyId)
            List<Map<String,Object>> countyAgencies = new ArrayList<>();
            if(countyId > 0)
            {
                io.github.dsheirer.rrapi.type.CountyInfo countyInfo = service.getCountyInfo(countyId);
                countyAgencies = idNames(countyInfo.getAgencies(), io.github.dsheirer.rrapi.type.Agency::getAgencyId,
                    io.github.dsheirer.rrapi.type.Agency::getName);
                Map<String,Object> all = new LinkedHashMap<>();
                all.put("id", -countyId);
                all.put("name", countyInfo.getName() + " County (All)");
                countyAgencies.add(0, all);
            }
            data.put("countyAgencies", countyAgencies);
            data.put("stateAgencies", stateId > 0 ? idNames(service.getStateInfo(stateId).getAgencies(),
                io.github.dsheirer.rrapi.type.Agency::getAgencyId, io.github.dsheirer.rrapi.type.Agency::getName) : new ArrayList<>());
            data.put("nationalAgencies", countryId > 0 ? idNames(service.getCountryInfo(countryId).getAgencies(),
                io.github.dsheirer.rrapi.type.Agency::getAgencyId, io.github.dsheirer.rrapi.type.Agency::getName) : new ArrayList<>());
            data.put("countyAgencyId", pref.getPreferredAgencyId(io.github.dsheirer.gui.playlist.radioreference.Level.COUNTY));
            data.put("stateAgencyId", pref.getPreferredAgencyId(io.github.dsheirer.gui.playlist.radioreference.Level.STATE));
            data.put("nationalAgencyId", pref.getPreferredAgencyId(io.github.dsheirer.gui.playlist.radioreference.Level.NATIONAL));
            data.put("countySystemId", pref.getPreferredSystemId(io.github.dsheirer.gui.playlist.radioreference.Level.COUNTY));
            data.put("stateSystemId", pref.getPreferredSystemId(io.github.dsheirer.gui.playlist.radioreference.Level.STATE));
            return data;
        }

        if("query_rr_system".equals(action))
        {
            //target: county|countyId|systemId or state|stateId|systemId
            String level = parts.length > 0 ? parts[0] : "";
            int parentId = intAt(parts, 1), systemId = intAt(parts, 2);
            io.github.dsheirer.rrapi.type.System system = rrFindSystem(service, level, parentId, systemId);
            pref.setPreferredSystemId(systemId, "state".equals(level) ? io.github.dsheirer.gui.playlist.radioreference.Level.STATE :
                io.github.dsheirer.gui.playlist.radioreference.Level.COUNTY);
            io.github.dsheirer.rrapi.type.SystemInformation info = service.getSystemInformation(systemId);
            List<io.github.dsheirer.gui.playlist.radioreference.EnrichedSite> sites = rrSites(service, systemId);
            List<Map<String,Object>> options = onFxThread(() -> {
                List<Map<String,Object>> list = new ArrayList<>();
                for(io.github.dsheirer.gui.playlist.radioreference.EnrichedSite site: sites)
                {
                    list.add(rrSiteEditor().describeForRemote(site, system, info, decoder));
                }
                return list;
            });

            Map<String,Object> data = new LinkedHashMap<>();
            data.put("id", systemId);
            data.put("name", system.getName());
            data.put("protocol", rrProtocol(decoder, system));
            io.github.dsheirer.rrapi.type.Voice voice = decoder.getVoice(system);
            data.put("voice", voice != null ? voice.getName() : null);
            data.put("supported", decoder.hasSupportedProtocol(system));
            List<Map<String,Object>> siteList = new ArrayList<>();
            for(int i = 0; i < sites.size(); i++)
            {
                io.github.dsheirer.gui.playlist.radioreference.EnrichedSite es = sites.get(i);
                Map<String,Object> m = new LinkedHashMap<>();
                m.put("id", es.getSite().getSiteId());
                m.put("system", es.getSystemFormatted());
                m.put("rfss", es.getRfssFormatted());
                m.put("site", es.getSiteFormatted());
                m.put("county", es.getCountyName());
                m.put("name", es.getDescription());
                List<Map<String,Object>> freqs = new ArrayList<>();
                if(es.getSite().getSiteFrequencies() != null)
                {
                    for(io.github.dsheirer.rrapi.type.SiteFrequency sf: es.getSite().getSiteFrequencies())
                    {
                        if(sf.getFrequency() > 0.01)
                        {
                            Map<String,Object> f = new LinkedHashMap<>();
                            String use = sf.getUse();
                            f.put("type", use == null || use.isEmpty() ? "" : use.equalsIgnoreCase("a") ? "Alt Control" :
                                use.equalsIgnoreCase("d") ? "Control" : use);
                            f.put("lcn", sf.getLogicalChannelNumber());
                            f.put("hz", (long)(sf.getFrequency() * 1E6));
                            freqs.add(f);
                        }
                    }
                }
                //Same order as the tab: Control, Alt Control, then the rest, each by LCN
                freqs.sort((a, b) -> {
                    int ra = "Control".equals(a.get("type")) ? 0 : "Alt Control".equals(a.get("type")) ? 1 : 2;
                    int rb = "Control".equals(b.get("type")) ? 0 : "Alt Control".equals(b.get("type")) ? 1 : 2;
                    return ra != rb ? Integer.compare(ra, rb) : Integer.compare((Integer)a.get("lcn"), (Integer)b.get("lcn"));
                });
                m.put("freqs", freqs);
                m.put("options", options.get(i));
                siteList.add(m);
            }
            data.put("sites", siteList);
            return data;
        }

        if("query_rr_agency".equals(action) || "query_rr_freqs".equals(action))
        {
            //target: level|agencyId (query_rr_agency) or level|agencyId|categoryId|subCategoryId (query_rr_freqs,
            //0 = all, like the tab's "All" choices). A negative agency id is the county's own frequencies.
            String level = parts.length > 0 ? parts[0] : "county";
            int agencyId = parts.length > 1 && !parts[1].isBlank() ? Integer.parseInt(parts[1].trim()) : 0;
            io.github.dsheirer.gui.playlist.radioreference.Level lv = "national".equals(level) ?
                io.github.dsheirer.gui.playlist.radioreference.Level.NATIONAL : "state".equals(level) ?
                io.github.dsheirer.gui.playlist.radioreference.Level.STATE : io.github.dsheirer.gui.playlist.radioreference.Level.COUNTY;
            List<io.github.dsheirer.rrapi.type.Category> categories = agencyId < 0 ?
                service.getCountyInfo(-agencyId).getCategories() : service.getAgencyInfo(agencyId).getCategories();
            if(categories == null)
            {
                categories = new ArrayList<>();
            }
            if("query_rr_agency".equals(action))
            {
                pref.setPreferredAgencyId(agencyId, lv);
                List<Map<String,Object>> cats = new ArrayList<>();
                for(io.github.dsheirer.rrapi.type.Category category: categories)
                {
                    Map<String,Object> m = new LinkedHashMap<>();
                    m.put("id", category.getCategoryId());
                    m.put("name", category.getName());
                    m.put("subs", idNames(category.getSubCategories(), io.github.dsheirer.rrapi.type.SubCategory::getSubCategoryId,
                        io.github.dsheirer.rrapi.type.SubCategory::getName));
                    cats.add(m);
                }
                Map<String,Object> data = new LinkedHashMap<>();
                data.put("agencyId", agencyId);
                data.put("categories", cats);
                return data;
            }

            int categoryId = intAt(parts, 2), subCategoryId = intAt(parts, 3);
            Map<String,Object> data = new LinkedHashMap<>();
            List<Map<String,Object>> freqs = new ArrayList<>();
            Map<Integer,io.github.dsheirer.rrapi.type.Mode> modes = service.getModesMap();
            for(io.github.dsheirer.rrapi.type.Category category: categories)
            {
                if(categoryId != 0 && category.getCategoryId() != categoryId)
                {
                    continue;
                }
                if(category.getSubCategories() == null)
                {
                    continue;
                }
                for(io.github.dsheirer.rrapi.type.SubCategory sub: category.getSubCategories())
                {
                    if(subCategoryId != 0 && sub.getSubCategoryId() != subCategoryId)
                    {
                        continue;
                    }
                    List<io.github.dsheirer.rrapi.type.Frequency> list = service.getSubCategoryFrequencies(sub.getSubCategoryId());
                    if(list == null)
                    {
                        continue;
                    }
                    for(io.github.dsheirer.rrapi.type.Frequency f: list)
                    {
                        Map<String,Object> m = new LinkedHashMap<>();
                        m.put("id", f.getFrequencyId());
                        m.put("alpha", f.getAlphaTag());
                        m.put("description", f.getDescription());
                        m.put("hz", (long)(f.getDownlink() * 1E6));
                        m.put("tone", f.getTone());
                        io.github.dsheirer.rrapi.type.Mode mode = null;
                        try
                        {
                            mode = modes.get(Integer.parseInt(f.getMode()));
                        }
                        catch(Exception e)
                        {
                            //no mode
                        }
                        io.github.dsheirer.gui.playlist.radioreference.ModeDecoderType mdt =
                            io.github.dsheirer.gui.playlist.radioreference.ModeDecoderType.get(mode);
                        m.put("mode", mode != null ? mode.getName() : null);
                        m.put("modeId", f.getMode());
                        m.put("decoder", mdt.hasDecoderType() ? mdt.getDecoderType().getDisplayString() : null);
                        m.put("system", category.getName());
                        m.put("site", sub.getName());
                        freqs.add(m);
                    }
                }
            }
            //Same order as the tab: by alpha tag, blank ones last
            freqs.sort((a, b) -> {
                String x = (String)a.get("alpha"), y = (String)b.get("alpha");
                return x == null ? (y == null ? 0 : 1) : y == null ? -1 : x.compareTo(y);
            });
            data.put("freqs", freqs);
            return data;
        }

        if("query_rr_talkgroups".equals(action))
        {
            //target: county|countyId|systemId|aliasList - the Talkgroup View, with the alias each talkgroup already
            //has in that alias list (same match as the tab)
            String level = parts.length > 0 ? parts[0] : "";
            int parentId = intAt(parts, 1), systemId = intAt(parts, 2);
            String aliasListName = parts.length > 3 ? parts[3] : "";
            io.github.dsheirer.rrapi.type.System system = rrFindSystem(service, level, parentId, systemId);
            List<io.github.dsheirer.rrapi.type.Talkgroup> talkgroups = service.getTalkgroups(systemId);
            List<io.github.dsheirer.rrapi.type.TalkgroupCategory> categories = service.getTalkgroupCategories(systemId);
            Map<Integer,io.github.dsheirer.rrapi.type.TalkgroupCategory> categoryById = new HashMap<>();
            List<Map<String,Object>> categoryList = new ArrayList<>();
            if(categories != null)
            {
                List<io.github.dsheirer.rrapi.type.TalkgroupCategory> sorted = new ArrayList<>(categories);
                sorted.sort(java.util.Comparator.comparingInt(io.github.dsheirer.rrapi.type.TalkgroupCategory::getSort));
                for(io.github.dsheirer.rrapi.type.TalkgroupCategory category: sorted)
                {
                    categoryById.put(category.getTalkgroupCategoryId(), category);
                    Map<String,Object> m = new LinkedHashMap<>();
                    m.put("id", category.getTalkgroupCategoryId());
                    m.put("name", category.getName());
                    categoryList.add(m);
                }
            }
            List<io.github.dsheirer.rrapi.type.Talkgroup> list = talkgroups != null ? talkgroups : new ArrayList<>();
            List<Map<String,Object>> talkgroupList = onFxThread(() -> {
                io.github.dsheirer.alias.AliasList aliasList = aliasListName.isEmpty() ? null : mAliasModel.getAliasList(aliasListName);
                List<Map<String,Object>> out = new ArrayList<>();
                for(io.github.dsheirer.rrapi.type.Talkgroup tg: list)
                {
                    Map<String,Object> m = new LinkedHashMap<>();
                    m.put("id", tg.getTalkgroupId());
                    m.put("value", decoder.getTalkgroupValue(tg, system));
                    m.put("label", decoder.format(tg, system));
                    m.put("alpha", tg.getAlphaTag());
                    m.put("description", tg.getDescription());
                    m.put("mode", tg.getMode());
                    m.put("categoryId", tg.getTalkgroupCategoryId());
                    io.github.dsheirer.rrapi.type.TalkgroupCategory category = categoryById.get(tg.getTalkgroupCategoryId());
                    m.put("category", category != null ? category.getName() : null);
                    io.github.dsheirer.gui.playlist.radioreference.TalkgroupEncryption enc =
                        io.github.dsheirer.gui.playlist.radioreference.TalkgroupEncryption.lookup(tg.getEncryptionState());
                    m.put("encryption", enc.toString());
                    m.put("encrypted", enc == io.github.dsheirer.gui.playlist.radioreference.TalkgroupEncryption.FULL);
                    String alias = null;
                    if(aliasList != null)
                    {
                        List<Alias> found = aliasList.getAliases(decoder.getIdentifier(tg, system));
                        if(!found.isEmpty())
                        {
                            alias = found.get(0).getName();
                        }
                    }
                    m.put("alias", alias);
                    out.add(m);
                }
                return out;
            });
            Map<String,Object> data = new LinkedHashMap<>();
            data.put("systemId", systemId);
            data.put("aliasList", aliasListName);
            data.put("categories", categoryList);
            data.put("talkgroups", talkgroupList);
            return data;
        }

        throw new Exception("unknown query '" + action + "' - this SDR-Trunk build is older than the scanner");
    }

    /**
     * Agency frequency -> channel from the scanner's Radio Reference section, built by the tab's own FrequencyEditor
     * code (decoder from the RR mode; 25 kHz for FM, C4FM for P25, NXDN 4800/9600). Like the tab, no alias list or tone
     * is set - the admin's channel form does that.
     */
    private String rrFreqCreate(Map<String,Object> spec) throws Exception
    {
        io.github.dsheirer.rrapi.RadioReferenceService service;
        try
        {
            service = mPlaylistManager.getRadioReference().getService();
        }
        catch(Exception e)
        {
            throw new Exception(rrError(e));
        }
        long hz = Long.parseLong(str(spec.get("hz")));
        if(hz < 1_000_000L || hz > 6_000_000_000L)
        {
            throw new Exception(hz + " isn't a frequency in Hz");
        }
        io.github.dsheirer.rrapi.type.Mode mode = null;
        try
        {
            mode = service.getModesMap().get(Integer.parseInt(str(spec.get("modeId"))));
        }
        catch(NumberFormatException nfe)
        {
            //no mode
        }
        io.github.dsheirer.gui.playlist.radioreference.ModeDecoderType mdt =
            io.github.dsheirer.gui.playlist.radioreference.ModeDecoderType.get(mode);
        if(!mdt.hasDecoderType())
        {
            throw new Exception("SDR-Trunk has no decoder for Radio Reference mode " + (mode != null ? mode.getName() : "(none)"));
        }
        if(str(spec.get("name")).isEmpty())
        {
            throw new Exception("the channel needs a name");
        }
        Channel channel = onFxThread(() -> {
            Channel c = io.github.dsheirer.gui.playlist.radioreference.FrequencyEditor.buildChannel(mdt, hz,
                str(spec.get("system")), str(spec.get("site")), str(spec.get("name")));
            if(c != null)
            {
                mChannelModel.addChannel(c);
            }
            return c;
        });
        if(channel == null)
        {
            throw new Exception("SDR-Trunk couldn't make a channel for that mode");
        }
        mPlaylistManager.schedulePlaylistSave();
        return channel.getName() + " added (saved)";
    }

    /**
     * Talkgroup View import from the scanner's Radio Reference section - the tab's Import All Talkgroups (the ids the
     * admin's filter shows) or Create Talkgroup Alias for one (with its own name and group). Aliases are made the way
     * the tab makes them; a talkgroup that already has an alias in the list is left alone.
     */
    private String rrAliasImport(Map<String,Object> spec) throws Exception
    {
        io.github.dsheirer.rrapi.RadioReferenceService service;
        try
        {
            service = mPlaylistManager.getRadioReference().getService();
        }
        catch(Exception e)
        {
            throw new Exception(rrError(e));
        }
        io.github.dsheirer.gui.playlist.radioreference.RadioReferenceDecoder decoder = rrDecoder(service);
        String level = str(spec.get("level"));
        int parentId = intAt(new String[]{str(spec.get("parentId"))}, 0);
        int systemId = intAt(new String[]{str(spec.get("systemId"))}, 0);
        String aliasListName = str(spec.get("aliasList"));
        if(aliasListName.isEmpty())
        {
            throw new Exception("An alias list is required to create aliases");
        }
        boolean muteEncrypted = Boolean.TRUE.equals(spec.get("muteEncrypted"));
        java.util.Set<Integer> wanted = new java.util.HashSet<>();
        if(spec.get("talkgroupIds") instanceof List<?> ids)
        {
            for(Object o: ids)
            {
                wanted.add(Integer.parseInt(str(o)));
            }
        }
        if(wanted.isEmpty())
        {
            throw new Exception("no talkgroups to import");
        }
        String singleName = spec.containsKey("name") ? str(spec.get("name")) : null;
        String singleGroup = spec.containsKey("group") ? str(spec.get("group")) : null;

        io.github.dsheirer.rrapi.type.System system;
        List<io.github.dsheirer.rrapi.type.Talkgroup> talkgroups;
        Map<Integer,String> categoryNames = new HashMap<>();
        try
        {
            system = rrFindSystem(service, level, parentId, systemId);
            talkgroups = service.getTalkgroups(systemId);
            List<io.github.dsheirer.rrapi.type.TalkgroupCategory> categories = service.getTalkgroupCategories(systemId);
            if(categories != null)
            {
                for(io.github.dsheirer.rrapi.type.TalkgroupCategory c: categories)
                {
                    categoryNames.put(c.getTalkgroupCategoryId(), c.getName());
                }
            }
        }
        catch(io.github.dsheirer.rrapi.RadioReferenceException rre)
        {
            throw new Exception(rrError(rre));
        }

        final List<io.github.dsheirer.rrapi.type.Talkgroup> all = talkgroups != null ? talkgroups : new ArrayList<>();
        int[] counts = onFxThread(() -> {
            io.github.dsheirer.alias.AliasList aliasList = mAliasModel.getAliasList(aliasListName);
            List<Alias> created = new ArrayList<>();
            int had = 0;
            for(io.github.dsheirer.rrapi.type.Talkgroup tg: all)
            {
                if(!wanted.contains(tg.getTalkgroupId()))
                {
                    continue;
                }
                if(!aliasList.getAliases(decoder.getIdentifier(tg, system)).isEmpty())
                {
                    had++;
                    continue;
                }
                String group = singleGroup != null ? (singleGroup.isEmpty() ? null : singleGroup) :
                    categoryNames.get(tg.getTalkgroupCategoryId());
                Alias alias = decoder.createAlias(tg, system, aliasListName, group);
                if(singleName != null && !singleName.isEmpty())
                {
                    alias.setName(singleName);
                }
                if(muteEncrypted && io.github.dsheirer.gui.playlist.radioreference.TalkgroupEncryption.lookup(
                    tg.getEncryptionState()) == io.github.dsheirer.gui.playlist.radioreference.TalkgroupEncryption.FULL)
                {
                    alias.addAliasID(new io.github.dsheirer.alias.id.priority.Priority(
                        io.github.dsheirer.alias.id.priority.Priority.DO_NOT_MONITOR));
                }
                created.add(alias);
            }
            if(!created.isEmpty())
            {
                mAliasModel.addAliases(created);
            }
            return new int[]{created.size(), had};
        });
        mPlaylistManager.schedulePlaylistSave();
        return counts[0] + (counts[0] == 1 ? " alias" : " aliases") + " added to " + aliasListName +
            (counts[1] > 0 ? " (" + counts[1] + " already had one - left alone)" : "") + " (saved)";
    }

    /**
     * Create Channel Configuration(s) from the scanner's Radio Reference section, through the tab's own code
     */
    private String rrCreate(Map<String,Object> spec) throws Exception
    {
        io.github.dsheirer.rrapi.RadioReferenceService service;
        try
        {
            service = mPlaylistManager.getRadioReference().getService();
        }
        catch(Exception e)
        {
            throw new Exception(rrError(e));
        }
        io.github.dsheirer.gui.playlist.radioreference.RadioReferenceDecoder decoder = rrDecoder(service);
        String level = str(spec.get("level"));
        int parentId = intAt(new String[]{str(spec.get("parentId"))}, 0);
        int systemId = intAt(new String[]{str(spec.get("systemId"))}, 0);
        int siteId = intAt(new String[]{str(spec.get("siteId"))}, 0);

        io.github.dsheirer.rrapi.type.System system;
        io.github.dsheirer.rrapi.type.SystemInformation info;
        io.github.dsheirer.gui.playlist.radioreference.EnrichedSite site = null;
        try
        {
            system = rrFindSystem(service, level, parentId, systemId);
            info = service.getSystemInformation(systemId);
            for(io.github.dsheirer.gui.playlist.radioreference.EnrichedSite es: rrSites(service, systemId))
            {
                if(es.getSite().getSiteId() == siteId)
                {
                    site = es;
                }
            }
        }
        catch(io.github.dsheirer.rrapi.RadioReferenceException rre)
        {
            throw new Exception(rrError(rre));
        }
        if(site == null)
        {
            throw new Exception("site " + siteId + " isn't in that system any more");
        }

        List<Long> selected = new ArrayList<>();
        if(spec.get("selected") instanceof List<?> list)
        {
            for(Object o: list)
            {
                selected.add(Long.parseLong(str(o)));
            }
        }
        final io.github.dsheirer.gui.playlist.radioreference.EnrichedSite chosen = site;
        List<Channel> added = onFxThread(() -> rrSiteEditor().createForRemote(chosen, system, info, decoder,
            str(spec.get("frequencies")), Boolean.TRUE.equals(spec.get("forEach")), Boolean.TRUE.equals(spec.get("tdma")),
            str(spec.get("system")), str(spec.get("site")), str(spec.get("name")), str(spec.get("aliasList")), selected));
        if(added.isEmpty())
        {
            throw new Exception("nothing was created");
        }
        mPlaylistManager.schedulePlaylistSave();
        List<String> names = new ArrayList<>();
        for(Channel c: added)
        {
            names.add(c.getName());
        }
        return (added.size() == 1 ? names.get(0) + " added" : added.size() + " channels added: " +
            String.join(", ", names.size() > 12 ? names.subList(0, 12) : names) + (names.size() > 12 ? " ..." : "")) +
            " (saved)";
    }

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

    /**
     * Rod build: changes a playlist channel's settings from the scanner's admin page (channel details -> Edit) and
     * saves the playlist. Only the keys present are changed. A running channel keeps its old settings until it is
     * restarted - the admin page offers that.
     */
    private String editChannel(String target, Map<String,Object> change) throws Exception
    {
        Channel c = findChannel(target);
        List<String> done = applyChanges(c, change);

        if(done.isEmpty())
        {
            return c.getName() + ": nothing to change";
        }
        mPlaylistManager.schedulePlaylistSave();
        return c.getName() + ": changed " + String.join(", ", done) + " (saved)" +
            (c.isProcessing() ? " - restart the channel to use the new settings" : "");
    }

    /**
     * Rod build: adds a channel to the playlist from the scanner's admin page (New channel, or Clone). Clone starts
     * from a full copy of the existing channel (copyFrom), so its logging, recording and additional decoders come
     * along; New starts from SDR-Trunk's defaults for the decoder type (P25 Phase 1 or NBFM). The rest of the spec is
     * applied the same way as channel_edit. Needs a name and at least one frequency.
     */
    private Channel createChannel(Map<String,Object> spec) throws Exception
    {
        Channel c;
        if(spec.get("copyFrom") != null && !str(spec.get("copyFrom")).isEmpty())
        {
            c = findChannel(str(spec.get("copyFrom"))).copyOf();
        }
        else
        {
            String decoder = str(spec.get("decoder")).toUpperCase();
            io.github.dsheirer.module.decode.DecoderType type;
            if(decoder.equals("P25_PHASE1"))
            {
                type = io.github.dsheirer.module.decode.DecoderType.P25_PHASE1;
            }
            else if(decoder.equals("NBFM"))
            {
                type = io.github.dsheirer.module.decode.DecoderType.NBFM;
            }
            else
            {
                throw new Exception("new channels can be P25 Phase 1 or NBFM for now - not \"" + decoder + "\"");
            }
            c = new Channel();
            c.setDecodeConfiguration(io.github.dsheirer.module.decode.DecoderFactory.getDecodeConfiguration(type));
        }

        if(str(spec.get("name")).isEmpty())
        {
            throw new Exception("the channel needs a name");
        }
        Map<String,Object> change = new LinkedHashMap<>(spec);
        change.remove("copyFrom");
        change.remove("decoder");
        applyChanges(c, change);

        if(c.getFrequencyList() == null || c.getFrequencyList().isEmpty() || c.getFrequencyList().get(0) == null ||
            c.getFrequencyList().get(0) <= 0)
        {
            throw new Exception("the channel needs at least one frequency");
        }

        mChannelModel.addChannel(c);
        mPlaylistManager.schedulePlaylistSave();
        return c;
    }

    /**
     * Applies the keys present in the change to a channel (edit, new and clone all use this). Returns what changed.
     */
    private List<String> applyChanges(Channel c, Map<String,Object> change) throws Exception
    {
        List<String> done = new ArrayList<>();

        if(change.containsKey("name"))
        {
            String v = str(change.get("name"));
            if(v.isEmpty())
            {
                throw new Exception("the channel needs a name");
            }
            c.setName(v);
            done.add("name");
        }
        if(change.containsKey("system"))
        {
            c.setSystem(str(change.get("system")));
            done.add("system");
        }
        if(change.containsKey("site"))
        {
            c.setSite(str(change.get("site")));
            done.add("site");
        }
        if(change.containsKey("aliasList"))
        {
            String v = str(change.get("aliasList"));
            //A new list name is allowed when the admin chose "New list..." - SDR-Trunk lists it once an alias uses it
            if(!v.isEmpty() && !mAliasModel.getListNames().contains(v) && !Boolean.TRUE.equals(change.get("aliasListNew")))
            {
                throw new Exception("there is no alias list called \"" + v + "\"");
            }
            c.setAliasListName(v.isEmpty() ? null : v);
            done.add("alias list");
        }
        if(change.containsKey("autoStart"))
        {
            c.setAutoStart(Boolean.TRUE.equals(change.get("autoStart")));
            done.add("auto-start");
        }
        if(change.containsKey("autoStartOrder"))
        {
            Object o = change.get("autoStartOrder");
            c.setAutoStartOrder(o == null || str(o).isEmpty() ? null : Integer.parseInt(str(o)));
            done.add("auto-start order");
        }

        if(change.containsKey("freqs") || change.containsKey("preferredTuner"))
        {
            io.github.dsheirer.source.config.SourceConfiguration sc = c.getSourceConfiguration();
            String tuner = change.containsKey("preferredTuner") ? str(change.get("preferredTuner")) : null;
            if(change.containsKey("freqs"))
            {
                List<Long> freqs = new ArrayList<>();
                for(Object o: (List<Object>)change.get("freqs"))
                {
                    long f = Long.parseLong(str(o));
                    if(f < 1_000_000L || f > 6_000_000_000L)
                    {
                        throw new Exception(f + " isn't a frequency in Hz");
                    }
                    freqs.add(f);
                }
                if(freqs.isEmpty())
                {
                    throw new Exception("the channel needs at least one frequency");
                }
                if(sc instanceof SourceConfigTunerMultipleFrequency multi)
                {
                    multi.setFrequencies(freqs);
                }
                else if(sc instanceof SourceConfigTuner single && freqs.size() == 1)
                {
                    single.setFrequency(freqs.get(0));
                }
                else if(sc instanceof SourceConfigTuner single)
                {
                    SourceConfigTunerMultipleFrequency multi = new SourceConfigTunerMultipleFrequency();
                    multi.setPreferredTuner(single.getPreferredTuner());
                    multi.setFrequencies(freqs);
                    sc = multi;
                }
                else
                {
                    throw new Exception("this channel's source isn't a tuner - frequencies can't be set from here");
                }
                done.add("frequencies");
            }
            if(tuner != null)
            {
                String t = tuner.isEmpty() ? null : tuner;
                if(sc instanceof SourceConfigTunerMultipleFrequency multi)
                {
                    multi.setPreferredTuner(t);
                }
                else if(sc instanceof SourceConfigTuner single)
                {
                    single.setPreferredTuner(t);
                }
                done.add("preferred tuner");
            }
            c.setSourceConfiguration(sc);
        }

        if(change.containsKey("modulation") || change.containsKey("poolSize") || change.containsKey("ignoreDataCalls"))
        {
            io.github.dsheirer.module.decode.config.DecodeConfiguration dc = c.getDecodeConfiguration();
            if(!(dc instanceof io.github.dsheirer.module.decode.p25.phase1.DecodeConfigP25 p25))
            {
                throw new Exception("modulation, traffic channels and data calls are P25 settings - this channel isn't P25");
            }
            if(change.containsKey("modulation"))
            {
                if(!(dc instanceof io.github.dsheirer.module.decode.p25.phase1.DecodeConfigP25Phase1 p1))
                {
                    throw new Exception("modulation is a P25 Phase 1 setting");
                }
                String mod = str(change.get("modulation")).toUpperCase();
                p1.setModulation(mod.equals("LSM") || mod.equals("CQPSK") ? io.github.dsheirer.module.decode.p25.phase1.Modulation.CQPSK
                    : io.github.dsheirer.module.decode.p25.phase1.Modulation.C4FM);
                done.add("modulation");
            }
            if(change.containsKey("poolSize"))
            {
                int n = Integer.parseInt(str(change.get("poolSize")));
                if(n < 0 || n > 50)
                {
                    throw new Exception("traffic channels must be 0 to 50");
                }
                p25.setTrafficChannelPoolSize(n);
                done.add("traffic channels");
            }
            if(change.containsKey("ignoreDataCalls"))
            {
                p25.setIgnoreDataCalls(Boolean.TRUE.equals(change.get("ignoreDataCalls")));
                done.add("data calls");
            }
            c.setDecodeConfiguration(dc);
        }

        if(change.containsKey("bandwidth") || change.containsKey("deemphasis") || change.containsKey("audioFilter") ||
            change.containsKey("alc") || change.containsKey("talkgroup") || change.containsKey("squelchType") ||
            change.containsKey("squelchCode"))
        {
            if(!(c.getDecodeConfiguration() instanceof io.github.dsheirer.module.decode.nbfm.DecodeConfigNBFM nbfm))
            {
                throw new Exception("bandwidth, de-emphasis, filter, ALC, talkgroup and squelch are NBFM settings - this channel isn't NBFM");
            }
            applyNbfm(nbfm, change, done);
            c.setDecodeConfiguration(nbfm);
        }

        if(change.containsKey("logs") || change.containsKey("records") || change.containsKey("aux"))
        {
            io.github.dsheirer.module.decode.DecoderType type = c.getDecodeConfiguration() != null ?
                c.getDecodeConfiguration().getDecoderType() : null;
            if(change.containsKey("logs"))
            {
                io.github.dsheirer.module.log.config.EventLogConfiguration config = c.getEventLogConfiguration();
                if(config == null)
                {
                    config = new io.github.dsheirer.module.log.config.EventLogConfiguration();
                }
                config.clear();
                for(String name: strList(change.get("logs")))
                {
                    io.github.dsheirer.module.log.EventLogType t = io.github.dsheirer.module.log.EventLogType.valueOf(name);
                    if(!logOptions(type).contains(t))
                    {
                        throw new Exception(t.getDisplayString() + " logging isn't offered for this channel's decoder");
                    }
                    config.addLogger(t);
                }
                c.setEventLogConfiguration(config);
                done.add("logging");
            }
            if(change.containsKey("records"))
            {
                io.github.dsheirer.record.config.RecordConfiguration config = c.getRecordConfiguration();
                if(config == null)
                {
                    config = new io.github.dsheirer.record.config.RecordConfiguration();
                }
                config.clearRecorders();
                for(String name: strList(change.get("records")))
                {
                    io.github.dsheirer.record.RecorderType t = io.github.dsheirer.record.RecorderType.valueOf(name);
                    if(!recordOptions(type).contains(t))
                    {
                        throw new Exception(t.getDisplayString() + " recording isn't offered for this channel's decoder");
                    }
                    config.addRecorder(t);
                }
                c.setRecordConfiguration(config);
                done.add("recording");
            }
            if(change.containsKey("aux"))
            {
                io.github.dsheirer.module.decode.config.AuxDecodeConfiguration config = c.getAuxDecodeConfiguration();
                if(config == null)
                {
                    config = new io.github.dsheirer.module.decode.config.AuxDecodeConfiguration();
                }
                config.clearAuxDecoders();
                for(String name: strList(change.get("aux")))
                {
                    io.github.dsheirer.module.decode.DecoderType t = io.github.dsheirer.module.decode.DecoderType.valueOf(name);
                    if(!auxOptions(type).contains(t))
                    {
                        throw new Exception(t.getDisplayString() + " isn't an additional decoder for this channel's decoder");
                    }
                    config.addAuxDecoder(t);
                }
                c.setAuxDecodeConfiguration(config);
                done.add("additional decoders");
            }
        }

        return done;
    }

    private static List<String> strList(Object o)
    {
        List<String> out = new ArrayList<>();
        if(o instanceof List<?> list)
        {
            for(Object x: list)
            {
                out.add(str(x));
            }
        }
        return out;
    }

    // Logging / Recording / Additional Decoders offered per decoder - the same lists SDR-Trunk's channel editors show

    private static List<io.github.dsheirer.module.log.EventLogType> logOptions(io.github.dsheirer.module.decode.DecoderType type)
    {
        if(type == null)
        {
            return List.of();
        }
        return switch(type)
        {
            case P25_PHASE1, DMR, NXDN, MPT1327 -> List.of(io.github.dsheirer.module.log.EventLogType.CALL_EVENT,
                io.github.dsheirer.module.log.EventLogType.DECODED_MESSAGE, io.github.dsheirer.module.log.EventLogType.TRAFFIC_CALL_EVENT,
                io.github.dsheirer.module.log.EventLogType.TRAFFIC_DECODED_MESSAGE);
            case AM, NBFM, LTR, LTR_NET, PASSPORT, P25_PHASE2 -> List.of(io.github.dsheirer.module.log.EventLogType.CALL_EVENT,
                io.github.dsheirer.module.log.EventLogType.DECODED_MESSAGE);
            default -> List.of();
        };
    }

    private static List<io.github.dsheirer.record.RecorderType> recordOptions(io.github.dsheirer.module.decode.DecoderType type)
    {
        if(type == null)
        {
            return List.of();
        }
        return switch(type)
        {
            case P25_PHASE1, P25_PHASE2, DMR, NXDN -> List.of(io.github.dsheirer.record.RecorderType.BASEBAND,
                io.github.dsheirer.record.RecorderType.DEMODULATED_BIT_STREAM, io.github.dsheirer.record.RecorderType.MBE_CALL_SEQUENCE,
                io.github.dsheirer.record.RecorderType.TRAFFIC_BASEBAND, io.github.dsheirer.record.RecorderType.TRAFFIC_DEMODULATED_BIT_STREAM,
                io.github.dsheirer.record.RecorderType.TRAFFIC_MBE_CALL_SEQUENCE);
            case MPT1327 -> List.of(io.github.dsheirer.record.RecorderType.BASEBAND,
                io.github.dsheirer.record.RecorderType.DEMODULATED_BIT_STREAM, io.github.dsheirer.record.RecorderType.TRAFFIC_BASEBAND,
                io.github.dsheirer.record.RecorderType.TRAFFIC_DEMODULATED_BIT_STREAM);
            case AM, NBFM, LTR, LTR_NET, PASSPORT -> List.of(io.github.dsheirer.record.RecorderType.BASEBAND);
            default -> List.of();
        };
    }

    private static List<io.github.dsheirer.module.decode.DecoderType> auxOptions(io.github.dsheirer.module.decode.DecoderType type)
    {
        if(type == null)
        {
            return List.of();
        }
        return switch(type)
        {
            case NBFM -> new ArrayList<>(io.github.dsheirer.module.decode.DecoderType.AUX_DECODERS);
            case LTR, LTR_NET, PASSPORT -> List.of(io.github.dsheirer.module.decode.DecoderType.FLEETSYNC2,
                io.github.dsheirer.module.decode.DecoderType.MDC1200);
            default -> List.of();
        };
    }

    /**
     * Per decoder: the Logging / Recording / Additional Decoders choices with SDR-Trunk's labels (for the scanner's
     * channel form)
     */
    private static Map<String,Object> channelOptions()
    {
        Map<String,Object> out = new LinkedHashMap<>();
        for(io.github.dsheirer.module.decode.DecoderType type: io.github.dsheirer.module.decode.DecoderType.PRIMARY_DECODERS)
        {
            Map<String,Object> m = new LinkedHashMap<>();
            List<List<String>> logs = new ArrayList<>(), records = new ArrayList<>(), aux = new ArrayList<>();
            logOptions(type).forEach(x -> logs.add(List.of(x.name(), x.getDisplayString())));
            recordOptions(type).forEach(x -> records.add(List.of(x.name(), x.getDisplayString())));
            auxOptions(type).forEach(x -> aux.add(List.of(x.name(), x.getDisplayString())));
            m.put("logs", logs);
            m.put("records", records);
            m.put("aux", aux);
            out.put(type.name(), m);
        }
        return out;
    }

    /**
     * NBFM settings for the scanner's channel forms, in the same terms as SDR-Trunk's NBFM editor
     */
    private static void putNbfm(Map<String,Object> m, io.github.dsheirer.module.decode.nbfm.DecodeConfigNBFM nbfm)
    {
        m.put("bandwidth", nbfm.getBandwidth() != null ? String.valueOf(nbfm.getBandwidth().getValue() / 1000.0) : null);
        m.put("deemphasis", nbfm.getDeemphasis() == io.github.dsheirer.module.decode.nbfm.DeemphasisMode.NBFM_300 ? "300" : "none");
        m.put("audioFilter", nbfm.isAudioFilter());
        m.put("alc", nbfm.isAudioALC());
        m.put("talkgroup", nbfm.getTalkgroup());
        String type = "none", code = "";
        List<io.github.dsheirer.module.decode.squelch.SquelchDecoderConfig> sq = nbfm.getSquelchDecoders();
        if(sq != null && !sq.isEmpty() && sq.get(0) != null)
        {
            io.github.dsheirer.module.decode.squelch.SquelchDecoderConfig s = sq.get(0);
            if(s.getCTCSSCode() != null)
            {
                type = "ctcss";
                code = String.format(java.util.Locale.US, "%.1f", s.getCTCSSCode().getFrequency());
            }
            else if(s.getDCSCode() != null)
            {
                type = "dcs";
                String n = s.getDCSCode().name();   //N023 or I023
                code = n.substring(1) + (n.startsWith("I") ? "I" : "");
            }
        }
        m.put("squelchType", type);
        m.put("squelchCode", code);
    }

    /**
     * Applies NBFM settings. Squelch codes: CTCSS as the tone in Hz (100.0), DCS as the 3-digit code (023), with I on
     * the end for inverted (023I) - the same codes SDR-Trunk's own lists offer.
     */
    private static void applyNbfm(io.github.dsheirer.module.decode.nbfm.DecodeConfigNBFM nbfm, Map<String,Object> change,
                                  List<String> done) throws Exception
    {
        if(change.containsKey("bandwidth"))
        {
            String v = str(change.get("bandwidth"));
            io.github.dsheirer.module.decode.analog.DecodeConfigAnalog.Bandwidth bw = switch(v)
            {
                case "7.5" -> io.github.dsheirer.module.decode.analog.DecodeConfigAnalog.Bandwidth.BW_7_5;
                case "12.5" -> io.github.dsheirer.module.decode.analog.DecodeConfigAnalog.Bandwidth.BW_12_5;
                case "25", "25.0" -> io.github.dsheirer.module.decode.analog.DecodeConfigAnalog.Bandwidth.BW_25_0;
                default -> throw new Exception("NBFM bandwidth must be 7.5, 12.5 or 25 kHz");
            };
            nbfm.setBandwidth(bw);
            done.add("bandwidth");
        }
        if(change.containsKey("deemphasis"))
        {
            nbfm.setDeemphasis("300".equals(str(change.get("deemphasis"))) ?
                io.github.dsheirer.module.decode.nbfm.DeemphasisMode.NBFM_300 : io.github.dsheirer.module.decode.nbfm.DeemphasisMode.NONE);
            done.add("de-emphasis");
        }
        if(change.containsKey("audioFilter"))
        {
            nbfm.setAudioFilter(Boolean.TRUE.equals(change.get("audioFilter")));
            done.add("audio filter");
        }
        if(change.containsKey("alc"))
        {
            nbfm.setAudioALC(Boolean.TRUE.equals(change.get("alc")));
            done.add("ALC");
        }
        if(change.containsKey("talkgroup"))
        {
            int tg;
            try
            {
                tg = Integer.parseInt(str(change.get("talkgroup")));
            }
            catch(NumberFormatException nfe)
            {
                throw new Exception("talkgroup must be a number from 1 to 65535");
            }
            if(tg < 1 || tg > 65535)
            {
                throw new Exception("talkgroup must be a number from 1 to 65535");
            }
            nbfm.setTalkgroup(tg);
            done.add("talkgroup");
        }
        if(change.containsKey("squelchType") || change.containsKey("squelchCode"))
        {
            String type = str(change.get("squelchType")).toLowerCase();
            String code = str(change.get("squelchCode")).toUpperCase();
            List<io.github.dsheirer.module.decode.squelch.SquelchDecoderConfig> list = new ArrayList<>();
            if(type.equals("ctcss"))
            {
                float hz;
                try
                {
                    hz = Float.parseFloat(code);
                }
                catch(NumberFormatException nfe)
                {
                    throw new Exception("CTCSS tone must be in Hz, for example 100.0");
                }
                io.github.dsheirer.module.decode.squelch.ctcss.CTCSSCode ctcss =
                    io.github.dsheirer.module.decode.squelch.ctcss.CTCSSCode.fromFrequency(hz);
                if(!io.github.dsheirer.module.decode.squelch.ctcss.CTCSSCode.STANDARD_CODES.contains(ctcss))
                {
                    throw new Exception(code + " Hz isn't a standard CTCSS tone");
                }
                list.add(new io.github.dsheirer.module.decode.squelch.SquelchDecoderConfig(
                    io.github.dsheirer.module.decode.squelch.SquelchDecoderConfig.SquelchType.CTCSS, ctcss.name()));
            }
            else if(type.equals("dcs"))
            {
                boolean inverted = code.endsWith("I");
                String digits = code.replaceAll("[^0-9]", "");
                while(digits.length() < 3)
                {
                    digits = "0" + digits;
                }
                io.github.dsheirer.module.decode.squelch.dcs.DCSCode dcs;
                try
                {
                    dcs = io.github.dsheirer.module.decode.squelch.dcs.DCSCode.valueOf((inverted ? "I" : "N") + digits);
                }
                catch(IllegalArgumentException iae)
                {
                    throw new Exception(code + " isn't a DCS code - use the 3 digits, for example 023 (023I for inverted)");
                }
                list.add(new io.github.dsheirer.module.decode.squelch.SquelchDecoderConfig(
                    io.github.dsheirer.module.decode.squelch.SquelchDecoderConfig.SquelchType.DCS, dcs.name()));
            }
            else
            {
                list.add(new io.github.dsheirer.module.decode.squelch.SquelchDecoderConfig(
                    io.github.dsheirer.module.decode.squelch.SquelchDecoderConfig.SquelchType.NONE, null));
            }
            nbfm.setSquelchDecoders(list);
            done.add("squelch");
        }
    }

    private static String str(Object o)
    {
        return o == null ? "" : String.valueOf(o).trim();
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

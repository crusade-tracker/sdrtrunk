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
import io.github.dsheirer.alias.AliasModel;
import io.github.dsheirer.audio.broadcast.BroadcastConfiguration;
import io.github.dsheirer.audio.broadcast.BroadcastModel;
import io.github.dsheirer.audio.broadcast.rdioscanner.RdioScannerConfiguration;
import io.github.dsheirer.channel.metadata.ChannelMetadata;
import io.github.dsheirer.channel.metadata.ChannelMetadataModel;
import io.github.dsheirer.controller.channel.Channel;
import io.github.dsheirer.controller.channel.ChannelEvent;
import io.github.dsheirer.controller.channel.ChannelProcessingManager;
import io.github.dsheirer.identifier.Identifier;
import io.github.dsheirer.identifier.IdentifierCollection;
import io.github.dsheirer.module.decode.event.DecodeEventType;
import io.github.dsheirer.module.decode.event.IDecodeEvent;
import io.github.dsheirer.properties.SystemProperties;
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
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.CompletableFuture;
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
    private String mLastProblem;

    public StatusReporter(TunerManager tunerManager, ChannelProcessingManager channelProcessingManager,
                          BroadcastModel broadcastModel, AliasModel aliasModel)
    {
        mTunerManager = tunerManager;
        mChannelProcessingManager = channelProcessingManager;
        mBroadcastModel = broadcastModel;
        mAliasModel = aliasModel;

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
        mExecutor.scheduleWithFixedDelay(this::report, 10, INTERVAL_SECONDS, TimeUnit.SECONDS);
        mLog.info("Status reporter started - reports to the Rdio Scanner server " + STATUS_PATH);
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

            //The same event object is updated and re-sent while a call continues - count it once
            if(kind == null || !mSeenEvents.add(event))
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
                problem("no enabled Rdio Scanner streaming configuration - status reports are off");
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
            body.put("tuners", tuners(events));
            body.put("channels", channels());
            body.put("events", events);

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
            }
            else
            {
                requeue(events);
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

    private static String firstAlias(List<Alias> aliases)
    {
        return aliases != null && !aliases.isEmpty() && aliases.get(0) != null ? aliases.get(0).getName() : null;
    }
}

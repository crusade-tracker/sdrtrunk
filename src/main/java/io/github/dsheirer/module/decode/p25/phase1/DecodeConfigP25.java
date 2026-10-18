/*
 * *****************************************************************************
 * Copyright (C) 2014-2024 Dennis Sheirer
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
package io.github.dsheirer.module.decode.p25.phase1;


import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import io.github.dsheirer.module.decode.config.DecodeConfiguration;
import io.github.dsheirer.module.decode.p25.phase2.DecodeConfigP25Phase2;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = DecodeConfigP25Phase1.class, name = "decodeConfigP25Phase1"),
        @JsonSubTypes.Type(value = DecodeConfigP25Phase2.class, name = "decodeConfigP25Phase2"),
})
public abstract class DecodeConfigP25 extends DecodeConfiguration
{
    private int mTrafficChannelPoolSize = TRAFFIC_CHANNEL_LIMIT_DEFAULT;
    private boolean mIgnoreDataCalls = false;
    private boolean mIgnoreDoNotMonitor = false;

    public DecodeConfigP25()
    {
    }

    @JacksonXmlProperty(isAttribute = true, localName = "ignore_data_calls")
    public boolean getIgnoreDataCalls()
    {
        return mIgnoreDataCalls;
    }

    public void setIgnoreDataCalls(boolean ignore)
    {
        mIgnoreDataCalls = ignore;
    }

    /**
     * Indicates if calls to a talkgroup whose alias has the Do Not Monitor audio priority should not be decoded.  When
     * enabled, no traffic channel is allocated for those calls, saving the processing load of decoding them.
     */
    @JacksonXmlProperty(isAttribute = true, localName = "ignore_do_not_monitor")
    public boolean getIgnoreDoNotMonitor()
    {
        return mIgnoreDoNotMonitor;
    }

    public void setIgnoreDoNotMonitor(boolean ignore)
    {
        mIgnoreDoNotMonitor = ignore;
    }

    //Rod build: follow only talkgroups that have an alias in this channel's alias list (off = stock: follow every call)
    private boolean mFollowAliasedOnly = false;

    /**
     * Rod build: when on, calls to talkgroups with no alias in the channel's alias list aren't followed - no traffic
     * channel, no tuner time. They are still seen on the control channel and reported as "not followed".
     */
    @JacksonXmlProperty(isAttribute = true, localName = "follow_aliased_only")
    public boolean getFollowAliasedOnly()
    {
        return mFollowAliasedOnly;
    }

    public void setFollowAliasedOnly(boolean followAliasedOnly)
    {
        mFollowAliasedOnly = followAliasedOnly;
    }


    @JacksonXmlProperty(isAttribute = true, localName = "traffic_channel_pool_size")
    public int getTrafficChannelPoolSize()
    {
        return mTrafficChannelPoolSize;
    }

    /**
     * Sets the traffic channel pool size which is the maximum number of
     * simultaneous traffic channels that can be allocated.
     *
     * This limits the maximum calls so that busy systems won't cause more
     * traffic channels to be allocated than the decoder/software/host computer
     * can support.
     */
    public void setTrafficChannelPoolSize(int size)
    {
        mTrafficChannelPoolSize = size;
    }
}

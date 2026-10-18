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

package io.github.dsheirer.module.decode.nbfm;

import io.github.dsheirer.module.decode.squelch.ctcss.CTCSSCode;

/**
 * Rod build: measures which CTCSS tone each NBFM transmission carries - measurement only, it never mutes anything.
 * Uses the same test as SDR-Trunk's CTCSSDetector on each 512-sample block of 8 kHz audio: the strongest tone of all
 * CTCSS frequencies, at least 100 in power, not the guard tone at either end, and more than 3 standard deviations
 * above the mean of all of them. The call reports the tone found in the most blocks, out of all blocks measured.
 */
public class ToneMeter
{
    private static final double SAMPLE_RATE = 8000.0;
    private static final int MIN_BLOCK_SIZE = 512;
    private static final double MIN_POWER = 100;
    private static final double K_FACTOR = 3.0;

    private final CTCSSCode[] mCodes;
    private final double[] mCoefficients;
    private final int[] mHits;
    private int mBlocks;

    public ToneMeter()
    {
        mCodes = CTCSSCode.DETECTING_CODES.toArray(new CTCSSCode[0]);
        mCoefficients = new double[mCodes.length];
        for(int i = 0; i < mCodes.length; i++)
        {
            mCoefficients[i] = 2.0 * Math.cos(2.0 * Math.PI * mCodes[i].getFrequency() / SAMPLE_RATE);
        }
        mHits = new int[mCodes.length];
    }

    /**
     * Measures one buffer of resampled (8 kHz) demodulated audio, before any audio filters.
     */
    public synchronized void measure(float[] samples)
    {
        if(samples == null || samples.length < MIN_BLOCK_SIZE)
        {
            return;
        }
        mBlocks++;
        double[] power = new double[mCodes.length];
        double max = 0, sum = 0;
        int maxIndex = -1;
        for(int i = 0; i < mCodes.length; i++)
        {
            power[i] = goertzel(samples, mCoefficients[i]);
            sum += power[i];
            if(power[i] > max)
            {
                max = power[i];
                maxIndex = i;
            }
        }
        if(max < MIN_POWER || maxIndex <= 0 || maxIndex == mCodes.length - 1)
        {
            return;
        }
        double mean = sum / mCodes.length, var = 0;
        for(double p: power)
        {
            var += (p - mean) * (p - mean);
        }
        double threshold = Math.sqrt(var / (mCodes.length - 1)) * K_FACTOR + mean;
        if(max > threshold)
        {
            mHits[maxIndex]++;
        }
    }

    /**
     * Starts a new transmission
     */
    public synchronized void reset()
    {
        java.util.Arrays.fill(mHits, 0);
        mBlocks = 0;
    }

    /**
     * Result for the transmission so far: [tone in Hz as "100.0" or null, blocks with that tone, blocks measured]
     */
    public synchronized Object[] result()
    {
        int best = top(mHits);
        return new Object[]{hz(best), best >= 0 ? mHits[best] : 0, mBlocks};
    }

    private static int top(int[] counts)
    {
        int best = -1;
        for(int i = 0; i < counts.length; i++)
        {
            if(counts[i] > 0 && (best < 0 || counts[i] > counts[best]))
            {
                best = i;
            }
        }
        return best;
    }

    private String hz(int index)
    {
        return index >= 0 ? String.format(java.util.Locale.US, "%.1f", mCodes[index].getFrequency()) : null;
    }

    private static double goertzel(float[] samples, double coefficient)
    {
        double s1 = 0, s2 = 0;
        for(float sample: samples)
        {
            double s0 = sample + coefficient * s1 - s2;
            s2 = s1;
            s1 = s0;
        }
        return s1 * s1 + s2 * s2 - coefficient * s1 * s2;
    }
}

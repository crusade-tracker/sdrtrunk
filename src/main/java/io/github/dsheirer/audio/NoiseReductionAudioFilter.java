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
package io.github.dsheirer.audio;

/**
 * Rod build: hiss (noise) reduction for NBFM audio, 8 kHz.
 *
 * Short-time spectral noise reduction: the audio is cut into 32 ms frames (50% overlap, sqrt-Hann windows so the
 * frames add back up exactly). For each frequency bin it keeps learning the steady background level - the lowest
 * smoothed level over about the last 1.5 seconds, near-silent frames left out - and turns that bin down by how much of it is noise (a Wiener gain with the
 * decision-directed estimate, which keeps the "musical" twinkle low). Speech keeps changing, so it stays; steady hiss
 * is turned down, but never below the level's floor - that floor is what keeps voices from sounding watery.
 *
 * Adds 32 ms of delay. The noise estimate is kept between transmissions (a repeater's hiss stays much the same); the
 * frame buffers start fresh after a gap, so the end of one call never leaks into the next.
 */
public class NoiseReductionAudioFilter extends AbstractAudioFilter
{
    private static final int N = 256;            //32 ms at 8 kHz
    private static final int HOP = N / 2;        //16 ms
    private static final int BINS = N / 2 + 1;
    private static final long GAP_NANOS = 250_000_000L;   //no audio for this long = a new transmission

    private static final float[] WINDOW = new float[N];
    private static final int[] BIT_REVERSE = new int[N];
    private static final float[] COS = new float[N / 2];
    private static final float[] SIN = new float[N / 2];

    static
    {
        for(int n = 0; n < N; n++)
        {
            WINDOW[n] = (float)Math.sqrt(0.5 - 0.5 * Math.cos(2.0 * Math.PI * n / N));   //periodic sqrt-Hann
        }
        int bits = Integer.numberOfTrailingZeros(N);
        for(int i = 0; i < N; i++)
        {
            BIT_REVERSE[i] = Integer.reverse(i) >>> (32 - bits);
        }
        for(int i = 0; i < N / 2; i++)
        {
            COS[i] = (float)Math.cos(-2.0 * Math.PI * i / N);
            SIN[i] = (float)Math.sin(-2.0 * Math.PI * i / N);
        }
    }

    private final float mFloor;            //lowest gain: how far hiss can be turned down
    private final float mOverSubtract;     //how strongly the noise estimate is applied

    private final float[] mFrame = new float[N];       //last N input samples
    private int mFill = 0;                              //new samples in mFrame since the last frame
    private final float[] mOverlap = new float[N];     //overlap-add accumulator
    private float[] mOut = new float[4096];            //output FIFO
    private int mOutCount = 0;

    private final float[] mSmoothed = new float[BINS];
    private final float[] mNoise = new float[BINS];
    //Minimum statistics: the lowest smoothed level over the last SUBWINDOWS x SUB_FRAMES frames (about 1.5 seconds)
    private static final int SUB_FRAMES = 12;
    private static final int SUBWINDOWS = 8;
    private final float[][] mSubMin = new float[SUBWINDOWS][BINS];
    private final float[] mCurrentMin = new float[BINS];
    private int mSubIndex = 0;
    private int mSubFrames = 0;
    private int mSubFilled = 0;
    //A frame quieter than this (about -60 dBFS) is silence, not hiss - it is not used to learn the hiss level
    private static final float SILENT_FRAME_POWER = 1e-6f * N * N / 4f;
    private final float[] mPrevClean = new float[BINS];   //|gain * X|^2 of the previous frame
    private boolean mNoiseKnown = false;
    private long mLastNanos = 0;

    private final float[] mRe = new float[N];
    private final float[] mIm = new float[N];

    /**
     * Noise reduction strength, as offered on the channel's NBFM settings.
     */
    public enum Level
    {
        OFF("Off", 1.0f, 1.0f),
        LOW("Low", 0.45f, 1.0f),
        MEDIUM("Medium", 0.28f, 1.4f),
        HIGH("High", 0.16f, 2.0f);

        private final String mLabel;
        private final float mFloor;
        private final float mOverSubtract;

        Level(String label, float floor, float overSubtract)
        {
            mLabel = label;
            mFloor = floor;
            mOverSubtract = overSubtract;
        }

        @Override
        public String toString()
        {
            return mLabel;
        }

        /**
         * Level from a stored name, OFF if unknown or empty
         */
        public static Level fromName(String name)
        {
            if(name != null)
            {
                for(Level level: values())
                {
                    if(level.name().equalsIgnoreCase(name.trim()) || level.mLabel.equalsIgnoreCase(name.trim()))
                    {
                        return level;
                    }
                }
            }
            return OFF;
        }
    }

    public NoiseReductionAudioFilter(Level level)
    {
        mFloor = level.mFloor;
        mOverSubtract = level.mOverSubtract;
        startFresh();
    }

    /**
     * New transmission: empty frame and output buffers (the noise estimate stays). Output starts with HOP samples of
     * silence, so every call returns as many samples as it was given (32 ms delay in all).
     */
    private void startFresh()
    {
        java.util.Arrays.fill(mFrame, 0f);
        java.util.Arrays.fill(mOverlap, 0f);
        java.util.Arrays.fill(mPrevClean, 0f);
        mFill = 0;
        mOutCount = 0;
        pushOut(new float[HOP], HOP);
        if(mNoiseKnown)
        {
            System.arraycopy(mNoise, 0, mSmoothed, 0, BINS);   //the hiss estimate carries over to the next transmission
        }
    }

    @Override
    public float[] filter(float[] audio)
    {
        if(audio == null || audio.length == 0)
        {
            return audio;
        }

        long now = System.nanoTime();
        if(mLastNanos != 0 && now - mLastNanos > GAP_NANOS)
        {
            startFresh();
        }
        mLastNanos = now;

        for(float sample: audio)
        {
            //shift in one sample at the end of the frame window
            mFrame[N - HOP + mFill] = sample;
            mFill++;
            if(mFill == HOP)
            {
                processFrame();
                System.arraycopy(mFrame, HOP, mFrame, 0, N - HOP);
                mFill = 0;
            }
        }

        float[] out = new float[audio.length];
        int take = Math.min(audio.length, mOutCount);
        System.arraycopy(mOut, 0, out, audio.length - take, take);   //(only short at the very start)
        System.arraycopy(mOut, take, mOut, 0, mOutCount - take);
        mOutCount -= take;
        return out;
    }

    private void pushOut(float[] samples, int count)
    {
        if(mOutCount + count > mOut.length)
        {
            mOut = java.util.Arrays.copyOf(mOut, Math.max(mOut.length * 2, mOutCount + count));
        }
        System.arraycopy(samples, 0, mOut, mOutCount, count);
        mOutCount += count;
    }

    private void processFrame()
    {
        for(int n = 0; n < N; n++)
        {
            mRe[n] = mFrame[n] * WINDOW[n];
            mIm[n] = 0f;
        }
        fft(mRe, mIm, false);

        float total = 0f;
        for(int k = 0; k < BINS; k++)
        {
            total += mRe[k] * mRe[k] + mIm[k] * mIm[k];
        }
        boolean learn = total / BINS > SILENT_FRAME_POWER;
        if(learn)
        {
            learnNoise();
        }
        if(!mNoiseKnown)
        {
            //nothing learned yet (silence so far) - pass the audio through untouched
            pushWindowed();
            return;
        }

        for(int k = 0; k < BINS; k++)
        {
            float power = mRe[k] * mRe[k] + mIm[k] * mIm[k];
            float noise = Math.max(mNoise[k] * 1.3f * mOverSubtract, 1e-12f);   //minimum sits below the mean noise

            //decision-directed a-priori SNR -> Wiener gain
            float post = power / noise;
            float prio = 0.88f * (mPrevClean[k] / noise) + 0.12f * Math.max(post - 1f, 0f);
            float gain = prio / (1f + prio);
            if(gain < mFloor)
            {
                gain = mFloor;
            }
            mPrevClean[k] = gain * gain * power;

            mRe[k] *= gain;
            mIm[k] *= gain;
            if(k > 0 && k < N / 2)
            {
                mRe[N - k] = mRe[k];
                mIm[N - k] = -mIm[k];
            }
        }
        fft(mRe, mIm, true);
        overlapAdd();
    }

    /**
     * Unprocessed frame: the windowed input goes back out as it came in.
     */
    private void pushWindowed()
    {
        for(int n = 0; n < N; n++)
        {
            mRe[n] = mFrame[n] * WINDOW[n];
        }
        overlapAdd();
    }

    private void overlapAdd()
    {
        for(int n = 0; n < N; n++)
        {
            mOverlap[n] += mRe[n] * WINDOW[n];
        }
        pushOut(mOverlap, HOP);
        System.arraycopy(mOverlap, HOP, mOverlap, 0, N - HOP);
        java.util.Arrays.fill(mOverlap, N - HOP, N, 0f);
    }

    /**
     * Updates the hiss estimate from this frame (mRe / mIm hold its spectrum): smoothed level per bin, and the lowest
     * smoothed level over about the last 1.5 seconds - so it follows the hiss up or down within that time.
     */
    private void learnNoise()
    {
        boolean first = !mNoiseKnown;
        for(int k = 0; k < BINS; k++)
        {
            float power = mRe[k] * mRe[k] + mIm[k] * mIm[k];
            mSmoothed[k] = first ? power : 0.8f * mSmoothed[k] + 0.2f * power;
            mCurrentMin[k] = mSubFrames == 0 ? mSmoothed[k] : Math.min(mCurrentMin[k], mSmoothed[k]);
        }
        mSubFrames++;
        if(mSubFrames >= SUB_FRAMES)
        {
            System.arraycopy(mCurrentMin, 0, mSubMin[mSubIndex], 0, BINS);
            mSubIndex = (mSubIndex + 1) % SUBWINDOWS;
            mSubFilled = Math.min(mSubFilled + 1, SUBWINDOWS);
            mSubFrames = 0;
        }
        for(int k = 0; k < BINS; k++)
        {
            float min = mSubFrames > 0 ? mCurrentMin[k] : Float.MAX_VALUE;
            for(int w = 0; w < mSubFilled; w++)
            {
                min = Math.min(min, mSubMin[w][k]);
            }
            mNoise[k] = min;
        }
        mNoiseKnown = true;
    }

    /**
     * In-place radix-2 FFT of size N. Inverse includes the 1/N scale.
     */
    private static void fft(float[] re, float[] im, boolean inverse)
    {
        for(int i = 0; i < N; i++)
        {
            int j = BIT_REVERSE[i];
            if(j > i)
            {
                float t = re[i]; re[i] = re[j]; re[j] = t;
                t = im[i]; im[i] = im[j]; im[j] = t;
            }
        }
        for(int size = 2; size <= N; size <<= 1)
        {
            int half = size >> 1;
            int step = N / size;
            for(int start = 0; start < N; start += size)
            {
                for(int k = 0; k < half; k++)
                {
                    float wr = COS[k * step];
                    float wi = inverse ? -SIN[k * step] : SIN[k * step];
                    int a = start + k, b = a + half;
                    float tr = re[b] * wr - im[b] * wi;
                    float ti = re[b] * wi + im[b] * wr;
                    re[b] = re[a] - tr;
                    im[b] = im[a] - ti;
                    re[a] += tr;
                    im[a] += ti;
                }
            }
        }
        if(inverse)
        {
            float scale = 1f / N;
            for(int i = 0; i < N; i++)
            {
                re[i] *= scale;
                im[i] *= scale;
            }
        }
    }
}

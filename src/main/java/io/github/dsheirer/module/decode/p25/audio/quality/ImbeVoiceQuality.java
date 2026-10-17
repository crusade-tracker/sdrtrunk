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
package io.github.dsheirer.module.decode.p25.audio.quality;

import java.nio.ByteOrder;

/**
 * Rod build: voice quality of P25 Phase 1 calls. Checks each IMBE voice frame's error correction the same way the
 * JMBE codec does (de-interleave, Golay / Hamming check of the 7 coset words, de-randomize) and calls a frame bad when
 * the codec would have to repeat it (too many errors) or mute it (running error rate too high). Measurement only -
 * the frame bytes are copied, nothing that is played or recorded changes.
 */
public class ImbeVoiceQuality
{
    private static final int[] RANDOMIZER_SEED = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};
    private float mErrorRate = 0f;
    private int mLastErrors = 0;

    /**
     * Checks one IMBE frame (the 18 bytes handed to the codec).
     * @return true if the codec would repeat or mute this frame
     */
    public boolean isBad(byte[] data)
    {
        if(data == null || data.length < 18)
        {
            return true;
        }

        BinaryFrame frame = BinaryFrame.fromBytes(data, ByteOrder.LITTLE_ENDIAN);
        IMBEInterleave.deinterleave(frame);
        int errors0 = Golay23.checkAndCorrect(frame, 0);
        int total = errors0;
        derandomize(frame);
        total += Golay23.checkAndCorrect(frame, 23);
        total += Golay23.checkAndCorrect(frame, 46);
        total += Golay23.checkAndCorrect(frame, 69);
        total += Hamming15.checkAndCorrect(frame, 92);
        total += Hamming15.checkAndCorrect(frame, 107);
        total += Hamming15.checkAndCorrect(frame, 122);
        mLastErrors = total;

        //Same running error rate and thresholds as the codec (IMBE algorithms #97 / #98 and the mute threshold)
        mErrorRate = (0.95f * mErrorRate) + (0.000365f * total);
        boolean repeat = errors0 >= 2 && total >= (10.0f + (40.0f * mErrorRate));
        boolean mute = mErrorRate > 0.0875f;
        return repeat || mute;
    }

    /**
     * Bit errors found in the last frame checked
     */
    public int getLastErrors()
    {
        return mLastErrors;
    }

    /**
     * Start of a new call
     */
    public void reset()
    {
        mErrorRate = 0f;
        mLastErrors = 0;
    }

    private static void derandomize(BinaryFrame frame)
    {
        int offset = 23;
        int seed = frame.getInt(RANDOMIZER_SEED);
        int prX = 16 * seed;

        for(int x = 0; x < 114; x++)
        {
            prX = (173 * prX + 13849) % 65536;

            if(prX >= 32768)
            {
                frame.flip(x + offset);
            }
        }
    }
}

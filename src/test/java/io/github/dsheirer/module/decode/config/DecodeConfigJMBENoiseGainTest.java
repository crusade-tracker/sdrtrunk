package io.github.dsheirer.module.decode.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.JacksonXmlModule;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import io.github.dsheirer.module.decode.dmr.DecodeConfigDMR;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Rod build: comfort noise gain saved as exactly 1.0 (the old Rod default) loads as 0; other values load as saved;
 * the editor setter is not changed; saving writes the value back.
 */
public class DecodeConfigJMBENoiseGainTest
{
    private ObjectMapper mapper()
    {
        JacksonXmlModule module = new JacksonXmlModule();
        module.setDefaultUseWrapper(false);
        return new XmlMapper(module).configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private float load(String gain) throws Exception
    {
        String xml = "<config xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:jmbeNoiseGain=\"" + gain + "\"/>";
        return mapper().readValue(xml, DecodeConfigDMR.class).getComfortNoiseGain();
    }

    @Test
    public void loading() throws Exception
    {
        assertEquals(0.0f, load("1.0"), 0.0001, "old Rod default 1.0 loads as 0");
        assertEquals(0.5f, load("0.5"), 0.0001, "a value set on purpose is kept");
        assertEquals(0.05f, load("0.05"), 0.0001);
        assertEquals(0.0f, new DecodeConfigDMR().getComfortNoiseGain(), 0.0001, "new channels default to 0");
    }

    @Test
    public void editorAndSave() throws Exception
    {
        DecodeConfigDMR config = new DecodeConfigDMR();
        config.setComfortNoiseGain(1.0f);
        assertEquals(1.0f, config.getComfortNoiseGain(), 0.0001, "the editor setter keeps 1.0");
        config.setComfortNoiseGain(0.3f);
        String xml = mapper().writeValueAsString(config);
        assertEquals(0.3f, mapper().readValue(xml, DecodeConfigDMR.class).getComfortNoiseGain(), 0.0001, "round trip: " + xml);
    }
}

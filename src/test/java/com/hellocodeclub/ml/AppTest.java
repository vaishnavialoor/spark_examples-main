package com.hellocodeclub.ml;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Unit test for simple App.
 */
public class AppTest 
{
    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAnswerWithTrue()
    {
        String term = "\"{%why";
        term = term.replaceAll("\\p{Punct}","");

        assertEquals("why",term);
    }
}

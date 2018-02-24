package io.monkeypatch.mktd6.model.gibber;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Gibber is the twitter equivalent in the monkonomy.
 */
public class Gibb {

    @JsonProperty
    private final String text;

    @JsonCreator
    public Gibb(@JsonProperty("text") String text) {
        this.text = text;
    }

    public String getText() {
        return text;
    }

}

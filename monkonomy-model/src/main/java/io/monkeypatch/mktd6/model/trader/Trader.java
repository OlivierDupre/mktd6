package io.monkeypatch.mktd6.model.trader;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.monkeypatch.mktd6.model.Team;

public class Trader {

    private final Team team;
    private final String name;

    @JsonCreator
    public Trader(@JsonProperty("team") Team team, @JsonProperty("name") String name) {
        this.team = team;
        this.name = name;
    }

    public Team getTeam() {
        return team;
    }

    public String getName() {
        return name;
    }

}

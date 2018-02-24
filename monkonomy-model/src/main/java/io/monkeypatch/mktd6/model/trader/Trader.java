package io.monkeypatch.mktd6.model.trader;

import com.fasterxml.jackson.annotation.JsonCreator;
import io.monkeypatch.mktd6.model.Team;

public class Trader {

    private final Team team;
    private final String name;

    @JsonCreator
    public Trader(Team team, String name) {
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

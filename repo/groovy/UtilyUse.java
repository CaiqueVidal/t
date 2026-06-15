package com.exemplo.groovy;

import java.util.List;

public class UtilyUse {
    private final String utilitario;
    private final List<String> usadoEmRegraGroovy;

    public UtilyUse(String utilitario, List<String> usadoEmRegraGroovy) {
        this.utilitario = utilitario;
        this.usadoEmRegraGroovy = usadoEmRegraGroovy;
    }

    public String getUtilitario() {
        return utilitario;
    }

    public List<String> getUsadoEmRegraGroovy() {
        return usadoEmRegraGroovy;
    }

    @Override
    public String toString() {
        return "UsoUtilitario{" +
                "utilitario='" + utilitario + '\'' +
                ", usadoEmRegraGroovy=" + usadoEmRegraGroovy +
                '}';
    }
}
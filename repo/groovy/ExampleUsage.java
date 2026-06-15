package com.exemplo.groovy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class ExampleUsage {

    public static void main(String[] args) throws IOException {
        String nomeRecurso = "scripts/sample.groovy";

        String scriptGroovy;
        try (InputStream in = ExampleUsage.class.getClassLoader().getResourceAsStream(nomeRecurso)) {
            if (in == null) {
                throw new IOException("Recurso não encontrado no classpath: " + nomeRecurso);
            }
            scriptGroovy = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }

        List<String> utilitarios = List.of(
                "foo",
                "bar",
                "clienteUtil",
                "pedidoUtil",
                "calcUtil",
                "atrib"
        );

        UtilityPathExtractor extractor = new UtilityPathExtractor();
        List<UtilyUse> resultado = extractor.extract(utilitarios, scriptGroovy);

        ObjectMapper mapper = new ObjectMapper();
        mapper.enable(SerializationFeature.INDENT_OUTPUT);

        System.out.println(mapper.writeValueAsString(resultado));
    }
}
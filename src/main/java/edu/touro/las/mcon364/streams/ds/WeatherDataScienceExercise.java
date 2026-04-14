package edu.touro.las.mcon364.streams.ds;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.util.*;
import java.util.stream.*;


public class WeatherDataScienceExercise {

    record WeatherRecord(
            String stationId,
            String city,
            String date,
            double temperatureC,
            int humidity,
            double precipitationMm
    ) {}

    public static void main(String[] args) throws Exception {
        List<String> rows = readCsvRows("noaa_weather_sample_200_rows.csv");

        List<WeatherRecord> cleaned = rows.stream()
                .skip(1) // skip header
                .map(WeatherDataScienceExercise::parseRow)
                .flatMap(Optional::stream)
                .filter(WeatherDataScienceExercise::isValid)
                .toList();

        System.out.println("Total raw rows (excluding header): " + (rows.size() - 1));
        System.out.println("Total cleaned rows: " + cleaned.size());

        double avg = cleaned.stream().mapToDouble(WeatherRecord::temperatureC).average().orElse(0.0);

        // Find the city with the highest average temperature.
        String high = cleaned.stream().collect(Collectors.groupingBy(WeatherRecord::city, Collectors.averagingDouble(WeatherRecord::temperatureC))).entrySet().stream().max(Comparator.comparing(Map.Entry::getValue)).map(Map.Entry::getKey).get();

        // Group records by city.
        Map<String, List<WeatherRecord>> groups = cleaned.stream().collect(Collectors.groupingBy(WeatherRecord::city));

        // Compute average precipitation by city.
        Map<String, Double> avgGroup = cleaned.stream().collect(Collectors.groupingBy(WeatherRecord::city, Collectors.averagingDouble(WeatherRecord::precipitationMm)));

        // Partition rows into freezing days (temperature <= 0)
        // and non-freezing days (temperature > 0).
        Map<Boolean, List<WeatherRecord>> freezing = cleaned.stream().collect(Collectors.partitioningBy(w -> w.temperatureC <= 0));

        // Create a Set<String> of all distinct cities.
        Set<String> cities = cleaned.stream().map(WeatherRecord::city).collect(Collectors.toSet());

        // Find the wettest single day.
        WeatherRecord wettest = cleaned.stream().max(Comparator.comparingDouble(WeatherRecord::precipitationMm)).orElse(null);

        // Create a Map<String, Double> from city to average humidity.
        Map<String, Double> humidity = cleaned.stream().collect(Collectors.groupingBy(WeatherRecord::city, Collectors.averagingDouble(r -> r.humidity)));

        // Produce a list of formatted strings like:
        // "Miami on 2025-01-02: 25.1C, humidity 82%"
        List<String> formatted = cleaned.stream().map(r -> r.city + "on" + r.date + ": " + r.temperatureC + "C, humidity" + r.humidity + "%").collect(Collectors.toList());
        // TODO 11 (optional):
        // Build a Map<String, CityWeatherSummary> for all cities.

        // Put your code below these comments or refactor into helper methods.
    }

    static Optional<WeatherRecord> parseRow(String row) {
        String[] parts = row.split(",");
        if (parts.length != 6) {return Optional.empty();}
        if (parts[3].isBlank()) {return Optional.empty();}
        try{
            Double.parseDouble(parts[3]);
        }
        catch(NumberFormatException nfe){
            return Optional.empty();
        }
        String stationId = parts[0].trim();
        String city = parts[1].trim();
        String date = parts[2].trim();
        double temperatureC = Double.parseDouble(parts[3].trim());
        int humidity = Integer.parseInt(parts[4].trim());
        double precipitationMm = Double.parseDouble(parts[5].trim());
        return Optional.of(new WeatherRecord(stationId, city, date, temperatureC, humidity, precipitationMm));
    }

    static boolean isValid(WeatherRecord r) {
        if (r.temperatureC<-60||r.temperatureC>60) {return false;}
        if (r.humidity<0||r.humidity>100) {return false;}
        if (r.precipitationMm<0) {return false;}
        return true;
    }

    record CityWeatherSummary(
            String city,
            long dayCount,
            double avgTemp,
            double avgPrecipitation,
            double maxTemp
    ) {}

    private static List<String> readCsvRows(String fileName) throws IOException {
        InputStream in = WeatherDataScienceExercise.class.getResourceAsStream(fileName);
        if (in == null) {
            throw new NoSuchFileException("Classpath resource not found: " + fileName);
        }
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            return reader.lines().toList();
        }
    }
}

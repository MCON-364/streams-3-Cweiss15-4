package edu.touro.las.mcon364.streams.ds;

import edu.touro.las.mcon364.streams.ds.WeatherDataScienceExercise;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;


public class testWeather {

        // ---------------------------------------------------------------
        // Helper factories
        // ---------------------------------------------------------------

        private static WeatherDataScienceExercise.WeatherRecord record(
                double tempC, int humidity, double precipMm) {
            return new WeatherDataScienceExercise.WeatherRecord(
                    "ST00", "TestCity", "2025-01-01", tempC, humidity, precipMm);
        }

        // ---------------------------------------------------------------
        // parseRow — well-formed rows
        // ---------------------------------------------------------------

        @Test
        void parseRow_wellFormedRow_returnsPopulatedOptional() {
            Optional<WeatherDataScienceExercise.WeatherRecord> result =
                    WeatherDataScienceExercise.parseRow("ST01,New York,2025-01-01,3.3,61,3.2");
            assertTrue(result.isPresent(), "Well-formed row should parse successfully");
            WeatherDataScienceExercise.WeatherRecord r = result.get();
            assertEquals("ST01", r.stationId());
            assertEquals("New York", r.city());
            assertEquals("2025-01-01", r.date());
            assertEquals(3.3, r.temperatureC(), 0.001);
            assertEquals(61, r.humidity());
            assertEquals(3.2, r.precipitationMm(), 0.001);
        }

        // ---------------------------------------------------------------
        // parseRow — bad rows must return Optional.empty()
        // ---------------------------------------------------------------

        @Test
        void parseRow_tooFewColumns_returnsEmpty() {
            Optional<WeatherDataScienceExercise.WeatherRecord> result =
                    WeatherDataScienceExercise.parseRow("ST01,New York,2025-01-01");
            assertTrue(result.isEmpty(), "Row with too few columns should return empty");
        }

        @Test
        void parseRow_missingTemperature_returnsEmpty() {
            // temperature field is blank
            Optional<WeatherDataScienceExercise.WeatherRecord> result =
                    WeatherDataScienceExercise.parseRow("ST01,New York,2025-01-01,,61,3.2");
            assertTrue(result.isEmpty(), "Row with missing temperature should return empty");
        }

        @Test
        void parseRow_nonNumericTemperature_returnsEmpty() {
            Optional<WeatherDataScienceExercise.WeatherRecord> result =
                    WeatherDataScienceExercise.parseRow("ST01,New York,2025-01-01,N/A,61,3.2");
            assertTrue(result.isEmpty(), "Row with non-numeric temperature should return empty");
        }

        @Test
        void parseRow_emptyString_returnsEmpty() {
            Optional<WeatherDataScienceExercise.WeatherRecord> result =
                    WeatherDataScienceExercise.parseRow("");
            assertTrue(result.isEmpty(), "Empty string should return empty");
        }

        // ---------------------------------------------------------------
        // isValid — temperature boundaries
        // ---------------------------------------------------------------

        @Test
        void isValid_temperatureAtMaxBoundary_isValid() {
            assertTrue(WeatherDataScienceExercise.isValid(record(60.0, 50, 0.0)),
                    "Temperature exactly 60 should be valid");
        }

        @Test
        void isValid_temperatureAtMinBoundary_isValid() {
            assertTrue(WeatherDataScienceExercise.isValid(record(-60.0, 50, 0.0)),
                    "Temperature exactly -60 should be valid");
        }

        @Test
        void isValid_temperatureAboveMax_isInvalid() {
            assertFalse(WeatherDataScienceExercise.isValid(record(61.0, 50, 0.0)),
                    "Temperature 61 should be invalid");
        }

        @Test
        void isValid_temperatureBelowMin_isInvalid() {
            assertFalse(WeatherDataScienceExercise.isValid(record(-61.0, 50, 0.0)),
                    "Temperature -61 should be invalid");
        }

        // ---------------------------------------------------------------
        // isValid — humidity boundaries
        // ---------------------------------------------------------------

        @Test
        void isValid_humidityAtMinBoundary_isValid() {
            assertTrue(WeatherDataScienceExercise.isValid(record(20.0, 0, 0.0)),
                    "Humidity 0 should be valid");
        }

        @Test
        void isValid_humidityAtMaxBoundary_isValid() {
            assertTrue(WeatherDataScienceExercise.isValid(record(20.0, 100, 0.0)),
                    "Humidity 100 should be valid");
        }

        @Test
        void isValid_humidityBelowMin_isInvalid() {
            assertFalse(WeatherDataScienceExercise.isValid(record(20.0, -1, 0.0)),
                    "Humidity -1 should be invalid");
        }

        @Test
        void isValid_humidityAboveMax_isInvalid() {
            assertFalse(WeatherDataScienceExercise.isValid(record(20.0, 101, 0.0)),
                    "Humidity 101 should be invalid");
        }

        // ---------------------------------------------------------------
        // isValid — precipitation boundaries
        // ---------------------------------------------------------------

        @Test
        void isValid_precipitationZero_isValid() {
            assertTrue(WeatherDataScienceExercise.isValid(record(20.0, 50, 0.0)),
                    "Precipitation 0 should be valid");
        }

        @Test
        void isValid_negativePrecipitation_isInvalid() {
            assertFalse(WeatherDataScienceExercise.isValid(record(20.0, 50, -0.1)),
                    "Negative precipitation should be invalid");
        }

        // ---------------------------------------------------------------
        // Integration — real CSV
        // ---------------------------------------------------------------

        @Test
        void integration_cleanedListIsNonEmpty() throws Exception {
            List<String> rows = readCsvViaMain("noaa_weather_sample_200_rows.csv");
            List<WeatherDataScienceExercise.WeatherRecord> cleaned = rows.stream()
                    .skip(1)
                    .map(WeatherDataScienceExercise::parseRow)
                    .flatMap(Optional::stream)
                    .filter(WeatherDataScienceExercise::isValid)
                    .toList();
            assertFalse(cleaned.isEmpty(), "Cleaned list should not be empty");
        }

        @Test
        void integration_allCleanedRecordsPassIsValid() throws Exception {
            List<String> rows = readCsvViaMain("noaa_weather_sample_200_rows.csv");
            List<WeatherDataScienceExercise.WeatherRecord> cleaned = rows.stream()
                    .skip(1)
                    .map(WeatherDataScienceExercise::parseRow)
                    .flatMap(Optional::stream)
                    .filter(WeatherDataScienceExercise::isValid)
                    .toList();
            assertTrue(cleaned.stream().allMatch(WeatherDataScienceExercise::isValid),
                    "Every record in the cleaned list must pass isValid");
        }

        @Test
        void integration_cityWithHighestAvgTempIsNonNull() throws Exception {
            List<String> rows = readCsvViaMain("noaa_weather_sample_200_rows.csv");
            List<WeatherDataScienceExercise.WeatherRecord> cleaned = rows.stream()
                    .skip(1)
                    .map(WeatherDataScienceExercise::parseRow)
                    .flatMap(Optional::stream)
                    .filter(WeatherDataScienceExercise::isValid)
                    .toList();

            Optional<String> hottestCity = cleaned.stream()
                    .collect(java.util.stream.Collectors.groupingBy(
                            WeatherDataScienceExercise.WeatherRecord::city,
                            java.util.stream.Collectors.averagingDouble(
                                    WeatherDataScienceExercise.WeatherRecord::temperatureC)))
                    .entrySet().stream()
                    .max(java.util.Map.Entry.comparingByValue())
                    .map(java.util.Map.Entry::getKey);

            assertTrue(hottestCity.isPresent(), "Should find a city with the highest average temperature");
            assertFalse(hottestCity.get().isBlank(), "Hottest city name should not be blank");
        }

        @Test
        void integration_wettestDayHasNonNegativePrecipitation() throws Exception {
            List<String> rows = readCsvViaMain("noaa_weather_sample_200_rows.csv");
            Optional<WeatherDataScienceExercise.WeatherRecord> wettestDay = rows.stream()
                    .skip(1)
                    .map(WeatherDataScienceExercise::parseRow)
                    .flatMap(Optional::stream)
                    .filter(WeatherDataScienceExercise::isValid)
                    .max(java.util.Comparator.comparingDouble(
                            WeatherDataScienceExercise.WeatherRecord::precipitationMm));

            assertTrue(wettestDay.isPresent(), "Should find a wettest day");
            assertTrue(wettestDay.get().precipitationMm() >= 0,
                    "Wettest day precipitation should be >= 0");
        }

        // ---------------------------------------------------------------
        // Private helper — load CSV rows the same way the main class does
        // ---------------------------------------------------------------

        private List<String> readCsvViaMain(String fileName) throws Exception {
            try (java.io.InputStream in =
                         WeatherDataScienceExercise.class.getResourceAsStream(fileName)) {
                assertNotNull(in, "CSV resource not found on classpath: " + fileName);
                try (java.io.BufferedReader reader = new java.io.BufferedReader(
                        new java.io.InputStreamReader(in, java.nio.charset.StandardCharsets.UTF_8))) {
                    return reader.lines().toList();
                }
            }
        }
    }


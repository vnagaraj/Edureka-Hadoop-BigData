airports_relation = LOAD '/Users/vivekanandganapathynagarajan/Documents/Edureka/datasets/input/Air-datasets/airports_mod.dat' USING PigStorage(',') AS (airportid:chararray, airportname:chararray, city:chararray, country:chararray, iata:chararray, icao:chararray, latitude:chararray, longitude:chararray, altitude:chararray, timezone:chararray, dst:chararray, tz:chararray);
india_airport_relation = FILTER airports_relation BY country == 'India';
output_relation = FOREACH india_airport_relation GENERATE airportname,country;
rmf '/Users/vivekanandganapathynagarajan/Documents/Edureka/datasets/pig_output/air-output/airportsIndia'
store output_relation into '/Users/vivekanandganapathynagarajan/Documents/Edureka/datasets/pig_output/air-output/airportsIndia';

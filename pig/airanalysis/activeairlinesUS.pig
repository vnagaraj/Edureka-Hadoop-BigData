airlines_relation = LOAD '/Users/vivekanandganapathynagarajan/Documents/Edureka/datasets/input/Air-datasets/Final_airlines' USING PigStorage(',') AS (airlineid:chararray, name:chararray, alias:chararray, iata:chararray, icao:chararray, callsign:chararray, country:chararray, active:chararray);
active_relation = FILTER airlines_relation BY active == 'Y';
us_active_relation = FILTER active_relation BY country == 'United States';
output_relation = FOREACH us_active_relation GENERATE name;
rmf /Users/vivekanandganapathynagarajan/Documents/Edureka/datasets/pig_output/air-output/activeAirlinesUS
store output_relation into '/Users/vivekanandganapathynagarajan/Documents/Edureka/datasets/pig_output/air-output/activeAirlinesUS';

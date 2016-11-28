airlines_relation = LOAD '/Users/vivekanandganapathynagarajan/Documents/Edureka/datasets/input/Air-datasets/Final_airlines' USING PigStorage(',') AS (airlineid:chararray, name:chararray, alias:chararray, iata:chararray, icao:chararray, callsign:chararray, country:chararray, active:chararray);
routes_relation = LOAD '/Users/vivekanandganapathynagarajan/Documents/Edureka/datasets/input/Air-datasets/routes.dat' USING PigStorage(',') AS (airline:chararray, airlineid:chararray, sourceairport:chararray, sourceairportid:chararray, destinationairport:chararray, destinationairportid:chararray, codeshare:chararray, stops:chararray, equipment:chararray );
result = JOIN airlines_relation BY airlineid, routes_relation BY airlineid;
zero_stops_relation = FILTER result BY stops == '0';
output_relation = FOREACH zero_stops_relation GENERATE name,stops;
distinct_relation = DISTINCT output_relation;
rmf /Users/vivekanandganapathynagarajan/Documents/Edureka/datasets/pig_output/air-output/airlinesZeroStops
store distinct_relation into '/Users/vivekanandganapathynagarajan/Documents/Edureka/datasets/pig_output/air-output/airlinesZeroStops';

airports_relation = LOAD '/Users/vivekanandganapathynagarajan/Documents/Edureka/datasets/input/Air-datasets/airports_mod.dat' USING PigStorage(',') AS (airportid:chararray, airportname:chararray, city:chararray, country:chararray, iata:chararray, icao:chararray, latitude:chararray, longitude:chararray, altitude:chararray, timezone:chararray, dst:chararray, tz:chararray);
group_country_relation = GROUP airports_relation by country;
output_relation = FOREACH group_country_relation GENERATE FLATTEN(group) as (country), COUNT($1) as count;
order_desc_relation = order output_relation by count desc;
max_relation = limit order_desc_relation 1;
rmf /Users/vivekanandganapathynagarajan/Documents/Edureka/datasets/pig_output/air-output/countryhighestAirports
store max_relation into '/Users/vivekanandganapathynagarajan/Documents/Edureka/datasets/pig_output/air-output/countryhighestAirports';

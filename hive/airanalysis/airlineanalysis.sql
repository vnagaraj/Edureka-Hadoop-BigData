create database IF NOT EXISTS airdb;
use airdb;
create table IF NOT EXISTS airports(airportid String, name String, city String, country String, iata String, icao String, latitude Float, longitude Float, altitude Float, timezone String, dst String, tz String) row format delimited fields terminated by ',' ;
create table IF NOT EXISTS airlines(airlineid String, name String, alias String, iata String, icao String, callsign String, country String, active String) row format delimited fields terminated by ',' ;
create table IF NOT EXISTS routes(airline String, airlineid String, sourceairport String, sourceairportid String, destairport String, destairportid String, codeshare String, stops Int, equipment String) row format delimited fields terminated by ',' ;
LOAD DATA LOCAL INPATH '/Users/vivekanandganapathynagarajan/Documents/Edureka/datasets/input/Air-datasets/airports_mod.dat' OVERWRITE INTO TABLE airports;
LOAD DATA LOCAL INPATH '/Users/vivekanandganapathynagarajan/Documents/Edureka/datasets/input/Air-datasets/Final_airlines' OVERWRITE INTO TABLE airlines;
LOAD DATA LOCAL INPATH '/Users/vivekanandganapathynagarajan/Documents/Edureka/datasets/input/Air-datasets/routes.dat' OVERWRITE INTO TABLE routes;
--first_query
INSERT OVERWRITE LOCAL DIRECTORY '/Users/vivekanandganapathynagarajan/Documents/Edureka/datasets/hive_output/air-output/airportsIndia' SELECT a.NAME FROM airports a where country = "India";
--second query
INSERT OVERWRITE LOCAL DIRECTORY '/Users/vivekanandganapathynagarajan/Documents/Edureka/datasets/hive_output/air-output/airlinesZeroStops'SELECT DISTINCT a.name FROM airlines a JOIN routes r ON (a.airlineid = r.airlineid) where r.stops = 0;
--third query
INSERT OVERWRITE LOCAL DIRECTORY '/Users/vivekanandganapathynagarajan/Documents/Edureka/datasets/hive_output/air-output/airlinesCodeShare'SELECT DISTINCT a.name, r.codeshare FROM airlines a JOIN routes r ON (a.airlineid = r.airlineid) where r.codeshare = "Y";
--fourth query
INSERT OVERWRITE LOCAL DIRECTORY '/Users/vivekanandganapathynagarajan/Documents/Edureka/datasets/hive_output/air-output/countryhighestAirports' SELECT country, count(country) as c FROM airports a group by country order by c desc limit 1;
--fifth query
INSERT OVERWRITE LOCAL DIRECTORY '/Users/vivekanandganapathynagarajan/Documents/Edureka/datasets/hive_output/air-output/activeAirlinesUS' SELECT a.NAME FROM airlines a where active = "Y" and country = "United States";

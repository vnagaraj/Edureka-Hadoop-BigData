README File:

Queries solved using JAVA MR

Problem Statement
A. Find list of Airports operating in the Country India

Solution: 
(map-reduce.zip)
airportsIndia/AirportsIndiaDriver.java
airportsIndia/AirportsCountryMapper.java

sample output
(air-output.zip)
air-output/airportsIndia

B. Find the list of Airlines having zero stops

Solution:
(map-reduce.zip)
airlinesZeroStops/AirLinesZeroStopsDriver.java
airlinesZeroStops/AirLinesNameMapper.java
airlinesZeroStops/AirLinesStopsMapper.java
airlinesZeroStops/AirLinesNamesStopsReducer.java

sample output
(air-output.zip)
air-output/airlinesZeroStops

C. List of Airlines operating with code share

Solution:
(map-reduce.zip)
airlinesCodeShare/AirLinesCodeShareDriver.java
airlinesZeroStops/AirLinesNameMapper.java
airlinesCodeShare/AirLinesCodeShareMapper.java
airlinesCodeShare/AirLinesNamesCodeShareReducer.java

sample output
(air-output.zip)
air-output/airlinesCodeShare

D. Which country (or) territory having highest Airports

Solution:
(map-reduce.zip)
countryHighestAirports/HighestCountryDriver.java
countryHighestAirports/CountryMapper.java
countryHighestAirports/CountryReducer.java

sample output
(air-output.zip)
air-output/countryhighestAirports

E. Find the list of Active Airlines in United states

Solution:
(map-reduce.zip)
activeairlinesUS/AirLinesActiveDriver.java
activeairlinesUS/AirLinesActiveMapper.java

sample output
(air-output.zip)
air-output/activeAirlinesUS

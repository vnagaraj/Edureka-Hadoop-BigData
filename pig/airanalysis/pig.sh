#!/bin/bash
# execute pig scripts
pig -x local activeairlinesUS.pig
pig -x local airlinesCodeShare.pig
pig -x local airlinesZeroStops.pig
pig -x local airportsIndia.pig
pig -x local countryHighestAirport.pig

from mrjob.job import MRJob
from mrjob.step import MRStep
#Komenda gdy chcemy nasz wynik (output) zapisać do pliku o nazwie preprocessed_data.csv
#python zad9.py flights.csv > preprocessed_data.csv

class MRPreprocess(MRJob):

    def mapper(self, _, line):
        (YEAR, MONTH, DAY, DAY_OF_WEEK, AIRLINE, FLIGHT_NUMBER, TAIL_NUMBER, ORIGIN_AIRPORT, DESTINATION_AIRPORT,
         SCHEDULED_DEPARTURE, DEPARTURE_TIME, DEPARTURE_DELAY, TAXI_OUT, WHEELS_OFF, SCHEDULED_TIME, ELAPSED_TIME,
         AIR_TIME,
         DISTANCE, WHEELS_ON, TAXI_IN, SCHEDULED_ARRIVAL, ARRIVAL_TIME, ARRIVAL_DELAY, DIVERTED, CANCELLED,
         CANCELLATION_REASON,
         AIR_SYSTEM_DELAY, SECURITY_DELAY, AIRLINE_DELAY, LATE_AIRCRAFT_DELAY, WEATHER_DELAY) = line.split(',')
        month, day, distance = int(MONTH), int(DAY), int(DISTANCE)
        yield 'row', (month, day, AIRLINE, distance)

if __name__ == '__main__':
    MRPreprocess.run()


from mrjob.job import MRJob
from mrjob.step import MRStep

class MRFlights(MRJob):

    def step(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer)
        ]
    def reducer(self, key, values):
        total_dep_del = 0
        total_arr_del = 0
        num = 0
        for value in values:
            total_dep_del += value[0]
            total_arr_del += value[1]
            num += 1
        yield key, (total_dep_del/num, total_arr_del/num)

    def mapper(self, _, line):
        (YEAR, MONTH, DAY, DAY_OF_WEEK, AIRLINE, FLIGHT_NUMBER, TAIL_NUMBER, ORIGIN_AIRPORT, DESTINATION_AIRPORT,
         SCHEDULED_DEPARTURE, DEPARTURE_TIME, DEPARTURE_DELAY, TAXI_OUT, WHEELS_OFF, SCHEDULED_TIME, ELAPSED_TIME,
         AIR_TIME,
         DISTANCE, WHEELS_ON, TAXI_IN, SCHEDULED_ARRIVAL, ARRIVAL_TIME, ARRIVAL_DELAY, DIVERTED, CANCELLED,
         CANCELLATION_REASON,
         AIR_SYSTEM_DELAY, SECURITY_DELAY, AIRLINE_DELAY, LATE_AIRCRAFT_DELAY, WEATHER_DELAY) = line.split(',')
        if DEPARTURE_DELAY == '':
            DEPARTURE_DELAY = 0
        if AIRLINE_DELAY == '':
            AIRLINE_DELAY = 0
        DEPARTURE_DELAY, AIRLINE_DELAY = float(DEPARTURE_DELAY), float(AIRLINE_DELAY)
        MONTH = int(MONTH)
        yield AIRLINE, (DEPARTURE_DELAY, AIRLINE_DELAY)

if __name__ == '__main__':
    MRFlights.run()
#Task 4
# (b) Which pickup location generates the most revenue? 

from mrjob.job import MRJob # importing mrjob library
from mrjob.step import MRStep # importing mrstep library

class MostRevenue(MRJob): # extending the MRJob class

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(reducer=self.final_reducer)
        ]
    
# mapper function
    def mapper(self, _, line):
        # Skip header line
        if not line.startswith('VendorID'):
            fields = line.split(',')
            pickup_location = fields[7]
            revenue = float(fields[16])
            yield pickup_location, revenue

# reducer function
    def reducer(self, pickup_location, revenues):
        yield None, (sum(revenues), pickup_location)

    def final_reducer(self, _, max_revenues):
        max_revenue, pickup_location = max(max_revenues)
        yield pickup_location, max_revenue


if __name__ == '__main__': # main function
    MostRevenue.run() # calling the run function
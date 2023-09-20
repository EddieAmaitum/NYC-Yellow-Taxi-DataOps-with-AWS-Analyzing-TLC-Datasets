# Task 4 
# (c) What are the different payment types used by customers and their count? The final results should be in a sorted format.

from mrjob.job import MRJob # importing mrjob library
from mrjob.step import MRStep # importing mrstep library

class Payment_Types_Count(MRJob): # extending the MRJob class

    def mapper(self, _, line): # mapper function
        # Skip header line
        if not line.startswith('VendorID'):
            fields = line.split(',')
            payment_type = fields[9]
            yield payment_type, 1

    def combiner(self, payment_type, counts): # combiner function
        yield payment_type, sum(counts)

# reducer function
    def reducer(self, payment_type, counts):
        yield payment_type, sum(counts)

    def reducer_sort_results(self, payment_type, counts):
        yield None, (sum(counts), payment_type)

    def reducer_output_result(self, _, sorted_results):
        for count, payment_type in sorted(sorted_results, reverse=True):
            yield payment_type, count

    def steps(self): # steps function
        return [
            MRStep(mapper=self.mapper, combiner=self.combiner, reducer=self.reducer),
            MRStep(reducer=self.reducer_sort_results),
            MRStep(reducer=self.reducer_output_result)
        ]

if __name__ == '__main__': # main function
    Payment_Types_Count.run() # calling the run function
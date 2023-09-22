
import re
from mrjob.job import MRJob
from mrjob.step import MRStep


class MRTopOrders(MRJob):
    """Find the top 3 orders with products from most number of aisles"""
    def configure_args(self):
        super(MRTopOrders, self).configure_args()
        self.add_passthru_arg(
            '--cloud-temp-dir', type=str, default='gs://dataproc-staging-us-central1-453457372769-hpmipcy6/', help='GCS temp directory')

    def mapper(self, _, row):
        order_id, aisles_id = row.split(',')
        yield order_id, aisles_id

    def reducer_count_aisles(self, order_id, aisles_ids):
        distinct_aisles = set(aisles_ids)
        yield None, (len(distinct_aisles), order_id)

    def reducer_sort(self, _, order_counts):
        top_orders = sorted(order_counts, reverse=True)[:3]
        for count, order_id in top_orders:
            yield order_id, count

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer_count_aisles),
            MRStep(reducer=self.reducer_sort)
        ]

class MRTopUsers(MRJob):
    """Find the top 3 users who have bought products from the most number of aisles in their first 10 items in the cart"""
    def mapper(self, _, line):
        _, user_id, add_to_cart_order, aisle_id = line.split(',')
        if int(''.join(re.findall(r'\d+', add_to_cart_order))) <= 10: # to deal with int conversion error due to data like this '"3"'
            yield user_id, aisle_id

    def reducer_count_aisles(self, user_id, aisles_ids):
        distinct_aisles = set(aisles_ids)
        yield None, (len(distinct_aisles), user_id)

    def reducer_sort(self, _, user_aisle_counts):
        top_users = sorted(user_aisle_counts, reverse=True)[:3]
        for count, user_id in top_users:
            yield user_id, count

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer_count_aisles),
            MRStep(reducer=self.reducer_sort)
        ]
    
if __name__ == '__main__':
    # MRTopOrders.run()
    MRTopUsers.run()


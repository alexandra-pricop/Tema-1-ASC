"""
This module represents the Producer.

Computer Systems Architecture Course
Assignment 1
March 2021
"""
import time
from threading import Thread


class Producer(Thread):
    """
    Class that represents a producer.
    """

    def __init__(self, products, marketplace, republish_wait_time, **kwargs):
        """
        Constructor.

        @type products: List()
        @param products: a list of products that the producer will produce

        @type marketplace: Marketplace
        @param marketplace: a reference to the marketplace

        @type republish_wait_time: Time
        @param republish_wait_time: the number of seconds that a producer must
        wait until the marketplace becomes available

        @type kwargs:
        @param kwargs: other arguments that are passed to the Thread's __init__()
        """
        Thread.__init__(self, **kwargs)
        self.kwargs = kwargs
        self.products = products
        self.marketplace = marketplace
        self.republish_wait_time = republish_wait_time

    def run(self):
        producer_id = self.marketplace.register_producer()
        while True:
            for product in self.products:
                quantity = product[1]
                cnt = 0
                prod_time = product[2]
                while cnt < quantity:
                    if self.marketplace.publish(producer_id, product[0]):
                        time.sleep(prod_time)
                        cnt = cnt + 1
                    else:
                        time.sleep(self.republish_wait_time)

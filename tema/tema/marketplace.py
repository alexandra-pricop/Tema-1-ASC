"""
This module represents the Marketplace.

Computer Systems Architecture Course
Assignment 1
March 2021
"""
from threading import Lock, currentThread


class Marketplace:
    """
    Class that represents the Marketplace. It's the central part of the implementation.
    The producers and consumers use its methods concurrently.
    """

    def __init__(self, queue_size_per_producer):
        """
        Constructor

        :type queue_size_per_producer: Int
        :param queue_size_per_producer: the maximum size of a queue associated with each producer
        """
        self.queue_size_per_producer = queue_size_per_producer
        self.producer_id = -1
        self.prod_lock = Lock()
        self.cart_id = -1
        self.cart_lock = Lock()
        self.cart_list = []
        self.product_list = []
        self.published_list = []
        self.published_lock = Lock()

    def register_producer(self):
        """
        Returns an id for the producer that calls this.
        """
        with self.prod_lock:
            self.producer_id = self.producer_id + 1
            self.published_list.append(0)
            return self.producer_id

    def publish(self, producer_id, product):
        """
        Adds the product provided by the producer to the marketplace

        :type producer_id: String
        :param producer_id: producer id

        :type product: Product
        :param product: the Product that will be published in the Marketplace

        :returns True or False. If the caller receives False, it should wait and then try again.
        """

        if self.published_list[producer_id] >= self.queue_size_per_producer:
            return False

        self.product_list.append((product, producer_id))
        self.published_list[producer_id] += 1

        return True

    def new_cart(self):
        """
        Creates a new cart for the consumer

        :returns an int representing the cart_id
        """
        with self.cart_lock:
            self.cart_id = self.cart_id + 1
            self.cart_list.append([])
            return self.cart_id

    def add_to_cart(self, cart_id, product):
        """
        Adds a product to the given cart. The method returns

        :type cart_id: Int
        :param cart_id: id cart

        :type product: Product
        :param product: the product to add to cart

        :returns True or False. If the caller receives False, it should wait and then try again
        """

        for (a, b) in self.product_list:
            with self.published_lock:
                if product == a:

                    self.cart_list[cart_id].append((a, b))
                    self.product_list.remove((a, b))
                    self.published_list[b] -= 1

                    return True

        return False

    def remove_from_cart(self, cart_id, product):
        """
        Removes a product from cart.

        :type cart_id: Int
        :param cart_id: id cart

        :type product: Product
        :param product: the product to remove from cart
        """

        for (a, b) in self.cart_list[cart_id]:
            if product == a:
                self.cart_list[cart_id].remove((a, b))
                self.product_list.append((a, b))
                with self.published_lock:
                    self.published_list[b] += 1
                break

    def place_order(self, cart_id):
        """
        Return a list with all the products in the cart.

        :type cart_id: Int
        :param cart_id: id cart
        """
        products = []
        for (product, b) in self.cart_list[cart_id]:
            with self.cart_lock:
                print("{} bought {}".format(currentThread().getName(), product))
                products.append(product)

        return products
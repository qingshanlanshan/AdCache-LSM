import numpy as np
import random
class Zipf:
    def __init__(self, data: list, skewness=0.99):
        self.data = data
        random.shuffle(self.data)
        self.skewness = skewness
        self.data_size = len(data)
        self.data_prob = np.array([1.0 / (i ** self.skewness) for i in range(1, self.data_size + 1)])
        s = sum(self.data_prob)
        self.data_prob /= s
        
    def sample(self, n):
        n = int(n)
        return random.choices(self.data, weights=self.data_prob, k=n)
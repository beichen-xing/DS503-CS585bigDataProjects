import pandas as pd
import random

x = []
y = []

for i in range(12000000):
    x.append(random.randint(1, 10000))
    y.append(random.randint(1, 10000))

points = pd.DataFrame({'X': x, 'Y': y})

points.to_csv("Points.csv", header=None, index=None)
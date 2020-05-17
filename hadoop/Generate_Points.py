import random
import pandas as pd


x = []
y = []

for i in range(20000000):
    x.append(random.randint(1, 10000))
    y.append(random.randint(1, 10000))

records = pd.DataFrame({
    'x': x,
    'y': y
})
records.to_csv('Points.csv', index=None)

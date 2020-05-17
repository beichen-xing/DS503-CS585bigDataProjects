import random
import pandas as pd


x = []
y = []

for i in range(1000):
    x.append(random.randint(1, 10000))
    y.append(random.randint(1, 10000))

records = pd.DataFrame({
    'index': range(1, 1001),
    'x': x,
    'y': y
})
records.to_csv('initial.csv', index=None)
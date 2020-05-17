import random
import pandas as pd


names = []
xs = []
ys = []
widths = []
heights = []

for i in range(10000):
    x = random.randint(1, 9999)
    y = random.randint(1, 9999)
    width = random.randint(1, 5)
    height = random.randint(1, 20)
    while( x + width > 10000):
        width = random.randint(1, 5)
    while( y + height > 10000):
        height = random.randint(1, 20)
    names.append('r'+str(i+1))
    xs.append(x)
    ys.append(y)
    widths.append(width)
    heights.append(height)

records = pd.DataFrame({
    'name': names,
    'x': xs,
    'y': ys,
    'width': widths,
    'height': heights
})
records.to_csv('Rec.csv', index=None)

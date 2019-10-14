import numpy as np
import pandas

df = pandas.DataFrame({"a": np.random.random(100),
                       "b": np.random.random(100),
                       "id": np.arange(100)})

# Bin the data frame by "a" with 10 bins...
bins = np.linspace(df.a.min(), df.a.max(), 10)
groups = df.groupby(np.digitize(df.a, bins))

# Get the mean of each bin:
print(groups.mean()) # Also could do "groups.aggregate(np.mean)"

# Similarly, the median:
print(groups.median())

# Apply some arbitrary function to aggregate binned data
print(groups.aggregate(lambda x: np.mean(x[x > 0.5])))
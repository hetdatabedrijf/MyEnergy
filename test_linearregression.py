# import numpy as np
from sklearn.linear_model import LinearRegression

# Definieer de gegevens
x = np.array([[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
              [2, 4, 6, 8, 10, 12, 14, 16, 18, 20],
              [3, 6, 9, 12, 15, 18, 21, 24, 27, 30]])
y = np.array([2, 4, 6, 8, 10, 12, 14, 16, 18, 20])

# Voer de regressieanalyse uit
model = LinearRegression()
model.fit(x, y)

# Print de resultaten
print(model.coef_)
print(model.intercept_)
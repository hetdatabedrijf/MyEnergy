
from mpl_toolkits.mplot3d import Axes3D
import pandas as pd
import matplotlib.pyplot as plt
# import numpy as np
from sklearn import linear_model
np.random.seed(19680801)

# Input data files are available in the read-only "../input/" directory
# For example, running this (by clicking run or pressing Shift+Enter) will list all files under the input directory

# import os
# for root, dirs, files in os.walk("/Users/paulvanbrabant/Downloads/python", topdown=False):
#    for name in files:
#       print(os.path.join(root, name))
#    for name in dirs:
#       print(os.path.join(root, name))

# df = pandas.read_csv("/Users/paulvanbrabant/Downloads/python/slaaphistoriek.csv"),
# print(df),

y = [55, 75, 80, 70, 52, 77, 72, 90, 56, 60, 84, 91, 60]
x = [0,1,1,1,0,0,0,1,0,0,1,1,0]
z = [1,1,1,0,0,1,0,1,0,0,1,1,0]


data = pd.DataFrame({"slaap_kwal": [55,75,80,70,52,77,72,90,56,60,84,91,60],
                   "beweeg": [0,1,1,1,0,0,0,1,0,0,1,1,0],
                   "slaaptweedrie": [0,1,1,1,0,1,0,1,0,0,1,1,0],
                   "draagbril": [1,0,0,0,0,1,0,1,1,1,0,0,0]
                   })

# data=pd.read_csv("/Users/paulvanbrabant/Downloads/python/slaaphistoriek.csv")

print(data.head(10)),
print(data[["slaaptweedrie"]])

# data = data[["slaap_kwal","slaap_2230","beweeg"]],
# print(data[['slaap_kwal']])

fig=plt.figure()
ax=fig.add_subplot(111,projection='3d')
n=100
ax.scatter(data["slaap_kwal"],data["beweeg"] ,data["draagbril"],color="red")
ax.set_xlabel("slaap_kwal")
ax.set_ylabel("beweeg")
ax.set_zlabel("draagbril")
plt.show()

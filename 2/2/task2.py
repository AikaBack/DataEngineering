import numpy as np
import os
matrix = np.load("matrix_52_2.npy")

size = len(matrix)

x = list()
y = list()
z = list()

limit = 552

for i in range(0, size):
    for j in range(0, size):
        if matrix[i][j] > limit:
            x.append(i)
            y.append(j)
            z.append(matrix[i][j])

np.savez("points", x=x, y=y, z=z)
np.savez_compressed("points_rar", x=x, y=y, z=z)


points_size = os.path.getsize('points.npz') / (1024 * 1024)  # размер points.npz в МБ
points_rar_size = os.path.getsize('points_rar.npz') / (1024 * 1024)  # размер points_rar.npz в МБ

print(f"points.npz Size: {points_size:.2f} MB")
print(f"points_rar.npz Size: {points_rar_size:.2f} MB")
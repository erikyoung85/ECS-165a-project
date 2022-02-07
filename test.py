long = 92106474
short = 5
bytes_long = long.to_bytes(4, 'big')
bytes_short = short.to_bytes(4, 'big')

array = bytearray(16)
array[0:4] = bytes_long
array[4:8] = bytes_short


print(bytes_long)

print(bytes_short)

print(array)
print(array[0:4])
print(int.from_bytes(array[0:4], 'big'))
print(array[4:8])
print(int.from_bytes(array[4:8], 'big'))
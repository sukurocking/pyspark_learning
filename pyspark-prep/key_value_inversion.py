input =  {'A': 67, 'B': 23, 'C': 45, 'D': 56, 'E': 12, 'F': 69, 'G': 67, 'H': 23}
# Output: {67: ['A', 'G'], 69: ['F'], 23: ['B', 'H'], 56: ['D'], 12: ['E'], 45: ['C']}

input.values()
input.keys()

new_keys = set(list(input.values()))
# for key in new_keys:
#     print(key)
# Initializing the new dictionary
new_dict = {new_key: [] for new_key in new_keys}
# print(new_dict)

for old_key, old_value in input.items():
    new_dict[old_value].append(old_key)
    # print(old_key, old_value)
print(new_dict)
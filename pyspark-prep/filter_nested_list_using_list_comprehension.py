# Filtering a Nested List Using List Comprehension - 
# Extracting odd numbers from Python list within list and appending them to the list

nested_list = [
    [1, 3, 5, 7, 9, 2, 4, 6, 8],
    [11, 12, 13]
]

# Without using list comprehension
# odd_elements = []
# for a_list in nested_list:
#     odd_elements = odd_elements + [elem for elem in a_list if elem%2 != 0]
#
# print(odd_elements)

# Using list comprehension
odd_numbers = [num for sublist in nested_list for num in sublist if num % 2 != 0]
print(odd_numbers)

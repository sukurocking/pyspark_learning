# Write a list comprehension which can take a table list as input and return the square of each element in the list.

"""This code takes in first 10 numbers and outputs to squares of the even values
"""
first_10 = list(range(1,11))
squares = [i ** 2 for i in first_10 if i % 2 == 0]
print(squares)
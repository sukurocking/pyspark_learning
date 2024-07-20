# Program to get the common elements from 2 lists
list1 = [1,3,4]
list2 = [3,4,5]
final_set = set(list1) & set(list2) # The & operator stands for intersection
print(final_set)
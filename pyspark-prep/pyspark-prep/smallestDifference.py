def smallestDifference(arrayOne, arrayTwo):
    smallest_pair_tup = [arrayOne[0], arrayTwo[0]], abs(arrayOne[0] - arrayTwo[0])
    for val1 in arrayOne:
        for val2 in arrayTwo:
            cur_abs_val = abs(val1 - val2)
            if cur_abs_val < smallest_pair_tup[1]:
                smallest_pair_tup = [val1, val2], cur_abs_val
    
    return smallest_pair_tup[0]


arrayOne = [-1, 5, 10, 20, 28, 3]
arrayTwo = [26, 134, 135, 15, 17]

print(smallestDifference(arrayOne, arrayTwo))




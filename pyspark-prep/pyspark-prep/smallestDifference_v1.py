def smallestDifference(arrayOne : list, arrayTwo: list) -> list:
    arrayOne.sort()
    arrayTwo.sort()
    i = 0
    j = 0
    smallest_pair_tup = [arrayOne[i], arrayTwo[j]], abs(arrayOne[i] - arrayTwo[j])
    while (i < len(arrayOne) and j < len(arrayTwo)):
        val1 = arrayOne[i]
        val2 = arrayTwo[j]
        current_abs_val = abs(val1 - val2)
        if val1 == val2:
            smallest_pair_tup = [val1, val2], current_abs_val
            return smallest_pair_tup[0]
        elif val1 < val2:
            if current_abs_val < smallest_pair_tup[1]:
                smallest_pair_tup = [val1, val2], current_abs_val
            i += 1
            continue
        elif val1 > val2:
            if current_abs_val < smallest_pair_tup[1]:
                smallest_pair_tup = [val1, val2], current_abs_val
            j += 1
            continue
    return smallest_pair_tup[0]

arrayOne = [-1, 5, 10, 20, 28, 3]
arrayTwo = [26, 134, 135, 15, 17]

print(smallestDifference(arrayOne, arrayTwo))


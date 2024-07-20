def isValidSubsequence(array, sequence):
    # Write your code here.
    for num in sequence:
        if num not in array:
            return False
        else:
            prev_index = array.index(num)
            new_index = prev_index + 1
            if new_index <= len(array):
                array = array[new_index:]
                continue
    return True
    
array = [5, 1, 22, 25, 6, -1, 8, 10]
sequence = [1, 6, -1, 10]
print(isValidSubsequence(array,sequence))

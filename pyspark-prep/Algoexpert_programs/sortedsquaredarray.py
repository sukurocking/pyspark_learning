def sortedSquaredArray(array):
    # Write your code here.
    N = len(array)
    output_array = [0] * N
    start = 0
    end = N - 1
    i = N - 1

    while start <= end:
        if abs(array[start]) < abs(array[end]):
            output_array[i] = array[end] ** 2
            end -= 1
        else:
            output_array[i] = array[start] ** 2
            start += 1
        i -= 1
    return output_array

print(sortedSquaredArray([-3,-2,1,4]))
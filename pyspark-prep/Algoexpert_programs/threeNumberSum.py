def threeNumberSum(array, targetSum):
    array.sort()
    output_arr = []
    N = len(array)
    for i in range(N):
        for j in range(N-1,i,-1):
            val1 = array[i]
            val3 = array[j]
            val2 = targetSum - (val1 + val3)
            if val2 in array[i+1:j]:
                output_arr.append([val1, val2, val3])
    
    return output_arr

array = [12, 3, 1, 2, -6, 5, -8, 6]
targetSum = 0

print(threeNumberSum(array, targetSum))
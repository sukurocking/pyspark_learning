def twoNumberSum(array, targetSum):
    seen = set()
    for num in array:
        target_num = targetSum - num
        if target_num in seen:
            return [num, target_num]
        else:
            seen.add(num)
    return []

array = [4, 6, 1, -3]
targetSum = 3

print(twoNumberSum(array, targetSum))
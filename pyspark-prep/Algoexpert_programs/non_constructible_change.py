def nonConstructibleChange(coins: list(int)) -> int:
    coins.sort()
    min_change = 1
    max_change = sum(coins)
    all_possible = list(range(max_change + 1))
    
    if len(coins) == 0:
        return 1
    elif coins[0] > 1:
        return 1
    
    
        
    
    return 1


coins = [5, 7, 1, 1, 2, 3, 22]
# coins.sort() --> [1,1,2,3,5,7,22]

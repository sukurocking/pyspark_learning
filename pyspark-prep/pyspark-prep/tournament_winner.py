def tournamentWinner(competitions, results):
    # Write your code here.
    N = len(competitions)
    results_dict = {}
    for i in range(N):
        results_dict.setdefault(competitions[i][0],0)
        results_dict.setdefault(competitions[i][1],0)
        if results[i]:
            results_dict[competitions[i][0]] += 3
        else:
            results_dict[competitions[i][1]] += 3
    return max(results_dict, key=results_dict.get)

competitions = [
  ["HTML", "C#"],
  ["C#", "Python"],
  ["Python", "HTML"]
]
results = [0, 0, 1]
print (tournamentWinner(competitions, results))

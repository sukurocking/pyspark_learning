# Write a Python function that takes a sentence as input and returns a new sentence with the first and last words swapped.

def swap(input:str) -> str:
    split_words = input.split(" ")
    new_split_words = [split_words[-1]] + split_words[1:-1] + [split_words[0]]
    output = " ".join(new_split_words)
    print(output)
    return(output)

input = "This is test sentence"
swap(input)
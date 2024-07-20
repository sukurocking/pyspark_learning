# Write a function in Python that will take two arguments 
# and replace spaces of the first argument value with the second argument.

def replace_spaces(arg1:str, arg2:str) -> str:
    return arg1.replace(" ", arg2)

print(replace_spaces("abc x y", "_"))
# Open a text file named "example.txt," read its contents, and print them.

with open("example.txt") as file:
    file_contents = file.read()
    print(file_contents)

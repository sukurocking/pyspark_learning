# Write a program that asks the user for input until a 
# valid integer is entered, handling exceptions appropriately.

while(True):
    try:
        x = int(input("Please input a number: "))
    except ValueError:
        print("Not a number")
    else:
        print("Thank you for inputting a number")
        break


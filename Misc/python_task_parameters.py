import sys

if len(sys.argv) != 3:
    print("Usage: my_script.py arg1 arg2")
    sys.exit(1)

arg1 = sys.argv[1]
arg2 = sys.argv[2]

print("Arg1: ", arg1)
print("Arg2: ", arg2)
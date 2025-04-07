"""
Test script which reads in test-input.txt
and prints the answer in test-output.txt
"""

with open("data/test-input.txt", 'r') as f:
    lines = f.readlines()

with open("output/test-output.txt", 'w') as f:
    f.write(''.join(lines))

term = "memtotal"
file = open( "/home/hduser/file.txt")
for line in file:
    line.strip().split('/n')
    if term in line:
        print (line)
file.close()
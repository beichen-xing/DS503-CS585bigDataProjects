import names
import random
import json
import os
import string

filename = "Customers.txt"
n_lines=50000
#n_lines=10
with open('countries.json') as json_file:
    countries = json.load(json_file)
lines=[]
for i in range(n_lines):
    line=""
    line+=str(i+1) + "," #ID
    line+=names.get_full_name() + "," #Name
    line+=str(random.randint(18, 100)) + ","  #Age
    country=random.choice(countries)
    line+=country["cca3"] + "," #CountryCode
    line+=str("{:.2f}".format(random.uniform(100, 10000000))) + "\n" #Salary
    lines.append(line) #Form all the lines

with open(filename, "w+") as f:
    data = f.read()
    f.seek(0) #get to begining of file
    f.truncate() #delete everything
    f.writelines(lines) #write the lines
    f.seek(0, os.SEEK_END)              
    f.seek(f.tell() - 2, os.SEEK_SET)
    f.truncate() #delete last line


##############
filename = "Purchases.txt"
n_lines=5000000
#n_lines=10
lines=[]
for i in range(n_lines):
    line=""
    line+=str(i+1) + "," #TransID
    line+=str(random.randint(1, 50000)) + ","  #CustID
    line+=str("{:.2f}".format(random.uniform(10, 2000))) + "," #TransTotal    
    line+=str(random.randint(1, 15)) + "," #TransNumItems
    rand_str= ''.join(random.choice(string.ascii_letters) for x in range(random.randint(20, 50)))
    line+=str(rand_str) + "\n" #TransDesc
    lines.append(line) #Form all the lines
    


with open(filename, "w+") as f:
    data = f.read()
    f.seek(0) #get to begining of file
    f.truncate() #delete everything
    f.writelines(lines) #write the lines
    f.seek(0, os.SEEK_END)              
    f.seek(f.tell() - 2, os.SEEK_SET)
    f.truncate() #delete last line
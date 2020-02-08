import numpy as np
import pycountry
from faker import Faker
import time
from random import randint
import random
import os

os.chdir("D:\Courses\WPI-ds\DS503\Project1")

'''
def string_generator(size=10, chars=string.ascii_lowercase):
    return ''.join(random.choice(chars) for _ in range(size))
'''

# User Names
fake = Faker()

# Country Names
country_names = np.array([])
for i in range(249):
    cn = list(pycountry.countries)[i].name
    if 10 <= len(cn) <= 20 and ',' not in cn:
        country_names = np.append(country_names, cn)

country_names[1] = 'Aland Islands'
country_names[13] = "Cote d'Ivoire"

# Hobby
hobby1 = np.loadtxt('hobbies.txt', delimiter='\n', dtype=np.str)
hobby = np.array([])
for i in hobby1:
    if 10 <= len(i) <= 20:
        hobby = np.append(hobby, i)

# TypeOfAccess
toa = ["Simply Viewed", "Left a Note", "Simply Viewed", "Simply Viewed", "Left a Note", "Added a friendship"
    , "Simply Viewed", "Left a Note", "Simply Viewed", "Simply Viewed", "Simply Viewed", "Left a Note"
    , "Simply Viewed", "Left a Note", "Simply Viewed", "Simply Viewed", "Simply Viewed", "Left a Note"
    , "Simply Viewed", "Left a Note", "Simply Viewed", "Simply Viewed", "Left a Note", "Simply Viewed", "Simply Viewed",
       "Simply Viewed"
    , "Simply Viewed", "Simply Viewed", "Simply Viewed", "Simply Viewed", "Simply Viewed", "Simply Viewed"
    , "Simply Viewed", "Simply Viewed", "Simply Viewed", "Simply Viewed", "Simply Viewed", "Simply Viewed"]

''' MyPage: ID, Name, Nationality, CountryCode, Hobby'''

with open('MyPage', 'a') as file:
    for i in range(1, 200001):
        values = [str(i), fake.name(), random.choice(country_names), randint(1, 50), random.choice(hobby)]
        tmp = ''
        for j in range(len(values) - 1):
            tmp += str(values[j]) + ','
        tmp += str(values[4])
        tmp += '\n'
        if i % 40000 == 0:
            print(i / 200000)
        file.write(tmp)

'''Accesslog: accessID, ByWho, WhatPage, TypeOfAccess, AccessTime'''


a1 = np.random.randint(1, 200001, size=(10000000, 1))
a2 = np.random.randint(1, 200001, size=(10000000, 1))
a3 = random.choices(toa, k=10000000)
a4 = np.random.randint(1, 1000001, size=(10000000, 1))


start = time.time()
with open('AccessLog', 'a') as file:
    for i in range(10000000):
        values = [i+1, a1[i][0], a2[i][0], a3[i], a4[i][0]]
        tmp = ''
        for j in range(len(values) - 1):
            tmp += str(values[j]) + ','
        tmp += str(values[4])
        tmp += '\n'
        if i % 100000 == 0:
            print(i / 10000000)
            end = time.time()
            print(end - start)
        file.write(tmp)

'''AllFriends: FriendRel, PersonID, MyFriend, DateofFriendship, Desc'''

Desc = ['collegefriend', 'girlfriend', 'family', 'brother', 'sister', 'cousin', 'boyfriend', 'wife', 'husband', 'colleague',
        'highschoolfriend', 'partner', 'classmate', 'roommate', 'colleague', 'collegefriend', 'highschoolfriend', 'colleague', 'collegefriend'
        , 'colleague', 'collegefriend', 'colleague', 'collegefriend','colleague', 'collegefriend','colleague', 'collegefriend','colleague', 'collegefriend'
        ,'colleague', 'collegefriend','colleague', 'collegefriend','colleague', 'collegefriend', 'family']

a1 = np.random.randint(1, 2000001, size=(20000000, 1))
a2 = np.random.randint(1, 2000001, size=(20000000, 1))
a3 = np.random.randint(1, 1000001, size=(20000000, 1))
a4 = random.choices(Desc, k=20000000)


start = time.time()
with open('AllFriends', 'a') as file:
    for i in range(20000000):
        values = [i+1, a1[i][0], a2[i][0], a3[i][0], a4[i]]
        tmp = ''
        for j in range(len(values) - 1):
            tmp += str(values[j]) + ','
        tmp += str(values[4])
        tmp += '\n'
        if i % 200000 == 0:
            print(i / 20000000)
            end = time.time()
            print(end - start)
        file.write(tmp)
# 1. Digit Sum Calculator
n = input("enter a number: ")
s = 0
for d in n:
    if d.isdigit():
        s = s + int(d)
print("sum of digits is", s)

# 2. Reverse a 3-digit Number
n = input("enter a 3-digit number: ")
if len(n) == 3 and n.isdigit():
    print("reversed number is", n[::-1])
else:
    print("invalid input")

# 3. Unit Converter
m = float(input("distance in meters: "))
print("in cm:", m * 100)
print("in feet:", m * 3.28084)
print("in inches:", m * 39.3701)

# 4. Percentage Calculator
s1 = float(input("marks in subj 1: "))
s2 = float(input("marks in subj 2: "))
s3 = float(input("marks in subj 3: "))
s4 = float(input("marks in subj 4: "))
s5 = float(input("marks in subj 5: "))
total = s1 + s2 + s3 + s4 + s5
avg = total / 5
perc = (total / 500) * 100
print("total:", total)
print("avg:", avg)
print("percentage:", perc)
if perc >= 90:
    print("grade: A")
elif perc >= 75:
    print("grade: B")
elif perc >= 50:
    print("grade: C")
else:
    print("grade: D")

# 5. Leap Year Checker
yr = int(input("enter a year: "))
if yr % 4 == 0:
    if yr % 100 != 0 or yr % 400 == 0:
        print("leap year")
    else:
        print("not a leap year")
else:
    print("not a leap year")

# 6. Simple Calculator
x = float(input("first number: "))
y = float(input("second number: "))
op = input("operator (+, -, *, /): ")
if op == "+":
    print("result:", x + y)
elif op == "-":
    print("result:", x - y)
elif op == "*":
    print("result:", x * y)
elif op == "/":
    if y != 0:
        print("result:", x / y)
    else:
        print("can't divide by zero")
else:
    print("invalid operator")

# 7. Triangle Validator
a = float(input("side a: "))
b = float(input("side b: "))
c = float(input("side c: "))
if a + b > c and a + c > b and b + c > a:
    print("yes, valid triangle")
else:
    print("nope, not a triangle")

# 8. Bill Splitter with Tip
bill = float(input("bill amount: "))
people = int(input("number of people: "))
tip = float(input("tip %: "))
final = bill + (bill * tip / 100)
split = final / people
print("each person pays", round(split, 2))

# 9. Prime Numbers from 1 to 100
print("primes between 1 and 100:")
n = 2
while n <= 100:
    i = 2
    prime = True
    while i < n:
        if n % i == 0:
            prime = False
            break
        i = i + 1
    if prime:
        print(n, end=" ")
    n = n + 1
print()

# 10. Palindrome Checker
text = input("type something: ")
rev = ""
for ch in text:
    rev = ch + rev
if text == rev:
    print("yup, it's a palindrome")
else:
    print("nope, not a palindrome")

# 11. Fibonacci Series
count = int(input("how many fibonacci numbers? "))
a = 0
b = 1
i = 0
while i < count:
    print(a, end=" ")
    temp = a + b
    a = b
    b = temp
    i = i + 1
print()

# 12. Multiplication Table
num = int(input("enter a number: "))
i = 1
while i <= 10:
    print(num, "x", i, "=", num * i)
    i = i + 1

# 13. Number Guessing Game
import random
target = random.randint(1, 100)
guess = -1
while guess != target:
    guess = int(input("guess the number (1-100): "))
    if guess < target:
        print("too low")
    elif guess > target:
        print("too high")
    else:
        print("you got it!")

# 14. ATM Machine
bal = 10000
while True:
    print("\n1. deposit")
    print("2. withdraw")
    print("3. check balance")
    print("4. exit")
    ch = input("what do you want to do? ")
    if ch == "1":
        amt = float(input("amount to deposit: "))
        bal = bal + amt
    elif ch == "2":
        amt = float(input("amount to withdraw: "))
        if amt <= bal:
            bal = bal - amt
        else:
            print("not enough money")
    elif ch == "3":
        print("balance:", bal)
    elif ch == "4":
        print("bye!")
        break
    else:
        print("invalid option")

# 15. Password Strength Checker (Basic)
pwd = input("enter password: ")
has_cap = False
has_num = False
has_sym = False
if len(pwd) >= 8:
    for c in pwd:
        if c >= 'A' and c <= 'Z':
            has_cap = True
        elif c >= '0' and c <= '9':
            has_num = True
        elif not c.isalpha() and not c.isdigit():
            has_sym = True
if has_cap and has_num and has_sym:
    print("nice password")
else:
    print("weak password, bro")

# 16. GCD (Greatest Common Divisor)
a = int(input("first number: "))
b = int(input("second number: "))
while b != 0:
    temp = b
    b = a % b
    a = temp
print("gcd is", a)

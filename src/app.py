import sqlparse
import sql2ra

sql = 'select distinct name from person, eats where person.name = eats.name and gender=\'f\''
sql2 = 'select distinct A.name, B.name from Eats A, Eats B where A.pizza = B.pizza'

stmt = sqlparse.parse(sql)[0]
stmt2 = sqlparse.parse(sql2)[0]

ra = sql2ra.translate(stmt)
ra2 = sql2ra.translate(stmt2)

identifiers = stmt2[8]

print(sql)
print(stmt)
print(type(stmt))
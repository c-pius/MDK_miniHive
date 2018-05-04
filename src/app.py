import sqlparse
import sql2ra
import test_sql2ra


import radb.parse
import raopt
# The data dictionary describes the relational schema.
dd = {}
dd["Person"] = {"name": "string", "age": "integer", "gender": "string"}
dd["Eats"] = {"name": "string", "pizza": "string"}
stmt = "\project_{Person.name, Eats.pizza}\select_{Person.name = Eats.name}(Person \cross Eats);"
ra = radb.parse.one_statement_from_string(stmt)



ra1 = raopt.rule_break_up_selections(ra)
print(ra)
print(type(ra))
#ra2 = raopt.rule_push_down_selections(ra1, dd)
#ra3 = raopt.rule_merge_selections(ra2)
#ra4 = raopt.rule_introduce_joins(ra3)
#print(ra4)
#\project_{Person.name, Eats.pizza} (Person \join_{Person.name = Eats.name} Eats)


# sql = 'select distinct name from person, eats where person.name = eats.name and gender=\'f\''
# sql2 = 'select distinct A.name, B.name from Eats A, Eats B where A.pizza = B.pizza'
# sql3 = 'select distinct name from Eats'
# sql4 = 'select distinct * from Eats'

# stmt = sqlparse.parse(sql)[0]
# stmt2 = sqlparse.parse(sql2)[0]
# stmt3 = sqlparse.parse(sql3)[0]
# stmt4 = sqlparse.parse(sql4)[0]

# ra = sql2ra.translate(stmt)
# ra2 = sql2ra.translate(stmt2)
# ra3 = sql2ra.translate(stmt3)
# ra4 = sql2ra.translate(stmt4)

# print(ra)
# print(ra2)
# print(ra3)
# print(ra4)
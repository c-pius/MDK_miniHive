import luigi
import radb
import ra2mr
import sqlparse
import raopt
import sql2ra

# Take a relational algebra query...

#raquery = radb.parse.one_statement_from_string("\select_{gender = 'female' and pizza = 'mushroom'} (Person \join_{Person.name = Eats.name} Eats);")

dd = {}
dd["Person"] = {"name": "string", "age": "integer", "gender": "string"}
dd["Eats"] = {"name": "string", "pizza": "string"}
dd["Serves"] = {"pizzeria": "string", "pizza": "string", "price": "integer"}

# sqlstring = "select distinct * from Person, Eats, Serves " \
#                     "where Person.name = Eats.name and Eats.pizza = Serves.pizza "\
#                     "and Person.age = 16 and Serves.pizzeria = 'Little Ceasars'"

# sqlstring = "select distinct e.pizza from Person p, Eats e where e.name = p.name and p.age > 20 and p.gender = 'female'"

# sqlstring = """
#             select distinct p.name
#             from person p, eats e, serves s
#             where p.name = e.name and e.pizza = s.pizza and s.pizzeria = 'Straw Hat' and p.gender = 'female'
#             """

# sqlstring = """
#             select distinct s.pizzeria
#             from person p, eats e, serves s
#             where p.name = e.name and e.pizza = s.pizza and s.price < 10 and p.name = 'Amy'
#             """

sqlstring = """
                select distinct *
                from serves
                where pizzeria = 'Chicago Pizza' and pizza = 'cheese' and price = 7.75
            """

stmt = sqlparse.parse(sqlstring)[0]
ra0 = sql2ra.translate(stmt)

ra1 = raopt.rule_break_up_selections(ra0)
ra2 = raopt.rule_push_down_selections(ra1, dd)

ra3 = raopt.rule_merge_selections(ra2)
ra4 = raopt.rule_introduce_joins(ra3)

print("ra4: " + str(ra4))

# ... translate it into a luigi task encoding a MapReduce workflow...
task = ra2mr.task_factory(ra4, env=ra2mr.ExecEnv.LOCAL)

# ... and run the task on Hadoop, using HDFS for input and output:
# (for now, we are happy working with luigis local scheduler).
luigi.build([task], local_scheduler=True)






# import sqlparse
# import sql2ra
# import test_sql2ra
# import radb.parse
# import raopt

# sql = "select distinct X.name, foo, bar from Person X where X.name = 'bla' and foo = 'bar' and bar = 'foo'"
# stmt = sqlparse.parse(sql)[0]

# ra = sql2ra.translate(stmt)
# print(ra)


# # The data dictionary describes the relational schema.
# dd = {}
# dd["Person"] = {"name": "string", "age": "integer", "gender": "string"}
# dd["Eats"] = {"name": "string", "pizza": "string"}

# ra = radb.parse.one_statement_from_string("\select_{gender = 'm'} (Person \cross Eats);")
# new = raopt.rule_push_down_selections(ra, dd)
# print(new)

# ra1 = radb.parse.one_statement_from_string("\select_{Person.gender = 'f' and Person.age = 16}(Person);")
# ra2 = radb.parse.one_statement_from_string("\project_{name}(\select_{gender='f' and age=16}(Person));")
# ra3 = radb.parse.one_statement_from_string("\select_{Person.gender='f' and Person.age=16}(Person) \cross Eats;")
# ra4 = radb.parse.one_statement_from_string("\select_{E.pizza = 'mushroom' and E.price < 10} \\rename_{E: *}(Eats);")

# new1 = raopt.rule_break_up_selections(ra1)
# new2 = raopt.rule_break_up_selections(ra2)
# new3 = raopt.rule_break_up_selections(ra3)
# new4 = raopt.rule_break_up_selections(ra4)

# print(new1)
# print(new2)
# print(new3)
# print(new4)

#stmt = "\project_{Person.name, Eats.pizza}\select_{Person.name = Eats.name}(Person \cross Eats);"
#stmt = "\project_{Person.name, Eats.pizza}(\select_{Person.gender = 'f' and Person.age = 16}(Person \cross Eats));"
#ra = radb.parse.one_statement_from_string(stmt)

#ra1 = raopt.rule_break_up_selections(ra)
#print(ra1)
#print(type(ra1))
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
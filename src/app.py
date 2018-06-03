# import sqlparse
# import sql2ra
# import test_sql2ra
# import radb.parse
# import raopt


import luigi
import radb
import ra2mr
import ra2mr_initial


# Take a relational algebra query...
# raquery = radb.parse.one_statement_from_string("\project_{P.name} (\select_{P.gender='female'} \\rename_{P:*} (Person));")
raquery = radb.parse.one_statement_from_string("(Person \join_{Person.name = Eats.name} Eats) " \
                      "\join_{Eats.pizza = Serves.pizza} (\select_{pizzeria='Dominos'} Serves);")


# ... translate it into a luigi task encoding a MapReduce workflow...
task = ra2mr.task_factory(raquery, env=ra2mr.ExecEnv.HDFS)

# ... and run the task on Hadoop, using HDFS for input and output:
# (for now, we are happy working with luigis local scheduler).
luigi.build([task], local_scheduler=True)

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
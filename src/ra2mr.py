
from enum import Enum
import json
import luigi
import luigi.contrib.hadoop
import luigi.contrib.hdfs
from luigi.mock import MockTarget
import radb
import radb.ast
import radb.parse
from radb.parse import RAParser as sym

# gets a list of conditions that were joined using AND
def get_single_conditions(expression):
    single_expressions = []
    if expression.op == sym.AND:        
        single_expressions.extend(get_single_conditions(expression.inputs[1]))
        single_expressions.extend(get_single_conditions(expression.inputs[0]))
    else:
        single_expressions.append(expression)

    return single_expressions

# gets a tuple containing the relation name and the attribute name
def get_single_relation_expression_attribute(expression: radb.ast.FuncValExpr):
    return next(((attribute.rel, attribute.name) for attribute in expression.inputs if type(attribute) == radb.ast.AttrRef), None)


'''
Control where the input data comes from, and where output data should go.
'''
class ExecEnv(Enum):
    LOCAL = 1   # read/write local files
    HDFS = 2    # read/write HDFS
    MOCK = 3    # read/write mock data to an in-memory file system.

'''
Switches between different execution environments and file systems.
'''
class OutputMixin(luigi.Task):
    exec_environment = luigi.EnumParameter(enum=ExecEnv, default=ExecEnv.HDFS)
    
    def get_output(self, fn):
        if self.exec_environment == ExecEnv.HDFS:
            return luigi.contrib.hdfs.HdfsTarget(fn)
        elif self.exec_environment == ExecEnv.MOCK:
            return MockTarget(fn)
        else:
            return luigi.LocalTarget(fn)


class InputData(OutputMixin):
    filename = luigi.Parameter()

    def output(self):
        return self.get_output(self.filename)


'''
Counts the number of steps / luigi tasks that we need for evaluating this query.
'''
def count_steps(raquery):
    assert(isinstance(raquery, radb.ast.Node))

    if (isinstance(raquery, radb.ast.Select) or isinstance(raquery,radb.ast.Project) or
        isinstance(raquery,radb.ast.Rename)):
        return 1 + count_steps(raquery.inputs[0])

    elif isinstance(raquery, radb.ast.Join):
        return 1 + count_steps(raquery.inputs[0]) + count_steps(raquery.inputs[1])

    elif isinstance(raquery, radb.ast.RelRef):
        return 1

    else:
        raise Exception("count_steps: Cannot handle operator " + str(type(raquery)) + ".")


class RelAlgQueryTask(luigi.contrib.hadoop.JobTask, OutputMixin):
    '''
    Each physical operator knows its (partial) query string.
    As a string, the value of this parameter can be searialized
    and shipped to the data node in the Hadoop cluster.
    '''
    querystring = luigi.Parameter()

    '''
    Each physical operator within a query has its own step-id.
    This is used to rename the temporary files for exhanging
    data between chained MapReduce jobs.
    '''
    step = luigi.IntParameter(default=1)

    '''
    In HDFS, we call the folders for temporary data tmp1, tmp2, ...
    In the local or mock file system, we call the files tmp1.tmp...
    '''
    def output(self):
        if self.exec_environment == ExecEnv.HDFS:
            filename = "tmp" + str(self.step)
        else:
            filename = "tmp" + str(self.step) + ".tmp"
        return self.get_output(filename)


'''
Given the radb-string representation of a relational algebra query,
this produces a tree of luigi tasks with the physical query operators.
'''
def task_factory(raquery, step=1, env=ExecEnv.HDFS):
    assert(isinstance(raquery, radb.ast.Node))
    
    if isinstance(raquery, radb.ast.Select):
        return SelectTask(querystring=str(raquery) + ";", step=step, exec_environment=env)

    elif isinstance(raquery, radb.ast.RelRef):
        filename = raquery.rel + ".json"
        return InputData(filename=filename, exec_environment=env)

    elif isinstance(raquery, radb.ast.Join):
        return JoinTask(querystring=str(raquery) + ";", step=step, exec_environment=env)

    elif isinstance(raquery, radb.ast.Project):
        return ProjectTask(querystring=str(raquery) + ";", step=step, exec_environment=env)

    elif isinstance(raquery, radb.ast.Rename):
        return RenameTask(querystring=str(raquery) + ";", step=step, exec_environment=env)
                          
    else:
        # We will not evaluate the Cross product on Hadoop, too expensive.
        raise Exception("Operator " + str(type(raquery)) + " not implemented (yet).")
    

class JoinTask(RelAlgQueryTask):

    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert(isinstance(raquery, radb.ast.Join))
      
        task1 = task_factory(raquery.inputs[0], step=self.step + 1, env=self.exec_environment)
        task2 = task_factory(raquery.inputs[1], step=self.step + count_steps(raquery.inputs[0]) + 1, env=self.exec_environment)

        return [task1, task2]

    
    def mapper(self, line):

        relation, tuple = line.split('\t')
        json_tuple = json.loads(tuple)
        
        raquery = radb.parse.one_statement_from_string(self.querystring)
        stmt_condition = raquery.cond

        ''' ...................... fill in your code below ........................'''

        single_conditions = get_single_conditions(stmt_condition)
        
        operand_values = []
        for condition in single_conditions:
            # get operands of the condition (convert to string to find it in the json_tuple)
            left_operand = str(condition.inputs[0])
            right_operand = str(condition.inputs[1])

            # determine the values of both operands and add them to the list of operand values
            if left_operand in json_tuple:
                operand_values.append(json_tuple[left_operand])
            elif right_operand in json_tuple:
                operand_values.append(json_tuple[right_operand])

        # only yield if there is at least one value to join on
        if len(operand_values) > 0:
            # key is a list of the evaluated values that have to match in json format
            join_key = json.dumps(operand_values)
            yield(join_key, (relation, tuple))


        ''' ...................... fill in your code above ........................'''


    def reducer(self, key, values):
        raquery = radb.parse.one_statement_from_string(self.querystring)

        # iterating two time of values does not work => build own list of values  
        values_list = [value for value in values]

        ''' ...................... fill in your code below ........................'''
        joined_relation_name = None

        all_joined_tuples = []
        for input1 in values_list:
            joined_tuple = {}
            relation1, input_tuple1 = input1
            json_tuple1 = json.loads(input_tuple1)

            for input2 in values_list:
                relation2, input_tuple2 = input2
                json_tuple2 = json.loads(input_tuple2)

                # only join tuples if the relation differs
                if relation1 != relation2:
                    
                    # save a new relation name
                    if joined_relation_name == None:
                        relation_names = [relation1, relation2]
                        relation_names.sort()
                        joined_relation_name = "{}\join\{}".format(relation_names[0], relation_names[1])

                    # add all attributes from relation1
                    for attribute_key1 in json_tuple1.keys():
                        joined_tuple[attribute_key1] = json_tuple1[attribute_key1]
                    # add all attributes from relation2
                    for attribute_key2 in json_tuple2.keys():
                        joined_tuple[attribute_key2] = json_tuple2[attribute_key2]

                    if len(joined_tuple.keys()) > 0:
                        # convert the joined_tuple to json in order to use it as key for a dictionary
                        json_joined_tuple = json.dumps(joined_tuple)
                        all_joined_tuples.append(json_joined_tuple) # somehow json has to be used because otherwise the issubset function will not work

        # filter out duplicate tuples
        distinct_joined_tuples = []
        for json_joined_tuple in all_joined_tuples:
            joined_tuple = json.loads(json_joined_tuple) 

            already_added_tuple = next((tuple for tuple in distinct_joined_tuples if set(joined_tuple.items()).issubset(set(tuple.items()))), None)
            if already_added_tuple == None:
                distinct_joined_tuples.append(joined_tuple)

        
        for joined_tuple in distinct_joined_tuples:
            json_joined_tuple = json.dumps(joined_tuple)
            yield(joined_relation_name, json_joined_tuple)

        ''' ...................... fill in your code above ........................'''   

class SelectTask(RelAlgQueryTask):

    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert(isinstance(raquery, radb.ast.Select))
        
        return [task_factory(raquery.inputs[0], step=self.step + 1, env=self.exec_environment)]

    
    def mapper(self, line):
        relation, tuple = line.split('\t')
        json_tuple = json.loads(tuple)

        stmt_condition = radb.parse.one_statement_from_string(self.querystring).cond
        
        ''' ...................... fill in your code below ........................'''
        single_conditions = get_single_conditions(stmt_condition)

        for condition in single_conditions:
            required_attr_key = str(condition)
            print("required attr key: "+ required_attr_key)
            required_rel_name, required_attr_name = get_single_relation_expression_attribute(condition)
            required_attr_value = next((attribute for attribute in condition.inputs if type(attribute) != radb.ast.AttrRef), None)

            # at least name and value of the operand have to be given (rel_name of the operand is optional)
            if required_attr_name == None or required_attr_value == None:
                return

            # try to get value by full key
            actual_attr_value = None
            actual_attr_value = next((json_tuple[key] for key in json_tuple.keys() if key == required_attr_key), None)

            # try to get value by attr_name
            if actual_attr_value == None:
                actual_attr_value = next((json_tuple[key] for key in json_tuple.keys() if key.endswith(required_attr_name)), None)

            # abort if no value available
            if actual_attr_value == None:
                return

            # convert values to strings for comparison of all types
            # remove the ' from the required_attr_value in order to make comparison more easy
            required_attr_value = str(required_attr_value).replace("'", "")
            actual_attr_value = str(actual_attr_value)

            if not actual_attr_value == required_attr_value:
                return


        yield(relation, tuple)

        
        ''' ...................... fill in your code above ........................'''


class RenameTask(RelAlgQueryTask):

    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert(isinstance(raquery, radb.ast.Rename))

        return [task_factory(raquery.inputs[0], step=self.step + 1, env=self.exec_environment)]


    def mapper(self, line):
        relation, tuple = line.split('\t')
        json_tuple = json.loads(tuple)

        raquery = radb.parse.one_statement_from_string(self.querystring)
        
        ''' ...................... fill in your code below ........................'''
        
        # get new name of the relation
        renamed_relation_name = raquery.relname
    
        # create a tuple that uses new relation names e.g. {foo.name = 'Christoph'} => {bar.name = 'Christoph'}
        renamed_tuple = {}
        for attribute_key in json_tuple.keys():
            value = json_tuple[attribute_key]
            _, attribute_name = attribute_key.split('.')
            renamed_tuple['{}.{}'.format(renamed_relation_name, attribute_name)] = value

        json_renamed_tuple = json.dumps(renamed_tuple)
        yield(renamed_relation_name, json_renamed_tuple)
        
        ''' ...................... fill in your code above ........................'''


class ProjectTask(RelAlgQueryTask):

    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert(isinstance(raquery, radb.ast.Project))

        return [task_factory(raquery.inputs[0], step=self.step + 1, env=self.exec_environment)]    


    def mapper(self, line):
        relation, tuple = line.split('\t')
        json_tuple = json.loads(tuple)

        attrs = radb.parse.one_statement_from_string(self.querystring).attrs
        attributes_to_include = list(map(lambda attr: str(attr), attrs)) # build list of attributes as strings

        ''' ...................... fill in your code below ........................'''

        # filter out attributes that are not requested
        projected_attributes = {}
        for attr_key in json_tuple:
            attr_tuple = attr_key.split('.')
            # determine name of the attribute without relation
            attr_name = None
            if len(attr_tuple) == 2:
                _, attr_name = attr_tuple

            # check if either the attr_key (e.g. Person.age) or the attr_name (e.g. age) is included in the attrs
            if attr_key in attributes_to_include:
                projected_attributes[attr_key] = json_tuple[attr_key]
            elif attr_name and attr_name in attributes_to_include: # TODO: check with test test_project_Person_gender line 217
                projected_attributes[attr_key] = json_tuple[attr_key]

        yield(relation, json.dumps(projected_attributes))
        
        ''' ...................... fill in your code above ........................'''


    def reducer(self, key, values):

        ''' ...................... fill in your code below ........................'''
        # filter out duplicate tuples
        distinct_tuples = []
        for json_tuple in values:
            current_tuple = json.loads(json_tuple)

            already_added_tuple = next((tuple for tuple in distinct_tuples if set(current_tuple.items()).issubset(set(tuple.items()))), None)
            if already_added_tuple == None:
                distinct_tuples.append(current_tuple)

        for tuple in distinct_tuples:
            yield(key, json.dumps(tuple))

        ''' ...................... fill in your code above ........................'''
        
        
if __name__ == '__main__':
    luigi.run()

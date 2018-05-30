
from enum import Enum
import json
import luigi
import luigi.contrib.hadoop
import luigi.contrib.hdfs
from luigi.mock import MockTarget
import radb
import radb.ast
import radb.parse

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

        condition_values = []
        for condition in raopt.get_single_conditions(stmt_condition):
            left = str(condition.inputs[0])
            right = str(condition.inputs[1])

            if left in json_tuple:
                condition_values.append(json_tuple[left])
            elif right in json_tuple:
                condition_values.append(json_tuple[right])

        if len(condition_values) > 0:
            condition_key = json.dumps(condition_values)
            yield(condition_key, (relation, tuple))


        ''' ...................... fill in your code above ........................'''


    def reducer(self, key, values):
        raquery = radb.parse.one_statement_from_string(self.querystring)
               
        values_list = [value for value in values]

        ''' ...................... fill in your code below ........................'''
        all_tuples = {}
        for input1 in values_list:
            joined_tuple = {}

            relation1, input_tuple1 = input1
            json_tuple1 = json.loads(input_tuple1)

            for input2 in values_list:
                relation2, input_tuple2 = input2
                json_tuple2 = json.loads(input_tuple2)

                if relation1 != relation2:
                    for attribute_key1 in json_tuple1.keys():
                        joined_tuple[attribute_key1] = json_tuple1[attribute_key1]
                    for attribute_key2 in json_tuple2.keys():
                        joined_tuple[attribute_key2] = json_tuple2[attribute_key2]

                    if len(joined_tuple.keys()) > 0:
                        output_json = json.dumps(joined_tuple)
                        all_tuples[output_json] = 1

        distinct_output_tuples = []
        for json_output in all_tuples.keys():
            output_dict = json.loads(json_output)

            already_added = next((existing for existing in distinct_output_tuples if set(output_dict.items()).issubset(set(existing.items()))), None)
            if already_added == None:
                distinct_output_tuples.append(output_dict)

        for output_tuple in distinct_output_tuples:
            yield(4711, json.dumps(output_tuple))

        ''' ...................... fill in your code above ........................'''   

import raopt

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

        for condition in raopt.get_single_conditions(stmt_condition):
            rel_name, attr_name = raopt.get_single_relation_expression_attribute(condition)
            attr_value = next((attribute for attribute in condition.inputs if type(attribute) != radb.ast.AttrRef), None)

            if attr_name == None or attr_value == None:
                return
           
            attr_value = str(attr_value).replace("'", "")

            if rel_name == None:
                rel_name = relation

            attr_identifier = "{}.{}".format(rel_name, attr_name)

            if not attr_identifier in json_tuple:
                return

            if not str(json_tuple[attr_identifier]) == attr_value:
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
        
        renamed_relation_name = raquery.relname
    
        renamed_tuple = {}
        for attribute_key in json_tuple.keys():
            value = json_tuple[attribute_key]
            _, attribute_name = attribute_key.split('.')
            renamed_tuple['{}.{}'.format(renamed_relation_name, attribute_name)] = value

        json_output = json.dumps(renamed_tuple)
        yield(renamed_relation_name, json_output)
        
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
        attrs = list(map(lambda attr: str(attr), attrs))

        ''' ...................... fill in your code below ........................'''
        # attrs = list(map(lambda attr: "{}.{}".format(relation, str(attr)), attrs))

        projected_attributes = {}
        for attr_key in json_tuple:
            attr_tuple = attr_key.split('.')
            attr_name = None
            if len(attr_tuple) == 2:
                _, attr_name = attr_tuple

            if attr_key in attrs or (attr_name and attr_name in attrs):
                projected_attributes[attr_key] = json_tuple[attr_key]

        yield(1, json.dumps(projected_attributes))
        
        ''' ...................... fill in your code above ........................'''


    def reducer(self, key, values):

        ''' ...................... fill in your code below ........................'''
        distinct_tuples = []
        for json_tuple in values:
            tuple = json.loads(json_tuple)

            already_added = next((existing for existing in distinct_tuples if set(tuple.items()).issubset(set(existing.items()))), None)
            if already_added == None:
                distinct_tuples.append(tuple)

        for output_tuple in distinct_tuples:
            yield(1, json.dumps(output_tuple))

        ''' ...................... fill in your code above ........................'''
        
        
if __name__ == '__main__':
    luigi.run()

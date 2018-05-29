
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

        for condition in raopt.get_single_conditions(stmt_condition):
            left = str(condition.inputs[0])
            right = str(condition.inputs[1])

            left_rel, _ = left.split('.')
            right_rel, _ = right.split('.')

            if left_rel == relation and left in json_tuple:
                yield(json_tuple[left], tuple)
            elif right_rel == relation and right in json_tuple:
                yield(json_tuple[right], tuple)

        ''' ...................... fill in your code above ........................'''


    def reducer(self, key, values):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        
        ''' ...................... fill in your code below ........................'''
        joined_tuple = {}
        for input in values:
            json_tuple = json.loads(input)
            for attribute_key in json_tuple.keys():
                joined_tuple[attribute_key] = json_tuple[attribute_key]
            
        json_output = json.dumps(joined_tuple)
        yield(key, json_output)
        
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
        print(json_output)
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

        print(str(json.dumps(projected_attributes)))
        yield(1, json.dumps(projected_attributes))
        
        ''' ...................... fill in your code above ........................'''


    def reducer(self, key, values):

        ''' ...................... fill in your code below ........................'''
        unique_inputs = {}
        for input in values:
            unique_inputs[input] = 1
        
        for input in unique_inputs.keys():
            yield(1, input)


        ''' ...................... fill in your code above ........................'''
        
        
if __name__ == '__main__':
    luigi.run()

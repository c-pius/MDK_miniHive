import radb.ast
from typing import List
from radb.parse import RAParser as sym

def rule_introduce_joins(stmt):
    return introduce_joins(stmt)

def introduce_joins(stmt):
    # bottom => return statement
    if type(stmt) == radb.ast.RelRef or type(stmt) == radb.ast.Rename:
        return stmt
    # project => dig deeper
    elif type(stmt) == radb.ast.Project:
        return radb.ast.Project(stmt.attrs, introduce_joins(stmt.inputs[0]))
    # cross => dig deeper in both sides
    elif type (stmt) == radb.ast.Cross:
        left = introduce_joins(stmt.inputs[0])
        right = introduce_joins(stmt.inputs[1])
        return radb.ast.Cross(left, right)
    # join => dig deeper in both sides
    elif type(stmt) == radb.ast.Join:
        left = introduce_joins(stmt.inputs[0])
        right = introduce_joins(stmt.inputs[1])
        return radb.ast.Join(left, stmt.cond, right)
    # select
    elif type(stmt) == radb.ast.Select:
        # get the expression
        expression = stmt.cond

        # combined select using joining expression => break it up and introduce joins to the new statement
        if (expression.op == sym.AND and
            is_joining_expression(expression)):
            new_input_using_joins = radb.ast.Select(expression, introduce_joins(stmt.inputs[0]))
            broken_up_statement = break_up_single_selection(new_input_using_joins)
            return introduce_joins(broken_up_statement)
        elif is_expression_with_multiple_relation_attributes(expression):
            # introduce a single join for the given expression
            stmt_with_join = introduce_single_join(stmt.inputs[0], expression)
            # join was introduced => check for further joins
            if str(stmt) != str(stmt_with_join):
                return introduce_joins(stmt_with_join)
            # join was not introduced => keep the select and check for further joins
            else:
                return radb.ast.Select(stmt.cond, introduce_joins(stmt.inputs[0]))
        # not a join condition selection => keep the select and check for further joins
        else:
            return radb.ast.Select(stmt.cond, introduce_joins(stmt.inputs[0]))
    else:
        return stmt


def is_joining_expression(expression):
    single_conditions = get_single_conditions(expression)
    
    # if one condition is not a joining condition => cannot be used to introduce join
    for condition in single_conditions:
        if not is_expression_with_multiple_relation_attributes(condition):
            return False

    # get all included relations
    included_relations = {}
    for condition in single_conditions:
        attributes = get_multiple_relation_expression_attributes(condition)
        for attribute in attributes:
            relation_name, _ = attribute
            included_relations[relation_name] = True

    # if only two realtions included the condition can be pushed down
    if len(included_relations) == 2:
        return True
    else:
        return False
    
# recursivley merges a SINGLE expression to a cross
def introduce_single_join(stmt, expression):
    # bottom => return statement
    if type(stmt) == radb.ast.RelRef:
        return stmt
    # project => dig deeper
    elif type(stmt) == radb.ast.Project:
        return radb.ast.Project(stmt.attrs, introduce_single_join(stmt.inputs[0], expression))
    # select => dig deeper
    elif type(stmt) == radb.ast.Select:
        return radb.ast.Select(stmt.cond, introduce_single_join(stmt.inputs[0], expression))
    # cross
    elif type(stmt) == radb.ast.Cross:
        # check if the expression applies to the cross => introduce join and remove cross
        if is_cross_or_join_using_both_relations(stmt, expression):
            return radb.ast.Join(stmt.inputs[0], expression, stmt.inputs[1])
        # expression does not apply to the cross => dig deeper in both sides
        else:
            left = introduce_single_join(stmt.inputs[0], expression)
            right = introduce_single_join(stmt.inputs[1], expression)
            return radb.ast.Cross(left, right)
    elif type(stmt) == radb.ast.Join:
        if is_cross_or_join_using_both_relations(stmt, expression):
            new_join_expression = radb.ast.ValExprBinaryOp(stmt.cond, sym.AND, expression)
            return radb.ast.Join(stmt.inputs[0], new_join_expression, stmt.inputs[1])
    # cannot go any further
    else:
        return stmt


# checks wether the current stmt is a cross combining both relations used in the expression
def is_cross_or_join_using_both_relations(stmt, expression):
    assert(type(stmt) == radb.ast.Cross or type(stmt) == radb.ast.Join)
    
    # get statements of the cross
    left = stmt.inputs[0]
    right = stmt.inputs[1]

    # get names of relations used in the cross
    left_relation_names = get_relation_names(left)
    right_relation_names = get_relation_names(right)

    # get names of relations used in the expression
    expression_attributes_list = get_multiple_relation_expression_attributes(expression)
    expression_left_relation, _ = expression_attributes_list[0]
    expression_right_relation, _ = expression_attributes_list[1]

    # check whether relations from both sides of the cross are used in the expression
    if  expression_left_relation in left_relation_names:
        if  expression_right_relation in right_relation_names:
            return True
        else:
            return False
    elif expression_right_relation in left_relation_names:
        if expression_left_relation in right_relation_names:
            return True
        else:
            return False
    else:
        return False

# gets the relation names used in the statement
def get_relation_names(stmt):
    relation_names = []

    # cross or join => combine relations from both sides
    if type(stmt) == radb.ast.Cross or type(stmt) == radb.ast.Join:
        left = stmt.inputs[0]
        right = stmt.inputs[1]
        relation_names.extend(get_relation_names(left))
        relation_names.extend(get_relation_names(right))
    # project or select => dig deeper
    elif type(stmt) == radb.ast.Project or type(stmt) == radb.ast.Select:
        relation_names.extend(get_relation_names(stmt.inputs[0]))
    # must be rename or relation => get the name of the relation
    else:
        relation_names.append(get_relation_name(stmt))

    return relation_names

#######################################################

def rule_merge_selections(stmt):
    return merge_selections(stmt)

# recursively searches for select statement following each other and merges those two select statement
def merge_selections(stmt):
    # bottom => return statement
    if type(stmt) == radb.ast.RelRef or type(stmt) == radb.ast.Rename:
        return stmt
    # project => dig deeper
    elif type(stmt) == radb.ast.Project:
        return radb.ast.Project(stmt.attrs, merge_selections(stmt.inputs[0]))
    # cross => dig deeper in both sides
    elif type (stmt) == radb.ast.Cross:
        left = merge_selections(stmt.inputs[0])
        right = merge_selections(stmt.inputs[1])
        return radb.ast.Cross(left, right)
    # join => dig deeper in both sides
    elif type(stmt) == radb.ast.Join:
        left = merge_selections(stmt.inputs[0])
        right = merge_selections(stmt.inputs[1])
        return radb.ast.Join(left, stmt.cond, right)
    # select
    elif type(stmt) == radb.ast.Select:
        # check if the following statement is also select and pull the second selct up
        if type(stmt.inputs[0]) == radb.ast.Select:
            # get the following select statement
            expression = stmt.inputs[0].cond
            # strip the select statement from the following statement
            stripped_statement = strip_select(stmt.inputs[0])
            # merge the current select with the following select 
            merged_expression = radb.ast.ValExprBinaryOp(stmt.cond, sym.AND, expression)
            # create the select with the merged statement
            merged_select = radb.ast.Select(merged_expression, stripped_statement)
            # check if there is another select one level deeper 
            return merge_selections(merged_select)
        # following statement is not a select => dig deeper
        else:
            return radb.ast.Select(stmt.cond, merge_selections(stmt.inputs[0]))


# removes the top level select statement
def strip_select(stmt):
    if type(stmt) == radb.ast.Select:
        return stmt.inputs[0]
    else:
        return stmt

#################################################################

def rule_push_down_selections(stmt, relation_schemas):
    return push_down_selections(stmt, relation_schemas)

def push_down_selections(stmt, relation_schemas):
    # bottom => return statement
    if type(stmt) == radb.ast.RelRef:
        return stmt
    # project => dig deeper
    elif type(stmt) == radb.ast.Project:
        return radb.ast.Project(stmt.attrs, push_down_selections(stmt.inputs[0], relation_schemas))
    # cross => dig deeper in both sides
    elif type (stmt) == radb.ast.Cross:
        left = push_down_selections(stmt.inputs[0], relation_schemas)
        right = push_down_selections(stmt.inputs[1], relation_schemas)
        return radb.ast.Cross(left, right)
    elif type(stmt) == radb.ast.Join:
        left = push_down_selections(stmt.inputs[0], relation_schemas)
        right = push_down_selections(stmt.inputs[1], relation_schemas)
        return radb.ast.Join(left, stmt.cond, right)
    # select
    elif type(stmt) == radb.ast.Select:
        expression = stmt.cond
        stripped_statement = stmt.inputs[0] # removes the select from the current statement
        pushed_down = None
        if is_expression_with_single_relation_attribute(expression):
            expression_relation_name = get_expression_relation_name(expression, relation_schemas)
            if expression_relation_name != None and len(get_relation_names(stmt)) > 1: # regular select does not have to be pushed down if only one relation is used
                pushed_down = push_down_select_using_single_relation(stripped_statement, expression, expression_relation_name)
            else:
                return radb.ast.Select(expression, push_down_selections(stripped_statement, relation_schemas))
        elif is_expression_with_multiple_relation_attributes(expression):
            if len(get_relation_names(stmt)) > 2: # joining select does not have to be pushed down if only two relations are used
                pushed_down = push_down_select_using_multiple_relations(stripped_statement, expression)

        if pushed_down and str(pushed_down) != str(stmt):
            return push_down_selections(pushed_down, relation_schemas)
        else:
            return radb.ast.Select(expression, push_down_selections(stripped_statement, relation_schemas))
    # can't handle anything else
    else:
        return stmt

# gets the relation name or tries to find it from the relation schemas
def get_expression_relation_name(expression, relation_schemas):
    expression_relation, expression_attribute_name = get_single_relation_expression_attribute(expression)
    # no relation used in expression => find corresponding relation from the schemas
    if expression_relation == None:
        # get the first schema including the name => TODO: must be one only
        # schemas_containing_the_attribute_name = [relation_key for relation_key in relation_schemas.keys() if expression_attribute_name in relation_schemas[relation_key]]
        # if len(schemas_containing_the_attribute_name) == 1:
        #     expression_relation = schemas_containing_the_attribute_name[0]
        expression_relation = next((relation_key for relation_key in relation_schemas.keys() if expression_attribute_name in relation_schemas[relation_key]), None)

    return expression_relation

def push_down_select_using_single_relation(stmt, expression, expression_relation_name):  
    # bottom => return statement
    if type(stmt) == radb.ast.RelRef:
        relation_name = get_relation_name(stmt)
        if relation_name == expression_relation_name:
            return radb.ast.Select(expression, stmt)
        else:
            return stmt
    elif type(stmt) == radb.ast.Rename:
        relation_name = stmt.inputs[0].rel
        renamed_name = get_relation_name(stmt)
        if (relation_name == expression_relation_name or renamed_name == expression_relation_name):
            return radb.ast.Select(expression, stmt)
        else:
            return stmt
    # project => dig deeper
    elif type(stmt) == radb.ast.Project:
        return radb.ast.Project(stmt.attrs, push_down_select_using_single_relation(stmt.inputs[0], expression, expression_relation_name))
    # select => dig deeper
    elif type(stmt) == radb.ast.Select:
        return radb.ast.Select(stmt.cond, push_down_select_using_single_relation(stmt.inputs[0], expression, expression_relation_name))
    # cross
    elif type(stmt) == radb.ast.Cross:
        left = push_down_select_using_single_relation(stmt.inputs[0], expression, expression_relation_name)
        right = push_down_select_using_single_relation(stmt.inputs[1], expression, expression_relation_name)
        return radb.ast.Cross(left, right)
    # can't handle anything else
    else:
        return stmt

# pushed down select to the corresponding cross (does not require realtion_schema since the relation has to be stated anyway)
def push_down_select_using_multiple_relations(stmt, expression):
    # bottom => return statement
    if type(stmt) == radb.ast.RelRef:
        return stmt
    # project => dig deeper
    elif type(stmt) == radb.ast.Project:
        return radb.ast.Project(stmt.attrs, push_down_select_using_multiple_relations(stmt.inputs[0], expression))
    # select => dig deeper
    elif type(stmt) == radb.ast.Select:
        return radb.ast.Select(stmt.cond, push_down_select_using_multiple_relations(stmt.inputs[0], expression))
    # cross
    elif type(stmt) == radb.ast.Cross:
        # check if the expression applies to the cross => introduce join and remove cross
        if is_cross_or_join_using_both_relations(stmt, expression):
            return radb.ast.Select(expression, stmt)
        # expression does not apply to the cross => dig deeper in both sides
        else:
            left = push_down_select_using_multiple_relations(stmt.inputs[0], expression)
            right = push_down_select_using_multiple_relations(stmt.inputs[1], expression)
            return radb.ast.Cross(left, right)
    # can't handle anything else
    else:
        return stmt

# gets the name of included relations according to the type of the statement
def get_relation_name(statement: radb.ast.RelExpr):
    if type(statement) == radb.ast.RelRef:
        return statement.rel
    elif type(statement) == radb.ast.Select:
        return get_relation_name(statement.inputs[0])
    elif type(statement) == radb.ast.Rename:
        return statement.relname

# checks wether the expression contains only a single relation
def is_expression_with_single_relation_attribute(expression: radb.ast.FuncValExpr):
    assert(len(expression.inputs) == 2)
    left = expression.inputs[0]
    right = expression.inputs[1]

    if not (type(left) == radb.ast.AttrRef and type(right) == radb.ast.AttrRef):
        return True
    else:
        return False

# checks wether the expression contains multiple relations
def is_expression_with_multiple_relation_attributes(expression:radb.ast.FuncValExpr):
    assert(len(expression.inputs) == 2)
    left = expression.inputs[0]
    right = expression.inputs[1]

    if (type(left) == radb.ast.AttrRef and type(right) == radb.ast.AttrRef):
        return True
    else:
        return False

# gets a tuple containing the relation name and the attribute name
def get_single_relation_expression_attribute(expression: radb.ast.FuncValExpr):
    return next(((attribute.rel, attribute.name) for attribute in expression.inputs if type(attribute) == radb.ast.AttrRef), None)

# gets a list containing tuples containing the relation name and the attribute name
def get_multiple_relation_expression_attributes(expression: radb.ast.FuncValExpr):
    left = expression.inputs[0]
    right = expression.inputs[1]
    return [(left.rel, left.name), (right.rel, right.name)]

# recursively get all selection conditions included in the statment
def get_selection_expressions(stmt: radb.ast.RelExpr):
    selections: List[radb.ast.ValExpr] = []
    
    # add the condition if it is a select statement
    if type(stmt) == radb.ast.Select:
        selections.append(stmt.cond)

    # dig one level deeper for conditions and add it to the selections
    if len(stmt.inputs) == 1:
        deeper_selection = get_selection_expressions(stmt.inputs[0])
        if len(deeper_selection) > 0:
            selections.extend(deeper_selection)

        return selections
    # last level => return
    else:
        return selections


#########################################################################


def rule_break_up_selections(stmt):
    return break_up_selections(stmt)

def break_up_selections(stmt):
    #break up a given select statement
    if type(stmt) == radb.ast.Select:
        return break_up_single_selection(stmt)
    #create a new select statement with the broken up select statement
    elif type(stmt) == radb.ast.Project:
        return radb.ast.Project(stmt.attrs, break_up_selections(stmt.inputs[0]))
    #create a new cross from both sides broken up
    elif type(stmt) == radb.ast.Cross:
        left = break_up_selections(stmt.inputs[0])
        right = break_up_selections(stmt.inputs[1])
        return radb.ast.Cross(left, right)
    elif type(stmt) == radb.ast.Join:
        left = break_up_selections(stmt.inputs[0])
        right = break_up_selections(stmt.inputs[1])
        return radb.ast.Join(left, stmt.cond, right)
    #can't break up anything else
    else:
        return stmt   

def break_up_single_selection(stmt: radb.ast.Select):

    # split up expression using AND into its single components 
    single_expressions = get_single_conditions(stmt.cond)

    # dig deeper to break up following selects, too
    broken_up_statement = break_up_selections(stmt.inputs[0])

    # apply single selects to the reamining statement
    for expression in single_expressions:
        broken_up_statement = radb.ast.Select(expression, broken_up_statement)

    return broken_up_statement


def get_single_conditions(expression):
    single_expressions = []
    if expression.op == sym.AND:        
        single_expressions.extend(get_single_conditions(expression.inputs[1]))
        single_expressions.extend(get_single_conditions(expression.inputs[0]))
    else:
        single_expressions.append(expression)

    return single_expressions

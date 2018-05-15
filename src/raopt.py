import radb.ast
from typing import List
from radb.parse import RAParser as sym

def rule_introduce_joins(stmt):
    return introduce_joins(stmt)

def introduce_joins(stmt):
    # bottom => return statement
    if type(stmt) == radb.ast.RelRef:
        return stmt
    # project => dig deeper
    elif type(stmt) == radb.ast.Project:
        return radb.ast.Project(stmt.attrs, introduce_joins(stmt.inputs[0]))
    # cross => dig deeper in both sides
    elif type (stmt) == radb.ast.Cross:
        left = introduce_joins(stmt.inputs[0])
        right = introduce_joins(stmt.inputs[1])
        return radb.ast.Cross(left, right)
    # select
    elif type(stmt) == radb.ast.Select:
        # get the expression
        expression = stmt.cond
        if is_expression_with_multiple_relation_attributes(expression):
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
    elif type(stmt) == radb.ast.Join:
        left = introduce_joins(stmt.inputs[0])
        right = introduce_joins(stmt.inputs[1])
        return radb.ast.Join(left, stmt.cond, right)
    else:
        return stmt

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
        if is_cross_using_both_relations(stmt, expression):
            return radb.ast.Join(stmt.inputs[0], expression, stmt.inputs[1])
        # expression does not apply to the cross => dig deeper in both sides
        else:
            left = introduce_single_join(stmt.inputs[0], expression)
            right = introduce_single_join(stmt.inputs[1], expression)
            return radb.ast.Cross(left, right)
    # cannot go any further
    else:
        return stmt


# checks wether the current stmt is a cross combining both relations used in the expression
def is_cross_using_both_relations(stmt, expression):
    assert(type(stmt) == radb.ast.Cross)
    
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
    if type(stmt) == radb.ast.RelRef:
        return stmt
    # project => dig deeper
    elif type(stmt) == radb.ast.Project:
        return radb.ast.Project(stmt.attrs, merge_selections(stmt.inputs[0]))
    # cross => dig deeper in both sides
    elif type (stmt) == radb.ast.Cross:
        left = merge_selections(stmt.inputs[0])
        right = merge_selections(stmt.inputs[1])
        return radb.ast.Cross(left, right)
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
    #assert isinstance(stmt, radb.ast.Select)

    if type(stmt) == radb.ast.Project:
        pushed_down_stmt = push_down_selection(stmt.inputs[0], relation_schemas)
        return radb.ast.Project(stmt.attrs, pushed_down_stmt)
    else:
        return push_down_selection(stmt, relation_schemas)

def push_down_selection(stmt: radb.ast.Select, relation_schemas):

    # get list of single selection statements
    selection_expressions: List[radb.ast.ValExpr] = get_selection_expressions(stmt)

    # get list of single relations (has to account for renamed relations)
    relations: List[radb.ast.RelExpr] = get_relations(stmt)

    # build maps containing the expression and the expression attributes grouped by single or multiple relation statement
    single_relation_expressions = {}
    multiple_relation_expressions = {}
    for expression in selection_expressions:
        if is_expression_with_single_relation_attribute(expression):
            single_relation_expressions[expression] = get_single_relation_expression_attribute(expression)
        elif is_expression_with_multiple_relation_attributes(expression):
            multiple_relation_expressions[expression] = get_multiple_relation_expression_attributes(expression)

    # list containing selections that cannot be pushed down that are applied later
    # remaining_selections: List[radb.ast.ValExpr] = []

    # get a map of the relations including their real name and a list for storing expressions to push down: relation => (name, [])
    relations_with_real_name = get_relations_with_real_name(relations)

    # append all expressions using one relation only to the list associated to each relation included in the map of relations with name
    for expression in single_relation_expressions:
        attr_relation, attr_name = single_relation_expressions[expression]

        # get the name of the relation from the schema => uses the first relation from the schema including the attribute
        if attr_relation == None:
            relation_key = next((relation_key for relation_key in relation_schemas.keys() if attr_name in relation_schemas[relation_key]), None)
            if relation_key == None:
                # remaining_selections.append(expression)
                continue
            else:
                attr_relation = relation_key
                # check if the relation found from the schema was renamed in the statement
                if not attr_relation in relations_with_real_name:
                    renamed_relation_name = next((relation for relation in relations_with_real_name if relations_with_real_name[relation][1]  == attr_relation), None)
                    attr_relation = renamed_relation_name
                # else:
                #     attr_relation = relation_key

        # continue if the relation is not included in the list of relations => should not happen
        if not attr_relation in relations_with_real_name:
                continue

        # get the corresponding relation from the list of relations
        relation = relations_with_real_name[attr_relation]
        real_relation_name = relation[1]

        # continue if the real name is not included in the schemas
        if not real_relation_name in relation_schemas:
            continue

        # continue if the corresponding relation schema does not include the attribute
        relation_schema = relation_schemas[real_relation_name]
        if not attr_name in relation_schema:
            continue

        # append the current expression to the list of expressions to be pushed down of the current relation
        relation[2].append(expression)

    # push down expressions containing only a single relation to their corresponding relation
    pushed_down_selections: List[radb.ast.RelExpr] = push_down_single_relation_expressions(relations_with_real_name)

    # join the relations again using the cross join
    joined_relations = None
    for selection in pushed_down_selections:
        if joined_relations == None:
            joined_relations = selection
        else:
            joined_relations = radb.ast.Cross(joined_relations, selection)
    
    # push down selections using multiple relations to the corresponding cross joins
    for expression in multiple_relation_expressions:
        joined_relations = push_down_multiple_relation_expression(joined_relations, expression, multiple_relation_expressions[expression])

    return joined_relations

# pushed down an expression contianing to relations to the corresponding cross operatrion
def push_down_multiple_relation_expression(statement, expression, expression_attributes):
    # get the relations
    attr_relation_1, attr_name_1 = expression_attributes[0]
    attr_relation_2, attr_name_2 = expression_attributes[1]

    # try to push down the selection to the corresponding cross
    pushed_down = push_down_selection_to_corresponding_join(statement, expression, attr_relation_1, attr_relation_2)

    # nothing was pushed down => apply the selection globally to the statement
    if str(statement) == str(pushed_down):
        return radb.ast.Select(expression, statement)

    return pushed_down

# recusrively searches for a cross including both relations to apply the selection to
def push_down_selection_to_corresponding_join(statement, expression, relation1, relation2):   
    if type(statement) == radb.ast.Cross:
        # found the cross => apply selection
        if get_relation_name(statement.inputs[0]) == relation1 and get_relation_name(statement.inputs[1]) == relation2:
            return radb.ast.Select(expression, statement)
        # some other join => try to apply selection to both sides
        else:
            left = push_down_selection_to_corresponding_join(statement.inputs[0], expression, relation1, relation2)
            right = push_down_selection_to_corresponding_join(statement.inputs[1], expression, relation1, relation2)
            return radb.ast.Cross(left, right)
    # found relation => can't search any deeper
    elif type(statement) == radb.ast.RelRef:
        return statement
    # found select/project => dig deeper
    else:
        pushed_down = push_down_selection_to_corresponding_join(statement.inputs[0], expression, relation1, relation2)
        if type(statement) == radb.ast.Select:
            return radb.ast.Select(statement.cond, pushed_down)
        elif type(statement) == radb.ast.Project:
            return radb.ast.Project(statement.attr, pushed_down)
        else:
            return push_down_selection_to_corresponding_join(statement.inputs[0], expression, relation1, relation2)

# gets the name of included relations according to the type of the statement
def get_relation_name(statement: radb.ast.RelExpr):
    if type(statement) == radb.ast.RelRef:
        return statement.rel
    elif type(statement) == radb.ast.Select:
        return get_relation_name(statement.inputs[0])
    elif type(statement) == radb.ast.Rename:
        return statement.relname

# pushes down a selection using one relation only
def push_down_single_relation_expressions(relations_with_real_name):
        # list relations are stored in that have slections pushed down on them
    pushed_down_selections: List[radb.ast.RelExpr] = []
    
    for relation_by_name in relations_with_real_name.values():
        relation, real_name, expressions = relation_by_name

        # reverse expressions to keep the same order as before
        expressions.reverse()

        # combine selection statement 
        combined_statement = None
        for expression in expressions:
            # first selection => use expression and relation
            if combined_statement == None:
                combined_statement = radb.ast.Select(expression, relation)
            # further selection => use expression and previous statement
            else:
                combined_statement = radb.ast.Select(expression, combined_statement)

        # there was no selection => use the relation itself
        if combined_statement == None:
            pushed_down_selections.append(relation)
        # there were selection => use the combined statement
        else:
            pushed_down_selections.append(combined_statement)

    return pushed_down_selections

#get a map of relations including their real name and an empty array to store the attributes to be applied in
def get_relations_with_real_name(relations: radb.ast.RelExpr):
    relations_with_name = {}
    for relation in relations:
        #standard realtion => add with name as is
        if type(relation) == radb.ast.RelRef:
            relations_with_name[relation.rel] = (relation, relation.rel, [])
        #renamed relation => add with real name from input
        elif type(relation) == radb.ast.Rename:
            relations_with_name[relation.relname] = (relation, relation.inputs[0].rel, [])

    return relations_with_name

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

#recusively get all relations included in the statement
def get_relations(stmt: radb.ast.RelExpr):
    relations: List[radb.ast.RelExpr] = []

    # recusion end => return the relation or rename
    if type(stmt) == radb.ast.RelRef or type(stmt) == radb.ast.Rename:
        relations.append(stmt)
        return relations
    # cross => get relations to the left and relations to the right and merge them together with existing relations
    elif type(stmt) == radb.ast.Cross:
        leftRelations: List[radb.ast.RelExpr] = get_relations(stmt.inputs[0]) #stmt.inputs[0] = left
        rightRelations: List[radb.ast.RelExpr] = get_relations(stmt.inputs[1]) #stmt.inputs[1] = right
        relations.extend(leftRelations)
        relations.extend(rightRelations)
        return relations
    # something else => dig deeper for relations
    else:
        deeper_relations = get_relations(stmt.inputs[0])
        if len(deeper_relations) > 0:
            relations.extend(deeper_relations)
        return relations

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
    #break up a given select statement
    if type(stmt) == radb.ast.Select:
        return break_up_selections_internal(stmt)
    #create a new select statement with the broken up select statement
    elif type(stmt) == radb.ast.Project:
        return radb.ast.Project(stmt.attrs, break_up_selections_internal(stmt.inputs[0]))
    #create a new cross from both sides broken up
    elif type(stmt) == radb.ast.Cross:
        left = rule_break_up_selections(stmt.inputs[0])
        right = rule_break_up_selections(stmt.inputs[1])
        return radb.ast.Cross(left, right)
    #can't break up anything else
    else:
        return stmt   

def break_up_selections_internal(selectStmt: radb.ast.Select):
    #assumes that there is only one input
    return break_up(selectStmt.cond.inputs, selectStmt.inputs[0])

def break_up(conditions: [], input: radb.ast.RelExpr):
    if len(conditions) == 1:
        return radb.ast.Select(conditions[0], input)
    else:
        return radb.ast.Select(conditions[0], break_up(conditions[1:], input))



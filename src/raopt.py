import radb.ast
from typing import List


def rule_push_down_selections(stmt, relation_schemas):
    #assert isinstance(stmt, radb.ast.Select)

    if type(stmt) == radb.ast.Project:
        pushed_down_stmt = push_down_selection(stmt.inputs[0], relation_schemas)
        return radb.ast.Project(stmt.attrs, pushed_down_stmt)
    else:
        return push_down_selection(stmt, relation_schemas)

def push_down_selection(stmt: radb.ast.Select, relation_schemas):

    #get list of single selection statements
    selection_expressions: List[radb.ast.ValExpr] = []
    selection_expressions = get_selection_expressions(stmt, selection_expressions)

    #get list of single relations (has to account for renamed relations)
    relations: List[radb.ast.RelExpr] = []
    relations = get_relations(stmt, relations)

    remaining_selections: List[radb.ast.ValExpr] = []

    relations_by_name = get_relations_by_name(relations)

    for expression in selection_expressions:
        attribute = get_expression_attribute(expression)
        if attribute == None:
            remaining_selections.append(expression)
            continue

        attr_relation, attr_name = attribute

        if attr_relation == None:
            relation_key = next((relation_key for relation_key in relation_schemas.keys() if attr_name in relation_schemas[relation_key]), None)
            if relation_key == None:
                remaining_selections.append(expression)
                continue
            else:
                attr_relation = relation_key
                if not attr_relation in relations_by_name:
                    renamed_relation_name = next((relation for relation in relations_by_name if relations_by_name[relation][1]  == attr_relation), None)
                    attr_relation = renamed_relation_name
                else:
                    attr_relation = relation_key

        if not attr_relation in relations_by_name:
                continue

        relation = relations_by_name[attr_relation]
        real_relation_name = relation[1]

        if not real_relation_name in relation_schemas:
            continue
        relation_schema = relation_schemas[real_relation_name]
        
        if not attr_name in relation_schema:
            continue

        relation[2].append(expression)


    pushed_down_selections = []
    for relation_by_name in relations_by_name.values():
        relation, real_name, expressions = relation_by_name

        expressions.reverse()

        combined = None
        for expression in expressions:
            if combined == None:
                combined = radb.ast.Select(expression, relation)
            else:
                combined = radb.ast.Select(expression, combined)

        if combined == None:
            pushed_down_selections.append(relation)
        else:
            pushed_down_selections.append(combined)

    joined_relations = None
    for selection in pushed_down_selections:
        if joined_relations == None:
            joined_relations = selection
        else:
            joined_relations = radb.ast.Cross(joined_relations, selection)
    
    remaining_selections.reverse()
    for selection in remaining_selections:
        if joined_relations == None:
            break
        else:
           joined_relations = radb.ast.Select(selection, joined_relations)

    #for every selection
        #check if single relation is used
        #if so, find that relation from the list of relations
        #append selection to the relations => use a map
    
    #for every relation
        #build select relation

    #cmobine all relations

    return joined_relations

def get_relations_by_name(relations: radb.ast.RelExpr):
    relations_by_name = {}
    for relation in relations:
        if type(relation) == radb.ast.RelRef:
            relations_by_name[relation.rel] = (relation, relation.rel, [])
        elif type(relation) == radb.ast.Rename:
            relations_by_name[relation.relname] = (relation, relation.inputs[0].rel, [])

    return relations_by_name

def get_expression_attribute(expression: radb.ast.FuncValExpr):
    assert(len(expression.inputs) == 2)

    # cannot push down joining expressions
    if type(expression.inputs[0]) == radb.ast.AttrRef and type(expression.inputs[1]) == radb.ast.AttrRef:
        return None
    # get a tuple containing the relation and the name of the attribute
    else:
        return next(((attribute.rel, attribute.name) for attribute in expression.inputs if type(attribute) == radb.ast.AttrRef), None)


def get_relations(stmt: radb.ast.RelExpr, relations: List[radb.ast.RelExpr]):
    # recusion end => return the relation or rename
    if type(stmt) == radb.ast.RelRef or type(stmt) == radb.ast.Rename:
        relations.append(stmt)
        return relations
    # cross => get relations to the left and relations to the right and merge them together with existing relations
    elif type(stmt) == radb.ast.Cross:
        leftRelations: List[radb.ast.RelExpr] = get_relations(stmt.inputs[0], []) #stmt.inputs[0] = left
        rightRelations: List[radb.ast.RelExpr] = get_relations(stmt.inputs[1], []) #stmt.inputs[1] = right
        relations.extend(leftRelations)
        relations.extend(rightRelations)
        return relations
    # something else => dig deeper for relations
    else:
        return get_relations(stmt.inputs[0], relations)

# recursively get all selection conditions included in the statment
def get_selection_expressions(stmt: radb.ast.RelExpr, selections: List[radb.ast.ValExpr]):
    # add the condition if it is a select statement
    if type(stmt) == radb.ast.Select:
        selections.append(stmt.cond)

    # dig one level deeper for conditions
    if len(stmt.inputs) == 1:
        return get_selection_expressions(stmt.inputs[0], selections)
    # las level => return
    else:
        return selections



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



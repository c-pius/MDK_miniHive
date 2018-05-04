import radb.ast

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



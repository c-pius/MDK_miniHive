import sqlparse
import radb
import radb.parse
import radb.ast
from radb.parse import RAParser as sym

def translate(stmt: sqlparse.sql.Statement):
    # remove unnecessary tokens
    cleanedTokens = cleanTokens(stmt.tokens)
    return translateStatment(cleanedTokens)

def translateStatment(tokens: sqlparse.sql.TokenList):
    # get index of the select token
    selectToken = next(token for token in tokens if token.match(sqlparse.tokens.DML, ['SELECT']))
    selectTokenIndex = tokens.index(selectToken)
    if selectTokenIndex == -1:
        Exception('INVALID STATEMENT')

    # get token relevant for projection (token after select token)
    projectionToken = tokens[selectTokenIndex + 1]

    # get the statement without projection
    statement = select(tokens)

    # no project statement necessary if wildcard given
    if projectionToken.ttype == sqlparse.tokens.Token.Wildcard:
        return statement
    # add projection
    else:
        # get identifiers for projection
        projectionIdentifiers = getProjectionIdentifiers(projectionToken)

        projection = radb.ast.Project(projectionIdentifiers, statement)
        return projection

def getProjectionIdentifiers(projectionToken: sqlparse.tokens.Token):
    attributes = []

    # stupid approach by splitting identifers at ',' but works...
    separatedIdentifers = str(projectionToken.normalized).replace(' ', '').split(',')
    for identifier in separatedIdentifers:
        values = identifier.split('.')
        # length = 1 => simply state the identifier
        if len(values) == 1:
            attributes.append(radb.ast.AttrRef(None, str(values[0])))
        # length = 2 => attribute made up of relation and identifier
        elif len(values) == 2:
            attributes.append(radb.ast.AttrRef(str(values[0]), str(values[1])))

    return attributes

# gets the select statement including the conditions and tables
def select(tokens: sqlparse.sql.TokenList):
    # get the relations (joined and renamed)
    relations = getRelations(tokens)

    # get the where statment token
    whereToken = next((token for token in tokens if type(token) == sqlparse.sql.Where), None)

    # format with select statment if where token available
    if not whereToken == None:
        # get the conditions for the select statement
        conditions = getConditions(whereToken)
        select = radb.ast.Select(conditions, relations)
        return select
    # return blank relations if no where token available
    else:
        return relations

# gets conditions for the select statement
def getConditions(whereToken: sqlparse.sql.Token):

    # clean tokens and strip where token
    cleanedTokens = cleanTokens(whereToken.tokens)
    cleanedTokens = cleanedTokens[1:]

    # build a list of prepared comparisions and 'AND's and 'OR's
    comparisons = []
    for token in cleanedTokens:
        comparisons.append(getComparison(token))

    # combine the list of comparisions
    combinedComparisons = None
    for i in range(len(comparisons)):
        if type(comparisons[i]) == radb.ast.ValExprBinaryOp:
            # first comparision => take as is
            if combinedComparisons == None:
                combinedComparisons = comparisons[i]
            # combine following comparisions with the already combine ones
            else:
                combinedComparisons = radb.ast.ValExprBinaryOp(combinedComparisons, comparisons[i-1], comparisons[i])
          
    return combinedComparisons

def getComparison(comparisonToken: sqlparse.sql.Token):
    # comparisons consist of left operator right => get each and combine them 
    if type(comparisonToken) == sqlparse.sql.Comparison:
        tokens = cleanTokens(comparisonToken.tokens)
        left = getComparisonValue(tokens[0])
        operator = getComparisonOperator(tokens[1])
        right = getComparisonValue(tokens[2])
        comparison = radb.ast.ValExprBinaryOp(left, operator, right)
        return comparison
    elif comparisonToken.value == 'and':
        return sym.AND
    elif comparisonToken.value == 'or':
        return sym.OR
    else:
        raise Exception('error when parsing comparisons')

def getComparisonValue(valueToken: sqlparse.sql.Token):
    if valueToken.ttype == sqlparse.tokens.Number.Integer or valueToken.ttype == sqlparse.tokens.Number.Float:
        return radb.ast.RANumber(valueToken.value)        
    elif valueToken.ttype == sqlparse.tokens.String.Single:
        return radb.ast.RAString(valueToken.value)
    else:
        return getAttributeIdentifiers(valueToken)

def getComparisonOperator(operatorToken: sqlparse.sql.Token):
    if operatorToken.value == '=':
        return sym.EQ
    elif operatorToken.value == '!=':
        return sym.NE
    elif operatorToken.value == '<':
        return sym.LT
    elif operatorToken.value == '>':
        return sym.GT
    elif operatorToken.value == '<=':
        return sym.LE
    elif operatorToken.value == '>=':
        return sym.GE
    else:
        raise Exception('error when parsing comparison operator')

def getAttributeIdentifiers(token: sqlparse.sql.Token):
    if len(token.tokens) == 1:
        return radb.ast.AttrRef(None, str(token.tokens[0]))
    elif len(token.tokens) == 3:
        return radb.ast.AttrRef(str(token.tokens[0]), str(token.tokens[2]))
    else:
        raise Exception('error when parsing comparison identifiers')

# gets the relations joined and renamed if necessary
def getRelations(tokens: sqlparse.sql.TokenList):
    # get the token stating the relations
    relationsToken = getRelationsToken(tokens)
    
    relations = []
    # get single (renamed) relation
    if type(relationsToken) == sqlparse.sql.Identifier:
        relations.append(getRenamedRelation(relationsToken))
    # get several (renamed) relations
    elif type(relationsToken) == sqlparse.sql.IdentifierList:
        relations = getRenamedRelations(relationsToken)

    # return the SINGLE relation
    if len(relations) == 1:
        return relations[0]
    # return several relations cross joined
    else:
        return joinRelations(relations)

# cross joins several relations
def joinRelations(relations: []):
    if not len(relations) > 1:
        raise Exception('INVALID NUMBER OF RELATIONS FOR JOINING')

    #joinedRelations = ''
    crossedRelations = None
    for i in range(len(relations)):
        # join the first two relations
        if i == 0:
            crossedRelations = radb.ast.Cross(relations[i], relations[i+1])
        # join the next relation with the prvious statment starting from the third relation
        elif i > 1:
            crossedRelations = radb.ast.Cross(crossedRelations, relations[i])

    return crossedRelations
    #return joinedRelations

# gets the token specifying the used relations
def getRelationsToken(tokens: sqlparse.sql.TokenList):
    fromStatement = next(token for token in tokens if token.match(sqlparse.tokens.Token.Keyword, ['FROM']))
    indexOfFromStatment = tokens.index(fromStatement)

    # return the relations if there is a FROM statement and relations following
    if indexOfFromStatment > -1 or len(tokens) < indexOfFromStatment + 1:
        return tokens[indexOfFromStatment + 1]
    else:
        raise Exception('NO FROM STATEMENT OR NO FOLLOWING RELATIONS')

# renames a given set of relations
def getRenamedRelations(relationIdentifiers: sqlparse.sql.IdentifierList):
    # get identifiers only and rename them (remove whitespace and punctuation)
    renamedRelations = [getRenamedRelation(identifier) for identifier in relationIdentifiers if type(identifier) == sqlparse.sql.Identifier]

    if len(renamedRelations) > 0:
        return renamedRelations
    else:
        raise Exception('NO RELATIONS LEFT')

def getRenamedRelation(relationIdentifier: sqlparse.sql.Identifier):
    # perform renaming if necessary
    if len(relationIdentifier.tokens) > 1:
        return renameRelation(relationIdentifier.tokens)
    # no renaming necessary
    else:
        relation = radb.ast.RelRef(relationIdentifier.value)
        return relation

# performs the renaming of a SINGLE relation
def renameRelation(tokens: sqlparse.sql.TokenList):
    # remove unnecessary tokens so that only the relation and its new name remain 
    # => cleanedTokens is supposed to contain 2 tokens
    cleanedTokens = removeWhitespaceTokens(tokens)
    
    # perform renaming if 2 tokens remain
    if len(cleanedTokens) == 2:
        renamed = radb.ast.Rename(cleanedTokens[1].value, None, radb.ast.RelRef(cleanedTokens[0].value))
        return renamed
    else:
        raise Exception('RENAMING FAILED')

# removes unnecessary tokens
def cleanTokens(tokens: sqlparse.sql.TokenList):
    tokens = removeWhitespaceTokens(tokens)
    tokens = removeDistinct(tokens)
    return tokens

# removes all whitespace tokens
def removeWhitespaceTokens(tokens: sqlparse.sql.TokenList):
    return [token for token in tokens if not token.is_whitespace]

# removes all 'distinct' keyword tokens
def removeDistinct(tokens: sqlparse.sql.TokenList):
    return [token for token in tokens if not token.normalized == 'DISTINCT']

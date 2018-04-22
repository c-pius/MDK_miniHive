import sqlparse

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
        # remove surrounding parenthesis from the statement
        return removeSurroundingParenthesis(statement)
    else:
        # ensure there are single whitespaces after commas
        projection = projectionToken.value.replace(' ', '').replace(',', ', ')
        return '\project_{{{0}}} {1}'.format(projection, statement)

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
        return '(\select_{{{0}}} {1})'.format(conditions, relations)
    # return blank relations if no where token available
    else:
        return relations

# gets conditions for the select statement
def getConditions(token: sqlparse.sql.Token):
    # extract all comparison statments
    comparisons = [comparison.value for comparison in token.tokens if type(comparison) == sqlparse.sql.Comparison]
    # ensure there are single whitespaces between comparisons 
    comparisons = [comparison.replace(' ', '').replace('=', ' = ') for comparison in comparisons]

    # return single comparison
    if len(comparisons) == 1:
        return comparisons[0]
    # join multiple comparisons with AND
    elif len(comparisons) > 1:
        #add parenthisis if joinig multiple comparisons
        return ' and '.join(['({})'.format(comparison) for comparison in comparisons]) 
    else:
        Exception('NO COMPARISONS FOUND')

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

    joinedRelations = ''
    for i in range(len(relations)):
        # join the first two relations
        if i == 0:
            joinedRelations = '({} \\cross {})'.format(relations[i], relations[i+1])
        # join the next relation with the prvious statment starting from the third relation
        elif i > 1:
            joinedRelations = '({} \\cross {})'.format(joinedRelations, relations[i])

    return joinedRelations

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
        return relationIdentifier.value

# performs the renaming of a SINGLE relation
def renameRelation(tokens: sqlparse.sql.TokenList):
    # remove unnecessary tokens so that only the relation and its new name remain 
    # => cleanedTokens is supposed to contain 2 tokens
    cleanedTokens = removeWhitespaceTokens(tokens)
    
    # perform renaming if 2 tokens remain
    if len(cleanedTokens) == 2:
        return '(\\rename_{{{0}: *}} {1})'.format(cleanedTokens[1].value, cleanedTokens[0].value)
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

# removes surrounding parenthesis
def removeSurroundingParenthesis(statement: str):
    if statement[0] == '(' and statement[-1] == ')':
        statement = statement[1:]
        statement = statement[:-1]
    return statement
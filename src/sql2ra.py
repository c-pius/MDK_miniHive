import sqlparse

def translate(stmt: sqlparse.sql.Statement):
    tokens = cleanTokens(stmt.tokens)
    return project(tokens)

def project(tokens: sqlparse.sql.TokenList):
    for i in range(len(tokens)):
        if tokens[i].match(sqlparse.tokens.DML, ['SELECT']):
            projectionToken = tokens[i + 1]
            if projectionToken.ttype == sqlparse.tokens.Token.Wildcard:
                selected = select(tokens)
                if selected[0] == '(' and selected[-1] == ')':
                    selected = selected[1:]
                    selected = selected[:-1]
                return selected

            else:
                return '\project_{{{0}}} {1}'.format(projectionToken.value.replace(', ', ',').replace(',', ', '), select(tokens))

        return ''

def select(tokens: sqlparse.sql.TokenList):
    tables = getTables(tokens)
    if hasattr(tokens[-1], 'M_OPEN'):
        conditions = getConditions(tokens[-1])
        return '(\select_{{{0}}} {1})'.format(conditions, tables)
    else:
        return tables

def getConditions(token: sqlparse.sql.Token):
    conditions = token.value.replace('where ', '').replace(' = ', '=').replace('=', ' = ').split(' and ')
    if len(conditions) == 1:
            return conditions[0]
    elif len(conditions) > 1:
        formattedConditions = []
        for condition in conditions:
            formattedConditions.append('({})'.format(condition))
        return ' and '.join(formattedConditions)
    else:
        return ''

def getTables(tokens: sqlparse.sql.TokenList):
    joinedIdentifiers = None
    for i in range(len(tokens)):
        if (tokens[i].match(sqlparse.tokens.Token.Keyword, ['FROM'])):
            joinedIdentifiers = getIdentifiers(tokens[i + 1])
            return joinedIdentifiers

    return 'invalid'

def getIdentifiers(obj):
    if type(obj) == sqlparse.sql.Identifier:
        if len(obj.tokens) == 1:
            return obj.value
        elif len(obj.tokens) > 1:
            return renameIdentifiers(obj)
    elif type(obj) == sqlparse.sql.IdentifierList:
        identifiers = [identifier for identifier in obj.tokens if type(identifier) == sqlparse.sql.Identifier]
        renamedIdentifiers = []
        for identifier in identifiers:
            if len(identifier.tokens) == 1:
                renamedIdentifiers.append(identifier.value)
            else:
                renamedIdentifiers.append(renameIdentifiers(identifier.tokens))
            
        joinedIdentifiers = ''
        for i in range(len(renamedIdentifiers)):
            if i == 0:
                joinedIdentifiers = '({} \\cross {})'.format(renamedIdentifiers[i], renamedIdentifiers[i+1])
            elif i > 1:
                joinedIdentifiers = '({} \\cross {})'.format(joinedIdentifiers, renamedIdentifiers[i])

        return joinedIdentifiers
    else:
        return ''

def renameIdentifiers(tokens: sqlparse.sql.TokenList):
    cleanedTokens = removeWhitespaceTokens(tokens)
    for i in range(len(cleanedTokens)):
        if type(cleanedTokens[i]) == sqlparse.sql.Identifier:
            return '(\\rename_{{{0}: *}} {1})'.format(cleanedTokens[i].value, cleanedTokens[i-1].value)
    
    return 'invalid'

def cleanTokens(tokens: sqlparse.sql.TokenList):
    tokens = removeWhitespaceTokens(tokens)
    tokens = removeDistinct(tokens)
    return tokens

def removeWhitespaceTokens(tokens: sqlparse.sql.TokenList):
    return [token for token in tokens if not token.is_whitespace]

def removeDistinct(tokens: sqlparse.sql.TokenList):
    return [token for token in tokens if not token.normalized == 'DISTINCT']


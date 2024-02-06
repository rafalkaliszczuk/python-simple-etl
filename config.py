from configparser import ConfigParser

def read_config_file(filename = "database.ini", section = 'postgresql'):
    # Create a parser
    parser = ConfigParser()

    # Read config.py file
    parser.read(filename)
    db = {}

    if parser.has_section(section):
        params = parser.items(section) # params is in form of a tuple (a, b) -> param[0] is a first and param[1] is a second element
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception(
            'Section {0} is not found in the {1} file.'.format(section, filename)
            )
    return db
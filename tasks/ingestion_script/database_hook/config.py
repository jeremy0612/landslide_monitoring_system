from configparser import ConfigParser
import os
# Use definite path to avoid further traceback
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def load_config(filename=BASE_DIR+'/database.ini', section='postgresql'):
    parser = ConfigParser()
    parser.read(filename)

    # get section, default to postgresql
    config = {}
    parser.sections()
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return config

if __name__ == '__main__':
    config = load_config()
    print(config)
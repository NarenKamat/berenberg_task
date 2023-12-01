import configparser

def get_config(section, variablename):
	config = configparser.ConfigParser()
	config.read('config.txt')
	str_var = config[section][variablename]
	return str_var

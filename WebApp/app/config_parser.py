import sys, os, configparser

class ConfigParser(object):
    
    def __init__(self, config_file="app_config.conf", env="DEV"):
        self.server_hostname=None
        self.http_path=None
        self.access_token=None
        self.set_config(config_file)
        
    def set_config(self, config_file, env="DEV"):
        config = configparser.RawConfigParser(allow_no_value=True)        
        config.read(filenames = [config_file])   
        
        self.server_hostname=config.get(env, "server_hostname")
        self.http_path=config.get(env, "http_path")
        self.access_token=config.get(env, "access_token")
        
        
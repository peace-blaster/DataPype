# pipeline object- contains the .yaml interpreter.
#
#yaml interpreter: the heart of the module.
#
#This will interpret a user-supplied .yaml file which describes the needed pipeline.
#In future, a separate GUI will be built to generate the .yaml file which dictates the pipeline.
#
#sample .yaml file:
#
#

class pipeline:
    def __init__(self,yamlPath):
        self.yamlPath=yamlPath
        #initial tasks:
        self.getYaml()

##################################################################################
##################################################################################
##################################################################################    

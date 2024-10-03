from my_package.services.file import YAMLservice

my_yaml = YAMLservice(path = "my_package/config/input_output.yaml")
io = my_yaml.doRead()

model_yaml = YAMLservice(path="my_package/config/model_config.yaml")
model_config = model_yaml.doRead()
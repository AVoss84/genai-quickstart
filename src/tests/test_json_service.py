import pytest, os, json
# import sys
# sys.path.append(./src/my_package)
from my_package.services.file import JSONservice
from my_package.config import global_config as glob

def test_json_service_read():
    """Test the read_json method"""

    # create two test json files in the test directory
    test_json_1 = {"name": "John", "age": 30, "city": "New York"}
    test_json_2 = {"name": "Peter", "age": 45, "city": "New York"}

    json_service = JSONservice(root_path=os.path.join(glob.UC_CODE_DIR,'tests'), path="test_json_1.json")
    json_service.doWrite(test_json_1)

    json_service = JSONservice(root_path=os.path.join(glob.UC_CODE_DIR,'tests'), path="test_json_2.json")
    json_service.doWrite(test_json_2)

    # test the read_json method
    json_service = JSONservice(root_path=os.path.join(glob.UC_CODE_DIR,'tests'), path="test_json_1.json")
    json_1 = json_service.doRead()

    json_service = JSONservice(root_path=os.path.join(glob.UC_CODE_DIR,'tests'), path="test_json_2.json")
    json_2 = json_service.doRead()

    # test the write_json method
    assert json_1 == test_json_1
    assert json_2 == test_json_2

    # # test the read_json method with the wrong file name
    # with pytest.raises(FileNotFoundError):
    #     json_service.read_json("test_json_3.json")
    #     json_service = JSONservice(root_path=os.path.join(glob.UC_CODE_DIR,'tests'), path="test_json_3.json")
    #     json_service.doRead()

    # # test the write_json method with the wrong file name
    # with pytest.raises(FileNotFoundError):
    #     json_service.write_json("test_json_3.json", test_json_1)

    # # test the read_json method with the wrong file type.
    # with pytest.raises(TypeError):
    #     json_service.read_json("test_json_1.txt")

    # delete the test files
    os.remove(os.path.join(glob.UC_CODE_DIR,'tests',"test_json_1.json"))
    os.remove(os.path.join(glob.UC_CODE_DIR,'tests', "test_json_2.json"))




def main():
    test_case = "/home/mrhanice/OJdev/JudgeServer/tests/test_case/normal"
    # test_case_id = 'normal'
    # #if not (test_case or test_case_id) or (test_case and test_case_id):
    # if not (test_case or test_case_id):
    #     print("invalid parameter")
    # ok = bool(test_case)
    # print(ok)
    # test_case = ["1.in","2.in"]
    test_cases = {
        "1": {
            "stripped_output_md5": "eccbc87e4b5ce2fe28308fd9f2a7baf3",
            "output_size": 2,
            "output_md5": "6d7fce9fee471194aa8b5b6e47267f03",
            "input_name": "1.in",
            "input_size": 4,
            "output_name": "1.out"
        }
    }
    l = len(test_cases)
    print("len =",l)
    for index, item in enumerate(test_cases):
        # print(index,item["input_name"])
        print(index,item)

        # input_data = item["input"].encode("utf-8")
        # le = len(input_data)

if __name__ == '__main__':
    main()
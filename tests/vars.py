

def main(exe_path):
    str_vars = ["exe_path", "input_path", "output_path", "error_path", "log_path"]
    # for var in str_vars:
    var = str_vars[0]
    value = vars()[var]
        # if not isinstance(value, str):
        #     raise ValueError("{} must be a string".format(var))
    print("--{}={}".format(var, value))

if __name__ == '__main__':
    main('aaaa')
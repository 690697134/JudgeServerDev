
def run():
    try:
        a = 10
        b = 0
        c = a/b
    except Exception as e:
        raise e

if __name__ == '__main__':
    try:
        run()
    except Exception as e:
        print(e)
#### logging

import logging
LOG_FORMART = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.DEBUG, format=LOG_FORMART, filename="my.log")
#logging.debug("this is  debug")
#logging.info("this is  info")
#logging.warning("this is  warning")
#logging.error("this is  error")
#logging.critical("this is  critical")

#####  使用装饰器，根据不同的函数，传入的日志不相同
#def log(func):
#    def wrapper(*arg, **kv):
#        logging.error("this is info message")
#        return func(*arg, **kv)
#    return wrapper
def log(text):
    def decorator(func):
        def wrapper(*arg, **kv):
            logging.error(text)
            return func(*arg, **kv)
        return wrapper
    return decorator
@log("test donw")
def test():
    print("test done")
   
@log("main done")
def main():
    print("main done")
   
test()
main()



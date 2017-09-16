from multiprocessing import Process
import hug

# ---------------------------------
# 爬取页面和处理逻辑



# ---------------------------------
# 解析页面




# ---------------------------------
# REST服务

@hug.get('/')
@hug.default_input_format("application/json")
def returntest(data):
    """
    Return the welcome message to user
    :param name: User Name
    :param age: User Age
    :return:
    """
    return ('Result', hug.input_format.json(data))

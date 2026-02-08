from typing import Optional

from pulao.bar import SBar

class SupplyDemand:
    """
    # 供需强弱分析(市场状态解释与连续评估系统)

    ## sd需要回答三个问题：
    1. 这段走势中，谁在主导？
    2. 主导的效率高还是低？
    3. 这种主导是否正在被侵蚀？

    ## sd需要：
    1. sd要是解释性的，用于解释市场本身的，是市场本身展现出来的观点，
    2. sd要可以评估任意一段k线的供需强弱程度（swing/trend/k-window）
    3. sd要可以追踪实时行情的强弱以及可持续性评估

    ## 核心功能：
    ** 评估给定一段K线的多空力量强弱 **
    - 要求这个分值是无量纲的、不同段、不同标的之间要是可比的
    - 要由多个正交分量刻画这个行为，不能只用一个综合值
    - 可解释性要好要清晰
    """
    def __init__(self):
        self.bar:Optional[SBar] = None

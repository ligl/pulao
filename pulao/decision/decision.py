from pulao.constant import DecisionAction
from pulao.object import BaseDecorator


@BaseDecorator
class Decision:

    def __init__(self, action: DecisionAction):
        self.action: DecisionAction = action

    def evaluate(self):
        # 综合评估，得出结论
        pass

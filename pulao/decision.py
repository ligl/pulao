from .constant import DecisionAction
from .supply_demand import SupplyDemand, SupplyDemandManager


class Decision:
    action: DecisionAction

    def __init__(self, action: DecisionAction):
        self.action = action

    def evaluate(self):
        # 综合评估，得出结论
        pass

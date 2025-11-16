from pulao.constant import SwingDirection
from pulao.manager import SwingManager
from pulao.model import Swing

swing = Swing(direction=SwingDirection.UP)
swing_manager = SwingManager()
swing_manager.add(swing)
print(swing)
print(swing_manager)

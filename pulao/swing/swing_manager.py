from pulao.events import Observable
import polars as pl

from .swing import Swing


class SwingManager(Observable):
    df: pl.DataFrame

    def __init__(self):
        super().__init__()


    def add(self, swing: Swing):

        self.notify("swing.created", swing)

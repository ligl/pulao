from datetime import datetime as Datetime

import pytest
from pytest_mock import MockerFixture
from pulao.bar.cbar import CBar
from pulao.constant import FractalType
from pulao.swing.swing_manager import _SwingDiscoverer,_SwingBuilder
from pulao.bar import Fractal


@pytest.fixture
def setup_swing_builder(mocker:MockerFixture):
        # mock _SwingBuilder
    mock_swing_builder = mocker.create_autospec(_SwingBuilder, instance=True)
    mock_swing_builder.swing_manager = mocker.Mock()
    mock_swing_builder.swing_manager.cbar_manager = mocker.Mock()
    mock_swing_builder.detect_swing.return_value = False
    mock_swing_builder.swing_manager.cbar_manager.get_limit_sbar_id.return_value = 1
    return mock_swing_builder


def test_fractal_none(mocker:MockerFixture, setup_swing_builder):

    swing_discoverer = _SwingDiscoverer(setup_swing_builder)
    swing_discoverer.bottom_fractal = None
    swing_discoverer.top_fractal = None

    left_cbar = CBar(id=1, sbar_start_id=10, sbar_end_id=20, low_price=100.0, high_price=110.0, fractal_type=FractalType.NONE, created_at=Datetime.now())
    middle_cbar = CBar(id=2, sbar_start_id=21, sbar_end_id=30, low_price=90.0, high_price=100.0, fractal_type=FractalType.NONE, created_at=Datetime.now())
    right_cbar = CBar(id=3, sbar_start_id=31, sbar_end_id=40, low_price=95.0, high_price=115.0, fractal_type=FractalType.NONE, created_at=Datetime.now())
    fractal = Fractal(left=left_cbar, middle=middle_cbar, right=right_cbar)

    swing_dict = swing_discoverer.discover(fractal)

    assert swing_dict is None
    assert swing_discoverer.bottom_fractal is None
    assert swing_discoverer.top_fractal is None

def test_fractal_bottom(mocker:MockerFixture, setup_swing_builder):

    swing_discoverer = _SwingDiscoverer(setup_swing_builder)
    swing_discoverer.bottom_fractal = None
    swing_discoverer.top_fractal = None

    left_cbar = CBar(id=1, sbar_start_id=10, sbar_end_id=20, low_price=100.0, high_price=110.0, fractal_type=FractalType.NONE, created_at=Datetime.now())
    middle_cbar = CBar(id=2, sbar_start_id=21, sbar_end_id=30, low_price=90.0, high_price=100.0, fractal_type=FractalType.BOTTOM, created_at=Datetime.now())
    right_cbar = CBar(id=3, sbar_start_id=31, sbar_end_id=40, low_price=95.0, high_price=115.0, fractal_type=FractalType.NONE, created_at=Datetime.now())
    fractal = Fractal(left=left_cbar, middle=middle_cbar, right=right_cbar)

    swing_dict = swing_discoverer.discover(fractal)

    assert swing_dict is None
    assert swing_discoverer.top_fractal is None
    assert swing_discoverer.bottom_fractal is not None

def test_fractal_top(mocker:MockerFixture, setup_swing_builder):

    swing_discoverer = _SwingDiscoverer(setup_swing_builder)
    swing_discoverer.bottom_fractal = None
    swing_discoverer.top_fractal = None

    left_cbar = CBar(id=1, sbar_start_id=10, sbar_end_id=20, low_price=100.0, high_price=110.0, fractal_type=FractalType.NONE, created_at=Datetime.now())
    middle_cbar = CBar(id=2, sbar_start_id=21, sbar_end_id=30, low_price=95.0, high_price=120.0, fractal_type=FractalType.TOP, created_at=Datetime.now())
    right_cbar = CBar(id=3, sbar_start_id=31, sbar_end_id=40, low_price=95.0, high_price=115.0, fractal_type=FractalType.NONE, created_at=Datetime.now())
    fractal = Fractal(left=left_cbar, middle=middle_cbar, right=right_cbar)

    swing_dict = swing_discoverer.discover(fractal)

    assert swing_dict is None
    assert swing_discoverer.top_fractal is not None
    assert swing_discoverer.bottom_fractal is None

def test_fractal_both(mocker:MockerFixture, setup_swing_builder):

    swing_discoverer = _SwingDiscoverer(setup_swing_builder)

    left_cbar = CBar(id=1, sbar_start_id=10, sbar_end_id=20, low_price=100.0, high_price=110.0, fractal_type=FractalType.NONE, created_at=Datetime.now())
    middle_cbar = CBar(id=2, sbar_start_id=21, sbar_end_id=30, low_price=90.0, high_price=100.0, fractal_type=FractalType.BOTTOM, created_at=Datetime.now())
    right_cbar = CBar(id=3, sbar_start_id=31, sbar_end_id=40, low_price=95.0, high_price=115.0, fractal_type=FractalType.NONE, created_at=Datetime.now())
    swing_discoverer.bottom_fractal = Fractal(left=left_cbar, middle=middle_cbar, right=right_cbar)

    left_cbar = CBar(id=1, sbar_start_id=10, sbar_end_id=20, low_price=100.0, high_price=110.0, fractal_type=FractalType.NONE, created_at=Datetime.now())
    middle_cbar = CBar(id=2, sbar_start_id=21, sbar_end_id=30, low_price=90.0, high_price=120.0, fractal_type=FractalType.TOP, created_at=Datetime.now())
    right_cbar = CBar(id=3, sbar_start_id=31, sbar_end_id=40, low_price=95.0, high_price=115.0, fractal_type=FractalType.NONE, created_at=Datetime.now())
    swing_discoverer.top_fractal = Fractal(left=left_cbar, middle=middle_cbar, right=right_cbar)

    left_cbar = CBar(id=1, sbar_start_id=10, sbar_end_id=20, low_price=100.0, high_price=110.0, fractal_type=FractalType.NONE, created_at=Datetime.now())
    middle_cbar = CBar(id=2, sbar_start_id=21, sbar_end_id=30, low_price=80.0, high_price=120.0, fractal_type=FractalType.NONE, created_at=Datetime.now())
    right_cbar = CBar(id=3, sbar_start_id=31, sbar_end_id=40, low_price=95.0, high_price=115.0, fractal_type=FractalType.NONE, created_at=Datetime.now())
    fractal = Fractal(left=left_cbar, middle=middle_cbar, right=right_cbar)

    swing_dict = swing_discoverer.discover(fractal)

    assert swing_dict is None
    assert swing_discoverer.top_fractal is not None
    assert swing_discoverer.bottom_fractal is not None

def test_swing_discover(mocker:MockerFixture, setup_swing_builder):

    swing_discoverer = _SwingDiscoverer(setup_swing_builder)

    left_cbar = CBar(id=1, sbar_start_id=10, sbar_end_id=20, low_price=100.0, high_price=110.0, fractal_type=FractalType.NONE, created_at=Datetime.now())
    middle_cbar = CBar(id=2, sbar_start_id=21, sbar_end_id=30, low_price=90.0, high_price=120.0, fractal_type=FractalType.TOP, created_at=Datetime.now())
    right_cbar = CBar(id=3, sbar_start_id=31, sbar_end_id=40, low_price=95.0, high_price=115.0, fractal_type=FractalType.NONE, created_at=Datetime.now())
    fractal = Fractal(left=left_cbar, middle=middle_cbar, right=right_cbar)

    swing_dict = swing_discoverer.discover(fractal)

    assert swing_dict is not None

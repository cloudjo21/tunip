import matplotlib
import warnings
from matplotlib import font_manager, rc


warnings.filterwarnings(action='ignore')

from sys import platform

if platform == "win32":
    matplotlib.font_manager._rebuild()

    font_path = "/usr/share/fonts/nhn-nanum/NanumGothic.ttf"
    font = font_manager.FontProperties(fname=font_path).get_name()
    rc('font', family=font)


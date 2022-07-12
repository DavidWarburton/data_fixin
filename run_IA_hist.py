import sys

folder_name = os.path.split(__file__)[0]
if folder_name not in sys.path:
	sys.path.append(folder_name)

from ia_hist import ia_hist


ia_hist
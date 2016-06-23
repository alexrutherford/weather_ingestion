import glob
import utils

files=glob.glob('../data/weather_data/*_processed.json')

print '%d files to unprocess' % len(files)

map(utils.unRenameFile,files)

print 'Unrenamed'

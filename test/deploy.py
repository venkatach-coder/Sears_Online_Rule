import os
import subprocess as sp
import sys
import shutil
import os
import glob

def onerror(func, path, exc_info):

    import stat
    if not os.access(path, os.W_OK):
        # Is the error an access error ?
        os.chmod(path, stat.S_IWUSR)
        func(path)
    else:
        raise Exception


def clean():
    total_dir = os.listdir(PROJ_DIR)
    CLEAN_FILES = [x for x in total_dir if str(x).endswith('.egg-info') or x in ( 'build', 'dist')]
    print(CLEAN_FILES)
    for path_spec in CLEAN_FILES:
        shutil.rmtree(os.path.join(PROJ_DIR, path_spec), onerror=onerror)
if __name__ == '__main__':

    PROJ_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sp.check_output('{} {}/setup.py bdist_egg'.format(sys.executable, PROJ_DIR), shell=True, cwd= PROJ_DIR)
    sp.check_output('gsutil cp {} gs://jx_notebooks/Sears_Online_Rule-0.0.1-py3.6.egg'\
                    .format(os.path.join(PROJ_DIR, 'dist', 'Sears_Online_Rule-0.0.1-py3.6.egg')), shell=True)
    clean()





